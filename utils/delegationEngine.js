import { createVotingOps } from './redisKeys.js';
import { supabase } from '../database/supabase.js';

/**
 * Delegation Engine for Liquid Democracy
 * Handles bulk delegation chain resolution and voting power calculation
 */
export class DelegationEngine {
  constructor(redisClient) {
    this.redis = redisClient;
    this.votingOps = createVotingOps(redisClient);
  }

  /**
   * Core method: Resolve all delegation chains for a proposal at once
   * @param {string} proposalId - UUID of the proposal
   * @returns {Object} Complete delegation resolution with voting powers
   */
  async resolveBulkDelegations(proposalId) {
    const startTime = Date.now();
    
    try {
      console.log(`🔄 Starting bulk delegation resolution for proposal ${proposalId}`);

      // Step 1: Load all data from Redis
      const [delegationMap, votesMap, participants] = await Promise.all([
        this.votingOps.getAllDelegations(proposalId),
        this.votingOps.getAllVotes(proposalId),
        this.votingOps.getAllParticipants(proposalId)
      ]);

      console.log(`📊 Loaded data: ${Object.keys(delegationMap).length} delegations, ${Object.keys(votesMap).length} votes, ${participants.length} participants`);

      // Step 2: Identify direct voters (participants who are NOT delegators)
      const directVoters = participants.filter(wallet => !delegationMap[wallet.toLowerCase()]);
      
      console.log(`👥 Identified ${directVoters.length} direct voters, ${Object.keys(delegationMap).length} delegators`);

      // Step 3: Build delegation graph and resolve chains
      const delegationGraph = this.buildDelegationGraph(delegationMap);
      const resolvedChains = this.resolveAllChains(delegationGraph);

      // Step 4: Calculate voting powers
      const votingPowers = this.calculateVotingPowers(resolvedChains.delegationResolution, votesMap);

      // Step 5: Create complete resolution object
      const resolution = {
        proposalId,
        computedAt: new Date().toISOString(),
        delegationResolution: resolvedChains.delegationResolution,
        votingPowers,
        metadata: {
          totalParticipants: participants.length,
          directVoters: directVoters.length,
          delegators: Object.keys(delegationMap).length,
          longestChain: resolvedChains.longestChainLength,
          computationTimeMs: Date.now() - startTime
        },
        auditTrail: resolvedChains.auditTrail
      };

      // Step 6: Save to PostgreSQL for audit
      await this.saveResolutionAudit(resolution);

      console.log(`✅ Bulk delegation resolution completed in ${Date.now() - startTime}ms`);
      return {
        success: true,
        resolution
      };

    } catch (error) {
      const computationTime = Date.now() - startTime;
      console.error(`❌ Bulk delegation resolution failed for proposal ${proposalId}:`, error);

      // Save error to audit trail
      await this.saveErrorAudit(proposalId, error.message, computationTime);

      return {
        success: false,
        error: error.message,
        computationTimeMs: computationTime
      };
    }
  }

  /**
   * Build in-memory graph structure from delegation map
   * @param {Object} delegationMap - { delegator: delegate }
   * @returns {Object} Graph structure for efficient traversal
   */
  buildDelegationGraph(delegationMap) {
    const graph = {};
    const incomingEdges = {}; // Track who delegates TO each person

    // Build adjacency list and reverse mapping
    for (const [delegator, delegate] of Object.entries(delegationMap)) {
      const normalizedDelegator = delegator.toLowerCase();
      const normalizedDelegate = delegate.toLowerCase();

      graph[normalizedDelegator] = normalizedDelegate;
      
      if (!incomingEdges[normalizedDelegate]) {
        incomingEdges[normalizedDelegate] = [];
      }
      incomingEdges[normalizedDelegate].push(normalizedDelegator);
    }

    return {
      delegations: graph,
      incomingEdges,
      size: Object.keys(graph).length
    };
  }

  /**
   * Resolve all delegation chains simultaneously
   * @param {Object} delegationGraph - Graph structure
   * @returns {Object} Complete chain resolution results
   */
  resolveAllChains(delegationGraph) {
    const { delegations } = delegationGraph;
    const delegationResolution = {}; // Final delegate for each user
    const auditTrail = {}; // Chain details for transparency
    let longestChainLength = 1; // At least 1 for direct voters

    // Process each delegator
    for (const delegator of Object.keys(delegations)) {
      const chainResult = this.resolveChainForUser(delegator, delegations);
      
      delegationResolution[delegator] = chainResult.finalDelegate;
      auditTrail[delegator] = {
        path: chainResult.path,
        length: chainResult.length
      };

      longestChainLength = Math.max(longestChainLength, chainResult.length);
    }

    return {
      delegationResolution,
      auditTrail,
      longestChainLength
    };
  }

  /**
   * Resolve delegation chain for a single user
   * @param {string} startWallet - Starting wallet address
   * @param {Object} delegations - Delegation mapping
   * @returns {Object} Chain resolution result
   */
  resolveChainForUser(startWallet, delegations) {
    const path = [startWallet];
    const visited = new Set([startWallet]);
    let current = startWallet;

    // Follow delegation chain with cycle protection
    const MAX_CHAIN_LENGTH = 100; // Safety limit
    let steps = 0;

    while (steps < MAX_CHAIN_LENGTH) {
      const nextDelegate = delegations[current];
      
      if (!nextDelegate) {
        // End of chain - current is the final delegate
        break;
      }

      // Check for cycle (should be prevented by existing cycle detection, but safety check)
      if (visited.has(nextDelegate)) {
        console.warn(`⚠️ Cycle detected in delegation chain: ${path.join(' → ')} → ${nextDelegate}`);
        break;
      }

      path.push(nextDelegate);
      visited.add(nextDelegate);
      current = nextDelegate;
      steps++;
    }

    return {
      finalDelegate: current,
      path,
      length: path.length
    };
  }

  /**
   * Calculate voting power for each participant
   * @param {Object} delegationResolution - { delegator: finalDelegate }
   * @param {Object} votesMap - { voter: optionNumber }
   * @returns {Object} Voting powers { wallet: power }
   */
  calculateVotingPowers(delegationResolution, votesMap) {
    const votingPowers = {};

    // Step 1: Each delegator contributes 1 vote to their final delegate
    for (const [delegator, finalDelegate] of Object.entries(delegationResolution)) {
      votingPowers[finalDelegate] = (votingPowers[finalDelegate] || 0) + 1;
    }

    // Step 2: Each direct voter contributes 1 vote to themselves
    for (const directVoter of Object.keys(votesMap)) {
      votingPowers[directVoter] = (votingPowers[directVoter] || 0) + 1;
    }

    // Step 3: Remove any wallets with 0 power (cleanup)
    for (const [wallet, power] of Object.entries(votingPowers)) {
      if (power === 0) {
        delete votingPowers[wallet];
      }
    }

    return votingPowers;
  }

  /**
   * Save successful resolution to audit table
   * @param {Object} resolution - Complete resolution data
   */
  async saveResolutionAudit(resolution) {
    try {
      const { error } = await supabase
        .from('delegation_resolution_audit')
        .upsert({
          proposal_id: resolution.proposalId,
          resolution_data: resolution,
          computation_time_ms: resolution.metadata.computationTimeMs,
          total_participants: resolution.metadata.totalParticipants,
          direct_voters: resolution.metadata.directVoters,
          delegators: resolution.metadata.delegators,
          longest_chain_length: resolution.metadata.longestChain,
          status: 'completed'
        }, {
          onConflict: 'proposal_id'
        });

      if (error) {
        console.error('Error saving resolution audit:', error);
      } else {
        console.log(`✅ Resolution audit saved for proposal ${resolution.proposalId}`);
      }
    } catch (error) {
      console.error('Error saving resolution audit:', error);
    }
  }

  /**
   * Save error to audit table
   * @param {string} proposalId - Proposal ID
   * @param {string} errorMessage - Error message
   * @param {number} computationTime - Time taken before error
   */
  async saveErrorAudit(proposalId, errorMessage, computationTime) {
    try {
      await supabase
        .from('delegation_resolution_audit')
        .upsert({
          proposal_id: proposalId,
          resolution_data: { error: errorMessage, proposalId },
          computation_time_ms: computationTime,
          status: 'error',
          error_message: errorMessage,
          total_participants: 0,
          direct_voters: 0,
          delegators: 0,
          longest_chain_length: 0
        }, {
          onConflict: 'proposal_id'
        });

      console.log(`📝 Error audit saved for proposal ${proposalId}`);
    } catch (error) {
      console.error('Error saving error audit:', error);
    }
  }

  /**
   * Get saved resolution from audit table
   * @param {string} proposalId - Proposal ID
   * @returns {Object|null} Saved resolution or null
   */
  async getSavedResolution(proposalId) {
    try {
      const { data, error } = await supabase
        .from('delegation_resolution_audit')
        .select('resolution_data, computed_at, status')
        .eq('proposal_id', proposalId)
        .eq('status', 'completed')
        .single();

      if (error || !data) {
        return null;
      }

      return data.resolution_data;
    } catch (error) {
      console.error(`Error getting saved resolution for ${proposalId}:`, error);
      return null;
    }
  }

  /**
   * Test method: Get simple delegation summary for debugging
   * @param {string} proposalId - Proposal ID
   * @returns {Object} Simple summary for testing
   */
  async getResolutionSummary(proposalId) {
    try {
      const result = await this.resolveBulkDelegations(proposalId);
      
      if (!result.success) {
        return { error: result.error };
      }

      const { resolution } = result;
      
      return {
        proposalId,
        totalParticipants: resolution.metadata.totalParticipants,
        directVoters: resolution.metadata.directVoters,
        delegators: resolution.metadata.delegators,
        longestChain: resolution.metadata.longestChain,
        computationTime: resolution.metadata.computationTimeMs,
        votingPowers: Object.keys(resolution.votingPowers).length,
        sampleVotingPowers: Object.fromEntries(
          Object.entries(resolution.votingPowers).slice(0, 5) // First 5 for preview
        )
      };
    } catch (error) {
      return { error: error.message };
    }
  }
}

/**
 * Create delegation engine instance
 * @param {Object} redisClient - Redis client instance
 * @returns {DelegationEngine} Delegation engine instance
 */
export function createDelegationEngine(redisClient) {
  return new DelegationEngine(redisClient);
}

/**
 * Factory function for easy import
 */
export default createDelegationEngine;