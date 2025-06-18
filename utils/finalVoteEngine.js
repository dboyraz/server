import { createVotingOps } from './redisKeys.js';
import { supabase, proposalDb } from '../database/supabase.js';

/**
 * Final Vote Count Engine
 * Handles final vote tallying using delegation resolution (SQL) results and cast votes (Redis)
 */
export class FinalVoteEngine {
  constructor(redisClient) {
    this.redis = redisClient;
    this.votingOps = createVotingOps(redisClient);
  }

  /**
   * Core method: Calculate final vote results for a proposal
   * @param {string} proposalId - UUID of the proposal
   * @returns {Object} Complete final vote results
   */
  async calculateFinalResults(proposalId) {
    const startTime = Date.now();
    
    try {
      console.log(`🗳️ Starting final vote counting for proposal ${proposalId}`);

      // Step 1: Load delegation resolution results from Supabase
      const votingPowers = await this.loadVotingPowers(proposalId);
      if (!votingPowers) {
        throw new Error('No delegation resolution found - cannot proceed with vote counting');
      }

      // Step 2: Load actual votes from Redis
      const votes = await this.votingOps.getAllVotes(proposalId);
      console.log(`📊 Loaded ${Object.keys(votes).length} votes from Redis`);

      // Step 3: Load proposal options for validation
      const proposal = await proposalDb.getById(proposalId);
      if (!proposal) {
        throw new Error('Proposal not found');
      }

      // Step 4: Calculate weighted vote results
      const results = await this.calculateWeightedResults(votingPowers, votes, proposal.options);

      // Step 5: Create complete result object
      const finalResults = {
        proposalId,
        computedAt: new Date().toISOString(),
        optionResults: results.optionTallies,
        metadata: {
          totalVotingPower: results.totalVotingPower,
          totalVotesCast: results.totalVotesCast,
          totalUniqueVoters: results.totalUniqueVoters,
          winningOption: results.winningOption,
          computationTimeMs: Date.now() - startTime
        },
        voterBreakdown: results.voterBreakdown,
        proposalInfo: {
          title: proposal.title,
          totalOptions: proposal.options?.length || 0,
          votingDeadline: proposal.voting_deadline
        }
      };

      // Step 6: Save to PostgreSQL for audit and UI display
      await this.saveResultsAudit(finalResults);

      console.log(`✅ Final vote counting completed in ${Date.now() - startTime}ms`);
      return {
        success: true,
        results: finalResults
      };

    } catch (error) {
      const computationTime = Date.now() - startTime;
      console.error(`❌ Final vote counting failed for proposal ${proposalId}:`, error);

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
   * Load voting powers from delegation resolution audit
   * @param {string} proposalId - Proposal ID
   * @returns {Object|null} Voting powers { wallet: power } or null if not found
   */
  async loadVotingPowers(proposalId) {
    try {
      const { data, error } = await supabase
        .from('delegation_resolution_audit')
        .select('resolution_data')
        .eq('proposal_id', proposalId)
        .eq('status', 'completed')
        .single();

      if (error || !data) {
        console.warn(`⚠️ No delegation resolution found for proposal ${proposalId}`);
        return null;
      }

      const votingPowers = data.resolution_data?.votingPowers;
      if (!votingPowers) {
        console.warn(`⚠️ No voting powers found in delegation resolution for proposal ${proposalId}`);
        return null;
      }

      console.log(`📋 Loaded voting powers for ${Object.keys(votingPowers).length} participants`);
      return votingPowers;
      
    } catch (error) {
      console.error(`Error loading voting powers for ${proposalId}:`, error);
      throw error;
    }
  }

  /**
   * Calculate weighted vote results
   * @param {Object} votingPowers - { wallet: power }
   * @param {Object} votes - { wallet: optionNumber }
   * @param {Array} proposalOptions - Array of proposal options
   * @returns {Object} Calculated results
   */
  async calculateWeightedResults(votingPowers, votes, proposalOptions) {
    const optionTallies = {}; // { optionNumber: totalVotingPower }
    const voterBreakdown = {}; // { wallet: { votingPower, votedOption } }
    
    let totalVotingPower = 0;
    let totalVotesCast = 0;
    let totalUniqueVoters = 0;

    // Initialize option tallies to 0
    if (proposalOptions && proposalOptions.length > 0) {
      proposalOptions.forEach(option => {
        optionTallies[option.option_number] = 0;
      });
    }

    // Calculate total voting power (sum of all participants)
    for (const [wallet, power] of Object.entries(votingPowers)) {
      totalVotingPower += power;
    }

    // Process each vote
    for (const [voterWallet, votedOption] of Object.entries(votes)) {
      const normalizedWallet = voterWallet.toLowerCase();
      const votingPower = votingPowers[normalizedWallet] || 1; // Default to 1 if not in delegation resolution
      
      // Add voting power to the chosen option
      if (!optionTallies[votedOption]) {
        optionTallies[votedOption] = 0;
      }
      optionTallies[votedOption] += votingPower;
      
      // Track voter breakdown for audit
      voterBreakdown[normalizedWallet] = {
        votingPower,
        votedOption
      };
      
      totalVotesCast += votingPower;
      totalUniqueVoters++;
    }

    // Determine winning option (highest vote count)
    let winningOption = null;
    let maxVotes = 0;
    for (const [option, votes] of Object.entries(optionTallies)) {
      if (votes > maxVotes) {
        maxVotes = votes;
        winningOption = parseInt(option);
      }
    }

    // Handle ties (if there are multiple options with same max votes)
    const maxVoteOptions = Object.entries(optionTallies)
      .filter(([_, votes]) => votes === maxVotes && votes > 0)
      .map(([option, _]) => parseInt(option));
    
    if (maxVoteOptions.length > 1) {
      winningOption = null; // Tie - no single winner
    } else if (maxVotes === 0) {
      winningOption = null; // No votes cast
    }

    console.log(`📈 Vote tallies: ${JSON.stringify(optionTallies)}`);
    console.log(`🏆 Winning option: ${winningOption || 'TIE/NO_VOTES'}`);

    return {
      optionTallies,
      voterBreakdown,
      totalVotingPower,
      totalVotesCast,
      totalUniqueVoters,
      winningOption
    };
  }

  /**
   * Save successful results to audit table
   * @param {Object} results - Complete results data
   */
  async saveResultsAudit(results) {
    try {
      const { error } = await supabase
        .from('final_vote_results_audit')
        .upsert({
          proposal_id: results.proposalId,
          vote_results: results,
          computation_time_ms: results.metadata.computationTimeMs,
          total_voting_power: results.metadata.totalVotingPower,
          total_votes_cast: results.metadata.totalVotesCast,
          winning_option: results.metadata.winningOption,
          status: 'completed'
        }, {
          onConflict: 'proposal_id'
        });

      if (error) {
        console.error('Error saving vote results audit:', error);
      } else {
        console.log(`✅ Vote results audit saved for proposal ${results.proposalId}`);
      }
    } catch (error) {
      console.error('Error saving vote results audit:', error);
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
        .from('final_vote_results_audit')
        .upsert({
          proposal_id: proposalId,
          vote_results: { error: errorMessage, proposalId },
          computation_time_ms: computationTime,
          status: 'error',
          error_message: errorMessage,
          total_voting_power: 0,
          total_votes_cast: 0,
          winning_option: null
        }, {
          onConflict: 'proposal_id'
        });

      console.log(`📝 Vote counting error audit saved for proposal ${proposalId}`);
    } catch (error) {
      console.error('Error saving vote counting error audit:', error);
    }
  }

  /**
   * Get saved results from audit table
   * @param {string} proposalId - Proposal ID
   * @returns {Object|null} Saved results or null
   */
  async getSavedResults(proposalId) {
    try {
      const { data, error } = await supabase
        .from('final_vote_results_audit')
        .select('vote_results, computed_at, status')
        .eq('proposal_id', proposalId)
        .eq('status', 'completed')
        .single();

      if (error || !data) {
        return null;
      }

      return data.vote_results;
    } catch (error) {
      console.error(`Error getting saved results for ${proposalId}:`, error);
      return null;
    }
  }

  /**
   * Test method: Get simple results summary for debugging
   * @param {string} proposalId - Proposal ID
   * @returns {Object} Simple summary for testing
   */
  async getResultsSummary(proposalId) {
    try {
      const result = await this.calculateFinalResults(proposalId);
      
      if (!result.success) {
        return { error: result.error };
      }

      const { results } = result;
      
      return {
        proposalId,
        totalVotingPower: results.metadata.totalVotingPower,
        totalVotesCast: results.metadata.totalVotesCast,
        totalUniqueVoters: results.metadata.totalUniqueVoters,
        winningOption: results.metadata.winningOption,
        computationTime: results.metadata.computationTimeMs,
        optionResults: results.optionResults,
        proposalTitle: results.proposalInfo.title
      };
    } catch (error) {
      return { error: error.message };
    }
  }
}

/**
 * Create final vote engine instance
 * @param {Object} redisClient - Redis client instance
 * @returns {FinalVoteEngine} Final vote engine instance
 */
export function createFinalVoteEngine(redisClient) {
  return new FinalVoteEngine(redisClient);
}

export default createFinalVoteEngine;