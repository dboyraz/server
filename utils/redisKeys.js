/**
 * Redis key patterns for liquid democracy voting system
 * These functions generate consistent key names for different data types
 */
export const REDIS_KEYS = {
  // ========== CORE VOTING DATA ==========
  
  // Hash: wallet_address -> option_number (string)
  // Example: proposal:uuid123:votes = { "0x123...": "1", "0x456...": "2" }
  votes: (proposalId) => `proposal:${proposalId}:votes`,
  
  // Hash: delegator_wallet -> delegate_wallet
  // Example: proposal:uuid123:delegations = { "0x123...": "0x456...", "0x789...": "0x456..." }
  delegations: (proposalId) => `proposal:${proposalId}:delegations`,
  
  // ========== COMPUTED RESULTS (temporary during calculation) ==========
  
  // Hash: wallet_address -> computed_weight (string)
  // Example: proposal:uuid123:vote_weights = { "0x123...": "1.0", "0x456...": "3.0" }
  vote_weights: (proposalId) => `proposal:${proposalId}:vote_weights`,
  
  // Hash: delegator_wallet -> final_delegate_wallet (after chain resolution)
  // Example: proposal:uuid123:chains = { "0x123...": "0x789...", "0x456...": "0x789..." }
  delegation_chains: (proposalId) => `proposal:${proposalId}:chains`,
  
  // ========== PROCESSING STATUS ==========
  
  // String: "active" | "calculating" | "completed" | "error"
  status: (proposalId) => `proposal:${proposalId}:status`,
  
  // String: timestamp when processing started (for deadlock prevention)
  lock: (proposalId) => `proposal:${proposalId}:lock`,
  
  // ========== METADATA ==========
  
  // String: ISO timestamp of voting deadline
  deadline: (proposalId) => `proposal:${proposalId}:deadline`,
  
  // Set: all wallet addresses that have participated (voted or delegated)
  participants: (proposalId) => `proposal:${proposalId}:participants`,
  
  // ========== RATE LIMITING ==========
  
  // String: timestamp of last action (60 second cooldown)
  cooldown: (userWallet, proposalId) => `cooldown:${userWallet}:${proposalId}`,
  
  // ========== BATCH OPERATIONS ==========
  
  // Get all keys for a proposal (for backup/cleanup)
  allForProposal: (proposalId) => [
    `proposal:${proposalId}:votes`,
    `proposal:${proposalId}:delegations`, 
    `proposal:${proposalId}:vote_weights`,
    `proposal:${proposalId}:delegation_chains`,
    `proposal:${proposalId}:status`,
    `proposal:${proposalId}:lock`,
    `proposal:${proposalId}:deadline`,
    `proposal:${proposalId}:participants`
  ]
};

/**
 * Redis TTL (Time To Live) configuration
 * Keys expire 7 days after proposal deadline to prevent Redis bloat
 */
export const REDIS_TTL = {
  // 7 days in seconds (for completed proposals)
  COMPLETED_PROPOSAL: 7 * 24 * 60 * 60,
  
  // 1 hour for processing locks (prevent infinite locks)
  PROCESSING_LOCK: 60 * 60,
  
  // 60 seconds for rate limiting cooldown
  RATE_LIMIT: 60,
  
  // 24 hours for error status (allow retry next day)
  ERROR_STATUS: 24 * 60 * 60
};

/**
 * Redis operation helper functions
 * Provides consistent interface for common operations
 */
export class RedisVotingOps {
  constructor(redisClient) {
    this.redis = redisClient;
  }
  
  /**
   * Cast a vote
   */
  async castVote(proposalId, userWallet, optionNumber) {
    const walletKey = userWallet.toLowerCase();
    const optionValue = optionNumber.toString();
  
    const votesKey = REDIS_KEYS.votes(proposalId);
    const delegationsKey = REDIS_KEYS.delegations(proposalId);
    const participantsKey = REDIS_KEYS.participants(proposalId);
  
    try {
      // 1. Set the vote using object syntax
      await this.redis.hset(votesKey, { [walletKey]: optionValue });
    
      // 2. Add to participants
      await this.redis.sadd(participantsKey, walletKey);
    
      // 3. Remove any existing delegation
      await this.redis.hdel(delegationsKey, walletKey);
    
      return true;
    } catch (error) {
      console.error('Error casting vote:', error);
      throw error;
    }
  }
  
  /**
   * Set delegation
   */
  async setDelegation(proposalId, delegatorWallet, delegateWallet) {
    const delegatorKey = delegatorWallet.toLowerCase();
    const delegateKey = delegateWallet.toLowerCase();
  
    const votesKey = REDIS_KEYS.votes(proposalId);
    const delegationsKey = REDIS_KEYS.delegations(proposalId);
    const participantsKey = REDIS_KEYS.participants(proposalId);
  
    try {
      // 1. Set the delegation using object syntax
      await this.redis.hset(delegationsKey, { [delegatorKey]: delegateKey });
    
      // 2. Add to participants
      await this.redis.sadd(participantsKey, delegatorKey);
    
      // 3. Remove any existing vote
      await this.redis.hdel(votesKey, delegatorKey);
    
      return true;
    } catch (error) {
      console.error('Error setting delegation:', error);
      throw error;
    }
  }
  
  /**
   * Remove a user's vote
   */
  async removeVote(proposalId, userWallet) {
    const walletKey = userWallet.toLowerCase();
    return await this.redis.hdel(REDIS_KEYS.votes(proposalId), walletKey);
  }
  
  /**
   * Remove a user's delegation
   */
  async removeDelegation(proposalId, userWallet) {
    const walletKey = userWallet.toLowerCase();
    return await this.redis.hdel(REDIS_KEYS.delegations(proposalId), walletKey);
  }
  
  /**
   * Get a user's current vote
   */
  async getUserVote(proposalId, userWallet) {
    try {
      const vote = await this.redis.hget(REDIS_KEYS.votes(proposalId), userWallet.toLowerCase());
    
      if (vote === null || vote === undefined || vote === '') {
        return null;
      }
    
      const parsed = parseInt(vote);
      return isNaN(parsed) ? null : parsed;
    } catch (error) {
      console.error('Error getting user vote from Redis:', error);
      return null;
    }
  }
  
  /**
   * Get a user's current delegation
   */
  async getUserDelegation(proposalId, userWallet) {
    try {
      const delegation = await this.redis.hget(REDIS_KEYS.delegations(proposalId), userWallet.toLowerCase());
    
      if (delegation === null || delegation === undefined || delegation === '') {
        return null;
      }
    
      return delegation;
    } catch (error) {
      console.error('Error getting user delegation from Redis:', error);
      return null;
    }
  }
  
  /**
   * Get user's current voting status
   */
  async getUserStatus(proposalId, userWallet) {
    const [vote, delegation] = await Promise.all([
      this.getUserVote(proposalId, userWallet),
      this.getUserDelegation(proposalId, userWallet)
    ]);
    
    return {
      has_voted: vote !== null,
      voted_option: vote,
      has_delegated: delegation !== null,
      delegate_wallet: delegation
    };
  }
  
  /**
   * Set proposal processing status
   */
  async setProposalStatus(proposalId, status, ttl = null) {
    if (ttl) {
      return await this.redis.setex(REDIS_KEYS.status(proposalId), ttl, status);
    }
    return await this.redis.set(REDIS_KEYS.status(proposalId), status);
  }
  
  /**
   * Get proposal processing status
   */
  async getProposalStatus(proposalId) {
    return await this.redis.get(REDIS_KEYS.status(proposalId));
  }
  
  /**
   * Check if user is in cooldown period
   */
  async checkCooldown(userWallet, proposalId) {
    const lastAction = await this.redis.get(REDIS_KEYS.cooldown(userWallet.toLowerCase(), proposalId));
    return lastAction !== null;
  }
  
  /**
   * Set cooldown for user action
   */
  async setCooldown(userWallet, proposalId, seconds = REDIS_TTL.RATE_LIMIT) {
    return await this.redis.setex(
      REDIS_KEYS.cooldown(userWallet.toLowerCase(), proposalId), 
      seconds, 
      Date.now().toString()
    );
  }
  
  /**
   * Get all votes for a proposal
   */
  async getAllVotes(proposalId) {
    const votes = await this.redis.hgetall(REDIS_KEYS.votes(proposalId));
    
    // Convert string values back to integers
    const convertedVotes = {};
    for (const [wallet, option] of Object.entries(votes || {})) {
      convertedVotes[wallet.toLowerCase()] = parseInt(option);
    }
    
    return convertedVotes;
  }
  
  /**
   * Get all delegations for a proposal
   */
  async getAllDelegations(proposalId) {
    const delegations = await this.redis.hgetall(REDIS_KEYS.delegations(proposalId)) || {};
    
    // Ensure consistent casing
    const normalizedDelegations = {};
    for (const [delegator, delegate] of Object.entries(delegations)) {
      normalizedDelegations[delegator.toLowerCase()] = delegate.toLowerCase();
    }
    
    return normalizedDelegations;
  }
  
  /**
   * Get all participants (voters + delegators)
   */
  async getAllParticipants(proposalId) {
    return await this.redis.smembers(REDIS_KEYS.participants(proposalId)) || [];
  }
  
  /**
   * Delete all data for a proposal (cleanup the corrupted data)
   */
  async deleteProposalData(proposalId) {
    const keys = REDIS_KEYS.allForProposal(proposalId);
    
    if (keys.length > 0) {
      return await this.redis.del(...keys);
    }
    
    return 0;
  }

  /**
  * Detect cycles in delegation chains using DFS
  * Returns { hasCycle: boolean, cyclePath?: string[], reason?: string }
  */
  async detectCyclesDFS(proposalId, delegatorWallet, targetWallet) {
    try {
      // Normalize wallet addresses
      const delegator = delegatorWallet.toLowerCase();
      const target = targetWallet.toLowerCase();
    
      // Prevent self-delegation (trivial cycle)
      if (delegator === target) {
        return { 
          hasCycle: true, 
          cyclePath: [delegator],
          reason: 'Cannot delegate to yourself' 
        };
      }
    
      // Load all current delegations for this proposal
      const allDelegations = await this.getAllDelegations(proposalId);
    
      // Simulate the new delegation being added
      const simulatedDelegations = { ...allDelegations, [delegator]: target };
    
      // Simple cycle detection: follow the chain from target to see if we reach delegator
      const visited = new Set();
      const path = [target];
      let current = target;
    
      // Follow delegation chain with safety limit
      let maxHops = 100; // Reasonable limit for delegation chain depth
    
      while (maxHops-- > 0) {
        // Check if we've already seen this wallet (existing cycle, but not necessarily our target)
        if (visited.has(current)) {
          // Existing cycle detected, breaking to avoid infinite loop
          break;
        }
      
        visited.add(current);
      
        // Check where current wallet delegates to
        const nextWallet = simulatedDelegations[current];
      
        if (!nextWallet) {
          // Dead end - no delegation
          break;
        }
      
        // Check if we've reached our original delegator (cycle found!)
        if (nextWallet === delegator) {
          const cyclePath = [...path, nextWallet];
          return {
            hasCycle: true,
            cyclePath,
            reason: `Circular delegation detected: ${cyclePath.join(' → ')}`
          };
        }
      
       // Continue following the chain
        path.push(nextWallet);
        current = nextWallet;
      }
    
      if (maxHops <= 0) {
        return {
          hasCycle: true,
          reason: 'Delegation chain too long (possible infinite loop)'
        };
      }
    
      return { hasCycle: false };
    
      } catch (error) {
        console.error('Error in DFS cycle detection:', error);
        return {
          hasCycle: true,
          reason: `Cycle detection failed due to error: ${error.message}`
        };
      }
    }
  }

/**
 * Create voting operations instance
 * Usage: const votingOps = createVotingOps(redisClient);
 */
export function createVotingOps(redisClient) {
  return new RedisVotingOps(redisClient);
}