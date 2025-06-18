import { REDIS_KEYS } from './redisKeys.js';
import { supabase } from '../database/supabase.js';

/**
 * Redis Backup & Recovery System
 * Provides periodic snapshots and disaster recovery capabilities
 */
export class RedisBackupManager {
  constructor(redisClient) {
    this.redis = redisClient;
  }

  // ========== SNAPSHOT CREATION ==========

  /**
   * Create a complete snapshot of proposal voting data
   * @param {string} proposalId - UUID of the proposal
   * @param {string} snapshotType - 'hourly' | 'pre_calculation' | 'manual'
   */
  async createSnapshot(proposalId, snapshotType = 'hourly') {
    try {
      console.log(`📸 Creating ${snapshotType} snapshot for proposal ${proposalId}`);

      // Gather all voting data from Redis
      const [votes, delegations, participants, status, deadline] = await Promise.all([
        this.redis.hgetall(REDIS_KEYS.votes(proposalId)),
        this.redis.hgetall(REDIS_KEYS.delegations(proposalId)),
        this.redis.smembers(REDIS_KEYS.participants(proposalId)),
        this.redis.get(REDIS_KEYS.status(proposalId)),
        this.redis.get(REDIS_KEYS.deadline(proposalId))
      ]);

      const votesData = votes || {};
      const delegationsData = delegations || {};
      const participantsData = participants || [];

      // Create snapshot data structure
      const snapshotData = {
        timestamp: new Date().toISOString(),
        votes: votesData,
        delegations: delegationsData,
        participants: participantsData,
        status,
        deadline,
        vote_count: Object.keys(votesData).length,
        delegation_count: Object.keys(delegationsData).length,
        participant_count: participantsData.length
      };

      // Store snapshot in PostgreSQL
      const { data, error } = await supabase
        .from('redis_snapshots')
        .insert({
          proposal_id: proposalId,
          redis_data: snapshotData,
          snapshot_type: snapshotType
        })
        .select('snapshot_id')
        .single();

      if (error) {
        throw error;
      }

      console.log(`✅ ${snapshotType} snapshot created for proposal ${proposalId} (ID: ${data.snapshot_id})`);
      return {
        success: true,
        snapshot_id: data.snapshot_id,
        data: snapshotData
      };

    } catch (error) {
      console.error(`❌ Snapshot creation failed for proposal ${proposalId}:`, error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Create snapshots for multiple proposals (batch operation)
   */
  async createBatchSnapshots(proposalIds, snapshotType = 'hourly') {
    const results = [];
    
    for (const proposalId of proposalIds) {
      try {
        const result = await this.createSnapshot(proposalId, snapshotType);
        results.push({ proposalId, ...result });
      } catch (error) {
        results.push({ 
          proposalId, 
          success: false, 
          error: error.message 
        });
      }
    }
    
    return results;
  }

  // ========== SNAPSHOT RECOVERY ==========

  /**
   * Restore proposal data from snapshot
   * @param {string} proposalId - UUID of the proposal
   * @param {string} snapshotId - Specific snapshot ID (optional - uses latest if not provided)
   */
  async restoreFromSnapshot(proposalId, snapshotId = null) {
    try {
      console.log(`🔄 Restoring proposal ${proposalId} from snapshot...`);

      // Get snapshot data
      let query = supabase
        .from('redis_snapshots')
        .select('snapshot_id, redis_data, snapshot_type, snapshot_at')
        .eq('proposal_id', proposalId);

      if (snapshotId) {
        query = query.eq('snapshot_id', snapshotId);
      } else {
        query = query.order('snapshot_at', { ascending: false }).limit(1);
      }

      const { data: snapshot, error } = await query.single();

      if (error || !snapshot) {
        throw new Error(`No snapshot found for proposal ${proposalId}`);
      }

      const snapshotData = snapshot.redis_data;
      
      if (!snapshotData.data) {
        throw new Error('Invalid snapshot data structure');
      }

      // Restore Redis data using pipeline for efficiency
      const pipeline = this.redis.pipeline();

      // Clear existing data first
      const keysToDelete = REDIS_KEYS.allForProposal(proposalId);
      for (const key of keysToDelete) {
        pipeline.del(key);
      }

      // Restore votes
      if (snapshotData.data.votes && Object.keys(snapshotData.data.votes).length > 0) {
        pipeline.hset(REDIS_KEYS.votes(proposalId), snapshotData.data.votes);
      }

      // Restore delegations
      if (snapshotData.data.delegations && Object.keys(snapshotData.data.delegations).length > 0) {
        pipeline.hset(REDIS_KEYS.delegations(proposalId), snapshotData.data.delegations);
      }

      // Restore participants
      if (snapshotData.data.participants && snapshotData.data.participants.length > 0) {
        pipeline.sadd(REDIS_KEYS.participants(proposalId), ...snapshotData.data.participants);
      }

      // Restore status and deadline
      if (snapshotData.data.status) {
        pipeline.set(REDIS_KEYS.status(proposalId), snapshotData.data.status);
      }

      if (snapshotData.data.deadline) {
        pipeline.set(REDIS_KEYS.deadline(proposalId), snapshotData.data.deadline);
      }

      // Execute all operations
      await pipeline.exec();

      console.log(`✅ Restored proposal ${proposalId} from snapshot ${snapshot.snapshot_id}`);
      return {
        success: true,
        snapshot_id: snapshot.snapshot_id,
        snapshot_type: snapshot.snapshot_type,
        restored_data: snapshotData.data.metadata
      };

    } catch (error) {
      console.error(`❌ Recovery failed for proposal ${proposalId}:`, error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  // ========== SNAPSHOT MANAGEMENT ==========

  /**
   * Get snapshot history for a proposal
   */
  async getSnapshotHistory(proposalId, limit = 10) {
    try {
      const { data, error } = await supabase
        .from('redis_snapshots')
        .select('snapshot_id, snapshot_type, snapshot_at, redis_data->data->metadata')
        .eq('proposal_id', proposalId)
        .order('snapshot_at', { ascending: false })
        .limit(limit);

      if (error) throw error;

      return {
        success: true,
        snapshots: data || []
      };
    } catch (error) {
      console.error(`Error getting snapshot history for ${proposalId}:`, error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Clean up old snapshots (keep last 10 hourly, all pre_calculation and manual)
   */
  async cleanupOldSnapshots(proposalId) {
    try {
      // Keep all pre_calculation and manual snapshots
      // Only clean up hourly snapshots (keep latest 10)
      const { data: hourlySnapshots } = await supabase
        .from('redis_snapshots')
        .select('snapshot_id')
        .eq('proposal_id', proposalId)
        .eq('snapshot_type', 'hourly')
        .order('snapshot_at', { ascending: false })
        .range(10, 1000); // Skip first 10, get the rest

      if (hourlySnapshots && hourlySnapshots.length > 0) {
        const snapshotIds = hourlySnapshots.map(s => s.snapshot_id);
        
        const { error } = await supabase
          .from('redis_snapshots')
          .delete()
          .in('snapshot_id', snapshotIds);

        if (error) throw error;

        console.log(`🧹 Cleaned up ${snapshotIds.length} old hourly snapshots for proposal ${proposalId}`);
      }

      return { success: true, cleaned: hourlySnapshots?.length || 0 };
    } catch (error) {
      console.error(`Error cleaning up snapshots for ${proposalId}:`, error);
      return { success: false, error: error.message };
    }
  }

  // ========== AUTOMATED BACKUP JOBS ==========

  /**
   * Create hourly snapshots for all active proposals
   */
  async createHourlySnapshots() {
    try {
      console.log('🔄 Starting hourly snapshot job...');

      // Get all active proposals (voting deadline in future)
      const { data: activeProposals, error } = await supabase
        .from('proposals')
        .select('proposal_id')
        .gt('voting_deadline', new Date().toISOString());

      if (error) throw error;

      if (!activeProposals || activeProposals.length === 0) {
        console.log('📭 No active proposals found for hourly snapshot');
        return { success: true, processed: 0 };
      }

      // Filter proposals that have Redis data (have participants)
      const proposalsWithData = [];
      for (const proposal of activeProposals) {
        const participantCount = await this.redis.scard(REDIS_KEYS.participants(proposal.proposal_id));
        if (participantCount > 0) {
          proposalsWithData.push(proposal.proposal_id);
        }
      }

      if (proposalsWithData.length === 0) {
        console.log('📭 No active proposals with voting data found');
        return { success: true, processed: 0 };
      }

      // Create snapshots for proposals with data
      const results = await this.createBatchSnapshots(proposalsWithData, 'hourly');
      
      const successCount = results.filter(r => r.success).length;
      const failureCount = results.filter(r => !r.success).length;

      console.log(`✅ Hourly snapshot job completed: ${successCount} success, ${failureCount} failures`);

      return {
        success: true,
        processed: results.length,
        successes: successCount,
        failures: failureCount,
        results
      };

    } catch (error) {
      console.error('❌ Hourly snapshot job failed:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Data integrity verification
   */
  async verifyProposalData(proposalId) {
    try {
      const [votes, delegations] = await Promise.all([
        this.redis.hgetall(REDIS_KEYS.votes(proposalId)) || {},
        this.redis.hgetall(REDIS_KEYS.delegations(proposalId)) || {}
      ]);

      const issues = [];

      // Check for users with both vote and delegation (should not exist)
      for (const wallet of Object.keys(votes)) {
        if (delegations[wallet]) {
          issues.push(`User ${wallet} has both vote and delegation`);
        }
      }

      // Check for self-delegations
      for (const [delegator, delegate] of Object.entries(delegations)) {
        if (delegator === delegate) {
          issues.push(`User ${delegator} delegates to themselves`);
        }
      }

      return {
        valid: issues.length === 0,
        issues,
        stats: {
          vote_count: Object.keys(votes).length,
          delegation_count: Object.keys(delegations).length
        }
      };

    } catch (error) {
      return {
        valid: false,
        issues: [`Verification failed: ${error.message}`],
        stats: null
      };
    }
  }
}

/**
 * Create backup manager instance
 */
export function createBackupManager(redisClient) {
  return new RedisBackupManager(redisClient);
}

/**
 * Initialize automated backup system
 * Call this in server.js to set up periodic snapshots
 */
export function initializeBackupSystem(redisClient) {
  const backupManager = createBackupManager(redisClient);

  // Hourly snapshot job
  const hourlyInterval = setInterval(async () => {
    await backupManager.createHourlySnapshots();
  }, 60 * 60 * 1000); // Every hour

  console.log('🔄 Redis backup system initialized - hourly snapshots enabled');

  // Return cleanup function
  return () => {
    clearInterval(hourlyInterval);
    console.log('🛑 Redis backup system stopped');
  };
}