import { createDelegationEngine } from './delegationEngine.js';
import { createFinalVoteEngine } from './finalVoteEngine.js';
import { supabase } from '../database/supabase.js';

// Store active timeouts for cleanup
const activeTimeouts = new Map();

const SCHEDULE_WINDOW_HOURS = 36;
const CHECK_INTERVAL_HOURS = 24;

// Process a single proposal with both delegation resolution AND final vote counting
const processProposal = async (proposalId, redis) => {
  try {
    console.log(`🔄 Auto-finalizing proposal ${proposalId}`);
    
    // Step 1: Delegation Resolution
    console.log(`📊 Starting delegation resolution for proposal ${proposalId}`);
    const delegationEngine = createDelegationEngine(redis);
    const delegationResult = await delegationEngine.resolveBulkDelegations(proposalId);
    
    if (delegationResult.success) {
      console.log(`✅ Delegation resolution completed for proposal ${proposalId}`);
      
      // Step 2: Final Vote Counting (only if delegation resolution succeeded)
      console.log(`🗳️ Starting final vote counting for proposal ${proposalId}`);
      const voteEngine = createFinalVoteEngine(redis);
      const voteResult = await voteEngine.calculateFinalResults(proposalId);
      
      if (voteResult.success) {
        console.log(`✅ Final vote counting completed for proposal ${proposalId}`);
        console.log(`🏆 Proposal ${proposalId} fully processed - delegation + vote counting complete`);
        
        // Log summary for monitoring
        const summary = voteResult.results.metadata;
        console.log(`📈 Final Results: ${summary.totalVotesCast}/${summary.totalVotingPower} voting power used, winning option: ${summary.winningOption || 'TIE/NO_VOTES'}`);
      } else {
        console.error(`❌ Final vote counting failed for proposal ${proposalId}:`, voteResult.error);
        // Note: We don't fail the entire process if vote counting fails
        // The delegation resolution is still saved and valid
      }
    } else {
      console.error(`❌ Delegation resolution failed for proposal ${proposalId}:`, delegationResult.error);
      // Don't proceed to vote counting if delegation resolution failed
      // This ensures we don't have incorrect voting power calculations
    }
  } catch (error) {
    console.error(`❌ Error processing proposal ${proposalId}:`, error);
  }
};

// Schedule a single proposal finalization
const scheduleProposalFinalization = (proposalId, deadline, redis) => {
  const delay = new Date(deadline) - Date.now();
  
  if (delay <= 0) {
    // Already expired, process immediately
    processProposal(proposalId, redis);
    return;
  }
  
  const timeoutId = setTimeout(() => {
    processProposal(proposalId, redis);
    activeTimeouts.delete(proposalId);
  }, delay);
  
  activeTimeouts.set(proposalId, timeoutId);
  console.log(`⏰ Scheduled proposal ${proposalId} to finalize in ${Math.round(delay/1000/60)} minutes`);
};

// Check for expired but unprocessed proposals
export const checkExpiredProposals = async (redis) => {
  try {
    console.log('🔍 Checking for expired proposals...');
    
    // Get completed delegation resolutions
    const { data: completedDelegations, error: delegationError } = await supabase
      .from('delegation_resolution_audit')
      .select('proposal_id')
      .eq('status', 'completed');

    if (delegationError) {
      console.error('❌ Error getting completed delegation resolutions:', delegationError);
      return;
    }

    // Get completed vote counts
    const { data: completedVoteCounts, error: voteError } = await supabase
      .from('final_vote_results_audit')
      .select('proposal_id')
      .eq('status', 'completed');

    if (voteError) {
      console.error('❌ Error getting completed vote counts:', voteError);
      return;
    }

    const completedDelegationIds = completedDelegations?.map(p => p.proposal_id) || [];
    const completedVoteIds = completedVoteCounts?.map(p => p.proposal_id) || [];
    
    // Find proposals that are fully completed (both delegation AND vote counting)
    const fullyCompletedIds = completedDelegationIds.filter(id => 
      completedVoteIds.includes(id)
    );
    
    console.log(`📋 Found ${completedDelegationIds.length} completed delegations, ${completedVoteIds.length} completed vote counts`);
    console.log(`✅ ${fullyCompletedIds.length} proposals fully completed`);

    // Get expired proposals excluding fully completed ones
    let query = supabase
      .from('proposals')
      .select('proposal_id, voting_deadline, title')
      .lt('voting_deadline', new Date().toISOString());

    if (fullyCompletedIds.length > 0) {
      query = query.not('proposal_id', 'in', `(${fullyCompletedIds.map(id => `"${id}"`).join(',')})`);
    }

    const { data: expiredProposals, error } = await query;

    if (error) {
      console.error('❌ Query error:', error);
      return;
    }

    console.log(`📊 Found ${expiredProposals?.length || 0} unprocessed expired proposals`);
    
    if (!expiredProposals || expiredProposals.length === 0) {
      console.log('✅ No expired proposals to process');
      return;
    }

    // Process each expired proposal
    for (const proposal of expiredProposals) {
      console.log(`🔄 Processing expired proposal ${proposal.proposal_id} ("${proposal.title}")`);
      await processProposal(proposal.proposal_id, redis);
    }
  } catch (error) {
    console.error('❌ Scheduler error:', error);
  }
};

// Schedule proposals ending in next 36 hours only
const scheduleNearTermProposals = async (redis) => {
  try {
    console.log('🔄 Scheduling proposals ending in next 36 hours...');
    
    const now = new Date();
    const windowEnd = new Date(now.getTime() + SCHEDULE_WINDOW_HOURS * 60 * 60 * 1000);
    
    // Get fully completed proposal IDs (both delegation and vote counting)
    const { data: completedDelegations } = await supabase
      .from('delegation_resolution_audit')
      .select('proposal_id')
      .eq('status', 'completed');
    
    const { data: completedVoteCounts } = await supabase
      .from('final_vote_results_audit')
      .select('proposal_id')
      .eq('status', 'completed');
    
    const completedDelegationIds = completedDelegations?.map(p => p.proposal_id) || [];
    const completedVoteIds = completedVoteCounts?.map(p => p.proposal_id) || [];
    
    const fullyCompletedIds = completedDelegationIds.filter(id => 
      completedVoteIds.includes(id)
    );
    
    // Get proposals expiring in next 36 hours excluding fully completed ones
    let query = supabase
      .from('proposals')
      .select('proposal_id, voting_deadline, title')
      .gte('voting_deadline', now.toISOString())
      .lte('voting_deadline', windowEnd.toISOString());
    
    if (fullyCompletedIds.length > 0) {
      query = query.not('proposal_id', 'in', `(${fullyCompletedIds.map(id => `"${id}"`).join(',')})`);
    }
    
    const { data: nearTermProposals, error } = await query;
    
    if (error) {
      console.error('❌ Error getting near-term proposals:', error);
      return;
    }
    
    console.log(`📅 Found ${nearTermProposals?.length || 0} proposals to schedule in 36h window`);
    
    nearTermProposals?.forEach(proposal => {
      scheduleProposalFinalization(proposal.proposal_id, proposal.voting_deadline, redis);
    });
    
  } catch (error) {
    console.error('❌ Error scheduling near-term proposals:', error);
  }
};

// Main scheduling function - handles both expired and near-term proposals
const runSchedulerCycle = async (redis) => {
  console.log('🚀 Running proposal scheduler cycle...');
  
  // First process any expired proposals
  await checkExpiredProposals(redis);
  
  // Then schedule near-term proposals  
  await scheduleNearTermProposals(redis);
  
  console.log('✅ Proposal scheduler cycle completed');
};

export const startProposalScheduler = (redis) => {
  // Run immediately on startup
  runSchedulerCycle(redis);
  
  // Re-run every 24 hours
  setInterval(() => runSchedulerCycle(redis), CHECK_INTERVAL_HOURS * 60 * 60 * 1000);
  
  console.log('📅 Proposal scheduler started - processes expired + schedules 36h window, 24h refresh');
  console.log('🔄 Scheduler now handles: delegation resolution → final vote counting');
};