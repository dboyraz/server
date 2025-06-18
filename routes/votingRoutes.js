import express from 'express';
import { proposalDb, userDb, supabase } from '../database/supabase.js';
import { requireJwtAuth } from '../middleware/jwtAuth.js';
import { createVotingOps } from '../utils/redisKeys.js';

// Get Redis client from server.js - we'll need to pass it differently
// For now, let's create a singleton pattern
let votingOpsInstance = null;

const getVotingOps = () => {
  if (!votingOpsInstance) {
    // Import Redis from server - this will be fixed in server.js update
    throw new Error('VotingOps not initialized - Redis client needed');
  }
  return votingOpsInstance;
};

// Initialize function to be called from server.js
export const initializeVotingOps = (redisClient) => {
  votingOpsInstance = createVotingOps(redisClient);
};

const router = express.Router();

// ================ VALIDATION HELPERS ================

/**
 * Validate user access to proposal
 */
const validateProposalAccess = async (proposalId, userWallet) => {
  try {
    // Get proposal details
    const proposal = await proposalDb.getById(proposalId);
    if (!proposal) {
      return { valid: false, error: 'Proposal not found', status: 404 };
    }

    // Get user details
    const user = await userDb.getByWallet(userWallet);
    if (!user || !user.organization_id) {
      return { valid: false, error: 'User not found or not in organization', status: 403 };
    }

    // Check organization match
    if (user.organization_id !== proposal.organization_id) {
      return { valid: false, error: 'Access denied - different organization', status: 403 };
    }

    // Check if proposal is still active
    const now = new Date();
    const deadline = new Date(proposal.voting_deadline);
    if (now >= deadline) {
      return { valid: false, error: 'Voting period has ended', status: 410 };
    }

    return { 
      valid: true, 
      proposal, 
      user,
      organization_id: user.organization_id
    };
  } catch (error) {
    console.error('Error validating proposal access:', error);
    return { valid: false, error: 'Validation failed', status: 500 };
  }
};

/**
 * Wrapper function for cycle detection that calls the Redis method
 */
const detectDelegationCycle = async (proposalId, delegatorWallet, targetWallet) => {
  try {
    console.log(`🔍 detectDelegationCycle wrapper: ${delegatorWallet} → ${targetWallet}`);
    
    // Get the voting operations instance and call the DFS method
    const votingOps = getVotingOps();
    const result = await votingOps.detectCyclesDFS(proposalId, delegatorWallet, targetWallet);
    
    console.log(`🔍 detectDelegationCycle wrapper result:`, result);
    return result;
    
  } catch (error) {
    console.error('Error in detectDelegationCycle wrapper:', error);
    return { 
      hasCycle: true, 
      reason: 'Cycle detection failed - delegation not allowed for safety' 
    };
  }
};

/**
 * Set delegation to another user
 * POST /api/proposals/:id/delegate
 * Body: { target_user: string } // unique_id
 */
router.post('/:id/delegate', requireJwtAuth, async (req, res) => {
  try {
    const { id: proposalId } = req.params;
    const { target_user } = req.body;
    const userWallet = req.walletAddress;

    // Validate UUID format
    const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if (!uuidPattern.test(proposalId)) {
      return res.status(400).json({ error: 'Invalid proposal ID format' });
    }

    // Validate target user
    if (!target_user || typeof target_user !== 'string' || !target_user.trim()) {
      return res.status(400).json({ error: 'Target user unique_id is required' });
    }

    // Check rate limiting
    const rateLimited = await checkRateLimit(userWallet, proposalId);
    if (rateLimited) {
      return res.status(429).json({ 
        error: 'Rate limit exceeded - please wait before making another action',
        cooldown_seconds: 60
      });
    }

    // Validate proposal access
    const validation = await validateProposalAccess(proposalId, userWallet);
    if (!validation.valid) {
      return res.status(validation.status).json({ error: validation.error });
    }

    const { user, organization_id } = validation;

    // Find target user by unique_id in same organization
    const { data: targetUser, error: targetError } = await supabase
      .from('users')
      .select('wallet_address, unique_id, first_name, last_name')
      .eq('unique_id', target_user.toLowerCase())
      .eq('organization_id', organization_id)
      .single();

    if (targetError || !targetUser) {
      return res.status(404).json({ 
        error: 'Target user not found in your organization' 
      });
    }

    const targetWallet = targetUser.wallet_address;

    // Check for delegation cycles
    const votingOps = getVotingOps();
    const cycleCheck = await votingOps.detectCyclesDFS(proposalId, userWallet, targetWallet);
    
    if (cycleCheck.hasCycle) {
      return res.status(400).json({ 
        error: `Delegation not allowed: ${cycleCheck.reason}` 
      });
    }

    // Get current user status
    const currentStatus = await votingOps.getUserStatus(proposalId, userWallet);

    // Set delegation (this removes any existing vote automatically)
    await votingOps.setDelegation(proposalId, userWallet, targetWallet);

    // Set rate limit
    await votingOps.setCooldown(userWallet, proposalId);

    // Create audit trail
    await createAuditEntry(
      proposalId, 
      userWallet, 
      organization_id, 
      'delegate', 
      target_user.toLowerCase(),
      targetWallet
    );

    // If user had a vote, create remove_vote audit entry
    if (currentStatus.has_voted) {
      await createAuditEntry(
        proposalId,
        userWallet,
        organization_id,
        'remove_vote'
      );
    }

    console.log(`✅ Delegation set: ${user.unique_id} delegated to ${targetUser.unique_id} on proposal ${proposalId}`);

    res.status(200).json({
      success: true,
      message: 'Delegation set successfully',
      delegation: {
        target_user: targetUser.unique_id,
        target_name: `${targetUser.first_name} ${targetUser.last_name}`,
        delegated_at: new Date().toISOString()
      },
      previous_action: currentStatus.has_voted ? 'vote_removed' : currentStatus.has_delegated ? 'delegation_updated' : 'none'
    });

  } catch (error) {
    console.error('Error setting delegation:', error);
    res.status(500).json({ error: 'Failed to set delegation' });
  }
});

/**
 * Create audit trail entry
 */
const createAuditEntry = async (proposalId, userWallet, organizationId, actionType, target = null, targetWallet = null) => {
  try {
    const { error } = await supabase
      .from('vote_audit')
      .insert({
        proposal_id: proposalId,
        user_wallet: userWallet.toLowerCase(),
        organization_id: organizationId,
        action_type: actionType,
        target: target,
        target_wallet: targetWallet ? targetWallet.toLowerCase() : null
      });

    if (error) {
      console.error('Audit trail creation failed:', error);
    }
  } catch (error) {
    console.error('Error creating audit entry:', error);
  }
};

/**
 * Rate limiting check (60-second cooldown)
 */
const checkRateLimit = async (userWallet, proposalId) => {
  const votingOps = getVotingOps();
  const inCooldown = await votingOps.checkCooldown(userWallet, proposalId);
  return inCooldown;
};

// ================ VOTING ENDPOINTS ================

/**
 * Cast a vote for a proposal option
 * POST /api/proposals/:id/vote
 * Body: { option_number: number }
 */
router.post('/:id/vote', requireJwtAuth, async (req, res) => {
  try {
    const { id: proposalId } = req.params;
    const { option_number } = req.body;
    const userWallet = req.walletAddress;

    // Validate UUID format
    const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if (!uuidPattern.test(proposalId)) {
      return res.status(400).json({ error: 'Invalid proposal ID format' });
    }

    // Validate option number
    if (!Number.isInteger(option_number) || option_number < 1) {
      return res.status(400).json({ error: 'Invalid option number' });
    }

    // Check rate limiting
    const rateLimited = await checkRateLimit(userWallet, proposalId);
    if (rateLimited) {
      return res.status(429).json({ 
        error: 'Rate limit exceeded - please wait before making another action',
        cooldown_seconds: 60
      });
    }

    // Validate proposal access
    const validation = await validateProposalAccess(proposalId, userWallet);
    if (!validation.valid) {
      return res.status(validation.status).json({ error: validation.error });
    }

    const { proposal, user, organization_id } = validation;

    // Check if option exists
    const optionExists = proposal.options?.some(opt => opt.option_number === option_number);
    if (!optionExists) {
      return res.status(400).json({ error: `Option ${option_number} does not exist` });
    }

    // Get current user status
    const votingOps = getVotingOps();
    const currentStatus = await votingOps.getUserStatus(proposalId, userWallet);

    // Cast vote (this removes any existing delegation automatically)
    await votingOps.castVote(proposalId, userWallet, option_number);

    // Set rate limit
    await votingOps.setCooldown(userWallet, proposalId);

    // Create audit trail
    await createAuditEntry(
      proposalId, 
      userWallet, 
      organization_id, 
      'vote', 
      option_number.toString()
    );

    // If user had a delegation, create remove_delegation audit entry
    if (currentStatus.has_delegated) {
      await createAuditEntry(
        proposalId,
        userWallet,
        organization_id,
        'remove_delegation'
      );
    }

    console.log(`✅ Vote cast: ${user.unique_id} voted for option ${option_number} on proposal ${proposalId}`);

    res.status(200).json({
      success: true,
      message: 'Vote cast successfully',
      vote: {
        option_number,
        voted_at: new Date().toISOString()
      },
      previous_action: currentStatus.has_delegated ? 'delegation_removed' : currentStatus.has_voted ? 'vote_updated' : 'none'
    });

  } catch (error) {
    console.error('Error casting vote:', error);
    res.status(500).json({ error: 'Failed to cast vote' });
  }
});

/**
 * Remove user's vote
 * DELETE /api/proposals/:id/vote
 */
router.delete('/:id/vote', requireJwtAuth, async (req, res) => {
  try {
    const { id: proposalId } = req.params;
    const userWallet = req.walletAddress;

    // Validate UUID format
    const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if (!uuidPattern.test(proposalId)) {
      return res.status(400).json({ error: 'Invalid proposal ID format' });
    }

    // Check rate limiting
    const rateLimited = await checkRateLimit(userWallet, proposalId);
    if (rateLimited) {
      return res.status(429).json({ 
        error: 'Rate limit exceeded - please wait before making another action',
        cooldown_seconds: 60
      });
    }

    // Validate proposal access
    const validation = await validateProposalAccess(proposalId, userWallet);
    if (!validation.valid) {
      return res.status(validation.status).json({ error: validation.error });
    }

    const { user, organization_id } = validation;

    // Check if user has a vote to remove
    const votingOps = getVotingOps();
    const currentVote = await votingOps.getUserVote(proposalId, userWallet);
    if (currentVote === null) {
      return res.status(404).json({ error: 'No vote found to remove' });
    }

    // Remove vote
    await votingOps.removeVote(proposalId, userWallet);

    // Set rate limit
    await votingOps.setCooldown(userWallet, proposalId);

    // Create audit trail
    await createAuditEntry(
      proposalId, 
      userWallet, 
      organization_id, 
      'remove_vote'
    );

    console.log(`✅ Vote removed: ${user.unique_id} removed vote from proposal ${proposalId}`);

    res.status(200).json({
      success: true,
      message: 'Vote removed successfully',
      removed_vote: {
        option_number: currentVote,
        removed_at: new Date().toISOString()
      }
    });

  } catch (error) {
    console.error('Error removing vote:', error);
    res.status(500).json({ error: 'Failed to remove vote' });
  }
});

/**
 * Set delegation to another user
 * POST /api/proposals/:id/delegate
 * Body: { target_user: string } // unique_id
 */
router.post('/:id/delegate', requireJwtAuth, async (req, res) => {
  try {
    const { id: proposalId } = req.params;
    const { target_user } = req.body;
    const userWallet = req.walletAddress;

    // Validate UUID format
    const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if (!uuidPattern.test(proposalId)) {
      return res.status(400).json({ error: 'Invalid proposal ID format' });
    }

    // Validate target user
    if (!target_user || typeof target_user !== 'string' || !target_user.trim()) {
      return res.status(400).json({ error: 'Target user unique_id is required' });
    }

    // Check rate limiting
    const rateLimited = await checkRateLimit(userWallet, proposalId);
    if (rateLimited) {
      return res.status(429).json({ 
        error: 'Rate limit exceeded - please wait before making another action',
        cooldown_seconds: 60
      });
    }

    // Validate proposal access
    const validation = await validateProposalAccess(proposalId, userWallet);
    if (!validation.valid) {
      return res.status(validation.status).json({ error: validation.error });
    }

    const { user, organization_id } = validation;

    // Find target user by unique_id in same organization
    const { data: targetUser, error: targetError } = await supabase
      .from('users')
      .select('wallet_address, unique_id, first_name, last_name')
      .eq('unique_id', target_user.toLowerCase())
      .eq('organization_id', organization_id)
      .single();

    if (targetError || !targetUser) {
      return res.status(404).json({ 
        error: 'Target user not found in your organization' 
      });
    }

    const targetWallet = targetUser.wallet_address;

    // Check for delegation cycles
    const cycleCheck = await detectDelegationCycle(proposalId, userWallet, targetWallet);
    if (cycleCheck.hasCycle) {
      return res.status(400).json({ 
        error: `Delegation not allowed: ${cycleCheck.reason}` 
      });
    }

    // Get current user status
    const votingOps = getVotingOps();
    const currentStatus = await votingOps.getUserStatus(proposalId, userWallet);

    // Set delegation (this removes any existing vote automatically)
    await votingOps.setDelegation(proposalId, userWallet, targetWallet);

    // Set rate limit
    await votingOps.setCooldown(userWallet, proposalId);

    // Create audit trail
    await createAuditEntry(
      proposalId, 
      userWallet, 
      organization_id, 
      'delegate', 
      target_user.toLowerCase(),
      targetWallet
    );

    // If user had a vote, create remove_vote audit entry
    if (currentStatus.has_voted) {
      await createAuditEntry(
        proposalId,
        userWallet,
        organization_id,
        'remove_vote'
      );
    }

    console.log(`✅ Delegation set: ${user.unique_id} delegated to ${targetUser.unique_id} on proposal ${proposalId}`);

    res.status(200).json({
      success: true,
      message: 'Delegation set successfully',
      delegation: {
        target_user: targetUser.unique_id,
        target_name: `${targetUser.first_name} ${targetUser.last_name}`,
        delegated_at: new Date().toISOString()
      },
      previous_action: currentStatus.has_voted ? 'vote_removed' : currentStatus.has_delegated ? 'delegation_updated' : 'none'
    });

  } catch (error) {
    console.error('Error setting delegation:', error);
    res.status(500).json({ error: 'Failed to set delegation' });
  }
});

/**
 * Remove user's delegation
 * DELETE /api/proposals/:id/delegate
 */
router.delete('/:id/delegate', requireJwtAuth, async (req, res) => {
  try {
    const { id: proposalId } = req.params;
    const userWallet = req.walletAddress;

    // Validate UUID format
    const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if (!uuidPattern.test(proposalId)) {
      return res.status(400).json({ error: 'Invalid proposal ID format' });
    }

    // Check rate limiting
    const rateLimited = await checkRateLimit(userWallet, proposalId);
    if (rateLimited) {
      return res.status(429).json({ 
        error: 'Rate limit exceeded - please wait before making another action',
        cooldown_seconds: 60
      });
    }

    // Validate proposal access
    const validation = await validateProposalAccess(proposalId, userWallet);
    if (!validation.valid) {
      return res.status(validation.status).json({ error: validation.error });
    }

    const { user, organization_id } = validation;

    // Check if user has a delegation to remove
    const votingOps = getVotingOps();
    const currentDelegation = await votingOps.getUserDelegation(proposalId, userWallet);
    if (!currentDelegation) {
      return res.status(404).json({ error: 'No delegation found to remove' });
    }

    // Get target user info for response
    const { data: targetUser } = await supabase
      .from('users')
      .select('unique_id, first_name, last_name')
      .eq('wallet_address', currentDelegation)
      .single();

    // Remove delegation
    await votingOps.removeDelegation(proposalId, userWallet);

    // Set rate limit
    await votingOps.setCooldown(userWallet, proposalId);

    // Create audit trail
    await createAuditEntry(
      proposalId, 
      userWallet, 
      organization_id, 
      'remove_delegation'
    );

    console.log(`✅ Delegation removed: ${user.unique_id} removed delegation from proposal ${proposalId}`);

    res.status(200).json({
      success: true,
      message: 'Delegation removed successfully',
      removed_delegation: {
        target_user: targetUser?.unique_id || 'unknown',
        target_name: targetUser ? `${targetUser.first_name} ${targetUser.last_name}` : 'Unknown User',
        removed_at: new Date().toISOString()
      }
    });

  } catch (error) {
    console.error('Error removing delegation:', error);
    res.status(500).json({ error: 'Failed to remove delegation' });
  }
});

/**
 * Get user's current voting status for a proposal (works for both active and expired)
 * GET /api/proposals/:id/voting-status
 */
router.get('/:id/voting-status', requireJwtAuth, async (req, res) => {
  try {
    const { id: proposalId } = req.params;
    const userWallet = req.walletAddress;

    // Validate UUID format
    const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if (!uuidPattern.test(proposalId)) {
      return res.status(400).json({ error: 'Invalid proposal ID format' });
    }

    // Use a custom validation that doesn't check deadline for status endpoint
    const proposal = await proposalDb.getById(proposalId);
    if (!proposal) {
      return res.status(404).json({ error: 'Proposal not found' });
    }

    // Check if user is in the same organization
    const user = await userDb.getByWallet(userWallet);
    if (!user || !user.organization_id) {
      return res.status(403).json({ error: 'User not found or not in organization' });
    }

    if (user.organization_id !== proposal.organization_id) {
      return res.status(403).json({ error: 'Access denied - different organization' });
    }

    // Get user's current status (works for both active and expired proposals)
    const votingOps = getVotingOps();
    const userStatus = await votingOps.getUserStatus(proposalId, userWallet);

    // Get target user info if delegated
    let delegateInfo = null;
    if (userStatus.has_delegated) {
      const { data: targetUser } = await supabase
        .from('users')
        .select('unique_id, first_name, last_name')
        .eq('wallet_address', userStatus.delegate_wallet)
        .single();

      if (targetUser) {
        delegateInfo = {
          unique_id: targetUser.unique_id,
          name: `${targetUser.first_name} ${targetUser.last_name}`
        };
      }
    }

    // Get selected option info if voted
    let selectedOption = null;
    if (userStatus.has_voted) {
      selectedOption = proposal.options?.find(opt => opt.option_number === userStatus.voted_option);
    }

    // Calculate time remaining and active status
    const now = new Date();
    const deadline = new Date(proposal.voting_deadline);
    const timeRemainingMs = deadline.getTime() - now.getTime();
    const timeRemaining = Math.max(0, Math.floor(timeRemainingMs / 1000)); // seconds
    const isActive = timeRemaining > 0;

    // Check rate limiting status (only relevant for active proposals)
    const inCooldown = isActive ? await checkRateLimit(userWallet, proposalId) : false;

    res.status(200).json({
      proposal_id: proposalId,
      proposal_title: proposal.title,
      voting_deadline: proposal.voting_deadline,
      time_remaining_seconds: timeRemaining,
      is_active: isActive,
      user_status: {
        has_voted: userStatus.has_voted,
        voted_option: userStatus.voted_option,
        selected_option: selectedOption,
        has_delegated: userStatus.has_delegated,
        delegate_info: delegateInfo,
        can_act: !inCooldown && isActive, // Can only act if active and not in cooldown
        cooldown_active: inCooldown
      },
      options: proposal.options || []
    });

  } catch (error) {
    console.error('Error getting voting status:', error);
    res.status(500).json({ error: 'Failed to get voting status' });
  }
});

/**
 * Create voting routes factory
 */
export const createVotingRoutes = () => {
  return router;
};

export default createVotingRoutes;