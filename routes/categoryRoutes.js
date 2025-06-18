import express from 'express';
import { categoryDb, userDb, proposalDb, supabase } from '../database/supabase.js';
import { requireJwtAuth } from '../middleware/jwtAuth.js';

const router = express.Router();

// ================ VALIDATION HELPERS ================

/**
 * Validate category data
 */
const validateCategoryData = (data) => {
  const errors = [];
  
  // Title validation
  if (!data.title || typeof data.title !== 'string') {
    errors.push('Title is required');
  } else {
    const trimmedTitle = data.title.trim();
    if (trimmedTitle.length < 5) {
      errors.push('Title must be at least 5 characters');
    } else if (trimmedTitle.length > 30) {
      errors.push('Title must be less than 30 characters');
    }
  }
  
  // Description validation
  if (!data.description || typeof data.description !== 'string') {
    errors.push('Description is required');
  } else {
    const trimmedDescription = data.description.trim();
    if (trimmedDescription.length < 50) {
      errors.push('Description must be at least 50 characters');
    } else if (trimmedDescription.length > 1000) {
      errors.push('Description must be less than 1000 characters');
    }
  }
  
  return errors;
};

/**
 * Validate suggestion data
 */
const validateSuggestionData = (data) => {
  const errors = [];
  
  // Check if suggestion_type is provided and valid
  if (!data.suggestion_type || !['delegate', 'vote_option'].includes(data.suggestion_type)) {
    errors.push('Suggestion type must be either "delegate" or "vote_option"');
  }
  
  // For delegation suggestions, target_user (unique_id) is required
  if (data.suggestion_type === 'delegate') {
    if (!data.target_user || typeof data.target_user !== 'string' || data.target_user.trim().length === 0) {
      errors.push('Target user unique ID is required for delegation suggestions');
    }
  }
  
  // For voting suggestions, target_option_number is required
  if (data.suggestion_type === 'vote_option') {
    if (!data.target_option_number || !Number.isInteger(data.target_option_number) || data.target_option_number < 1) {
      errors.push('Target option number is required for voting suggestions and must be a positive integer');
    }
  }
  
  return errors;
};

/**
 * Check if suggestions are still allowed for a proposal (not within 1 hour of deadline)
 */
const canCreateSuggestions = (votingDeadline) => {
  const now = new Date();
  const deadline = new Date(votingDeadline);
  const oneHourFromNow = new Date(now.getTime() + 60 * 60 * 1000);
  
  return deadline > oneHourFromNow;
};

// ================ ROUTE FACTORY ================

/**
 * Create category routes
 */
export const createCategoryRoutes = () => {
  
  // ================ PROTECTED ENDPOINTS (JWT REQUIRED) ================
  
  /**
   * Get organization categories
   * GET /api/categories/organization?limit=50&offset=0
   * Requires JWT authentication
   */
  router.get('/organization', requireJwtAuth, async (req, res) => {
    try {
      // Get user's organization
      const user = await userDb.getByWallet(req.walletAddress);
      if (!user || !user.organization_id) {
        return res.status(403).json({ 
          error: 'You must be part of an organization to view categories' 
        });
      }
      
      const limit = Math.min(parseInt(req.query.limit) || 50, 200); // Max 200
      const offset = Math.max(parseInt(req.query.offset) || 0, 0);
      
      const categories = await categoryDb.getByOrganization(user.organization_id, limit, offset);
      
      // Add follower counts to each category
      const categoriesWithCounts = await Promise.all(
        categories.map(async (category) => {
          const followerCount = await categoryDb.getFollowerCount(category.category_id);
          const isFollowing = await categoryDb.isFollowing(category.category_id, req.walletAddress);
          return {
            ...category,
            follower_count: followerCount,
            is_following: isFollowing
          };
        })
      );
      
      res.status(200).json({ 
        categories: categoriesWithCounts,
        organization_id: user.organization_id,
        organization_name: user.organizations?.organization_name,
        limit,
        offset,
        count: categoriesWithCounts.length
      });
      
    } catch (error) {
      console.error('Error getting organization categories:', error);
      res.status(500).json({ 
        error: 'Failed to get categories' 
      });
    }
  });
  
  /**
   * Create new category
   * POST /api/categories/create
   * Requires JWT authentication
   */
  router.post('/create', requireJwtAuth, async (req, res) => {
    try {
      const { title, description } = req.body;
      
      // Validate input data
      const validationErrors = validateCategoryData({ title, description });
      if (validationErrors.length > 0) {
        return res.status(400).json({ 
          error: 'Validation failed',
          details: validationErrors
        });
      }
      
      // Get user's organization
      const user = await userDb.getByWallet(req.walletAddress);
      if (!user || !user.organization_id) {
        return res.status(403).json({ 
          error: 'You must be part of an organization to create categories' 
        });
      }
      
      // Create category
      const categoryData = {
        title: title.trim(),
        description: description.trim(),
        organizationId: user.organization_id,
        createdBy: req.walletAddress
      };
      
      const newCategory = await categoryDb.create(categoryData);
      
      // Add initial follower count and following status
      const followerCount = await categoryDb.getFollowerCount(newCategory.category_id);
      const isFollowing = await categoryDb.isFollowing(newCategory.category_id, req.walletAddress);
      
      const categoryWithCounts = {
        ...newCategory,
        follower_count: followerCount,
        is_following: isFollowing
      };
      
      console.log(`✅ Category created: "${newCategory.title}" by ${user.unique_id}`);
      
      res.status(201).json({ 
        success: true,
        category: categoryWithCounts,
        message: 'Category created successfully'
      });
      
    } catch (error) {
      console.error('Error creating category:', error);
      
      // Handle specific database errors
      if (error.code === '23505') { // PostgreSQL unique violation
        if (error.detail?.includes('title')) {
          return res.status(409).json({ 
            error: 'Category title already exists in this organization' 
          });
        }
      }
      
      res.status(500).json({ 
        error: 'Failed to create category' 
      });
    }
  });
  
  /**
   * Get category by ID
   * GET /api/categories/:id
   * Requires JWT authentication
   */
  router.get('/:id', requireJwtAuth, async (req, res) => {
    try {
      const { id } = req.params;
      
      // Validate UUID format
      const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidPattern.test(id)) {
        return res.status(400).json({ 
          error: 'Invalid category ID format' 
        });
      }
      
      const category = await categoryDb.getById(id);
      
      if (!category) {
        return res.status(404).json({ 
          error: 'Category not found' 
        });
      }
      
      // Check if user is in the same organization
      const user = await userDb.getByWallet(req.walletAddress);
      if (user && user.organization_id !== category.organization_id) {
        return res.status(403).json({ 
          error: 'You can only view categories from your organization' 
        });
      }
      
      // Add follower count and following status
      const followerCount = await categoryDb.getFollowerCount(category.category_id);
      const isFollowing = await categoryDb.isFollowing(category.category_id, req.walletAddress);
      
      const categoryWithCounts = {
        ...category,
        follower_count: followerCount,
        is_following: isFollowing
      };
      
      res.status(200).json({ 
        category: categoryWithCounts 
      });
      
    } catch (error) {
      console.error('Error getting category by ID:', error);
      res.status(500).json({ 
        error: 'Failed to get category' 
      });
    }
  });
  
  /**
   * Follow a category
   * POST /api/categories/:id/follow
   * Requires JWT authentication
   */
  router.post('/:id/follow', requireJwtAuth, async (req, res) => {
    try {
      const { id } = req.params;
      
      // Validate UUID format
      const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidPattern.test(id)) {
        return res.status(400).json({ 
          error: 'Invalid category ID format' 
        });
      }
      
      // Check if category exists and user is in same organization
      const category = await categoryDb.getById(id);
      if (!category) {
        return res.status(404).json({ 
          error: 'Category not found' 
        });
      }
      
      const user = await userDb.getByWallet(req.walletAddress);
      if (user && user.organization_id !== category.organization_id) {
        return res.status(403).json({ 
          error: 'You can only follow categories from your organization' 
        });
      }
      
      // Check if already following
      const isAlreadyFollowing = await categoryDb.isFollowing(id, req.walletAddress);
      if (isAlreadyFollowing) {
        return res.status(409).json({ 
          error: 'You are already following this category' 
        });
      }
      
      // Follow the category
      await categoryDb.follow(id, req.walletAddress);
      
      // Get updated follower count
      const followerCount = await categoryDb.getFollowerCount(id);
      
      res.status(200).json({ 
        success: true,
        message: 'Category followed successfully',
        follower_count: followerCount
      });
      
    } catch (error) {
      console.error('Error following category:', error);
      
      // Handle duplicate follow attempts
      if (error.code === '23505') {
        return res.status(409).json({ 
          error: 'You are already following this category' 
        });
      }
      
      res.status(500).json({ 
        error: 'Failed to follow category' 
      });
    }
  });
  
  /**
   * Unfollow a category
   * DELETE /api/categories/:id/follow
   * Requires JWT authentication
   */
  router.delete('/:id/follow', requireJwtAuth, async (req, res) => {
    try {
      const { id } = req.params;
      
      // Validate UUID format
      const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidPattern.test(id)) {
        return res.status(400).json({ 
          error: 'Invalid category ID format' 
        });
      }
      
      // Check if category exists
      const category = await categoryDb.getById(id);
      if (!category) {
        return res.status(404).json({ 
          error: 'Category not found' 
        });
      }
      
      // Check if actually following
      const isFollowing = await categoryDb.isFollowing(id, req.walletAddress);
      if (!isFollowing) {
        return res.status(409).json({ 
          error: 'You are not following this category' 
        });
      }
      
      // Unfollow the category
      await categoryDb.unfollow(id, req.walletAddress);
      
      // Get updated follower count
      const followerCount = await categoryDb.getFollowerCount(id);
      
      res.status(200).json({ 
        success: true,
        message: 'Category unfollowed successfully',
        follower_count: followerCount
      });
      
    } catch (error) {
      console.error('Error unfollowing category:', error);
      res.status(500).json({ 
        error: 'Failed to unfollow category' 
      });
    }
  });

  // ================ SUGGESTION ROUTES ================

  /**
   * Create suggestion for a proposal
   * POST /api/categories/:id/suggest
   * Requires JWT authentication and category ownership
   */
  router.post('/:id/suggest', requireJwtAuth, async (req, res) => {
    try {
      const { id: categoryId } = req.params;
      const { proposal_id, suggestion_type, target_user, target_option_number } = req.body;
      
      // Validate UUID format for category
      const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidPattern.test(categoryId)) {
        return res.status(400).json({ 
          error: 'Invalid category ID format' 
        });
      }
      
      // Validate proposal ID format
      if (!proposal_id || !uuidPattern.test(proposal_id)) {
        return res.status(400).json({ 
          error: 'Valid proposal ID is required' 
        });
      }
      
      // Validate suggestion data
      const validationErrors = validateSuggestionData({ 
        suggestion_type, 
        target_user, 
        target_option_number 
      });
      if (validationErrors.length > 0) {
        return res.status(400).json({ 
          error: 'Validation failed',
          details: validationErrors
        });
      }
      
      // Check if category exists and user is the creator
      const category = await categoryDb.getById(categoryId);
      if (!category) {
        return res.status(404).json({ 
          error: 'Category not found' 
        });
      }
      
      if (category.created_by !== req.walletAddress.toLowerCase()) {
        return res.status(403).json({ 
          error: 'You can only create suggestions for categories you created' 
        });
      }
      
      // Check if proposal exists and is in same organization
      const proposal = await proposalDb.getById(proposal_id);
      if (!proposal) {
        return res.status(404).json({ 
          error: 'Proposal not found' 
        });
      }
      
      if (proposal.organization_id !== category.organization_id) {
        return res.status(403).json({ 
          error: 'You can only create suggestions for proposals in your organization' 
        });
      }
      
      // Check timing constraint - suggestions only allowed until 1 hour before deadline
      if (!canCreateSuggestions(proposal.voting_deadline)) {
        return res.status(403).json({ 
          error: 'Suggestions cannot be created within 1 hour of the voting deadline' 
        });
      }
      
      // For voting suggestions, validate the target option exists
      if (suggestion_type === 'voting') {
        const optionExists = proposal.options?.some(option => 
          option.option_number === target_option_number
        );
        if (!optionExists) {
          return res.status(400).json({ 
            error: `Option ${target_option_number} does not exist for this proposal` 
          });
        }
      }
      
      // For delegation suggestions, validate the target user exists by unique_id and is in same organization
      if (suggestion_type === 'delegate') {
        try {
          // Look up user by unique_id instead of wallet address
          const { data: targetUser, error: userError } = await supabase
            .from('users')
            .select('wallet_address, unique_id, organization_id')
            .eq('unique_id', target_user.toLowerCase())
            .single();
          
          if (userError || !targetUser) {
            return res.status(400).json({ 
              error: 'Target user not found. Please check the unique ID.' 
            });
          }
          
          if (targetUser.organization_id !== category.organization_id) {
            return res.status(400).json({ 
              error: 'Target user must be in the same organization' 
            });
          }
        } catch (error) {
          return res.status(400).json({ 
            error: 'Invalid target user unique ID' 
          });
        }
      }
      
      // Create the suggestion (target_user contains unique_id for delegation suggestions)
      const suggestionData = {
        categoryId,
        proposalId: proposal_id,
        suggestionType: suggestion_type, // Now uses 'delegate' or 'vote_option'
        targetUser: suggestion_type === 'delegate' ? target_user.toLowerCase() : null,
        targetOptionNumber: suggestion_type === 'vote_option' ? target_option_number : null
      };
      
      const newSuggestion = await categoryDb.createSuggestion(suggestionData);
      
      console.log(`✅ Suggestion created: ${suggestion_type} by category "${category.title}" for proposal "${proposal.title}"`);
      
      res.status(201).json({ 
        success: true,
        suggestion: newSuggestion,
        message: 'Suggestion created successfully'
      });
      
    } catch (error) {
      console.error('Error creating suggestion:', error);
      
      // Handle duplicate suggestion attempts
      if (error.code === '23505') {
        return res.status(409).json({ 
          error: 'This category has already made a suggestion for this proposal' 
        });
      }
      
      res.status(500).json({ 
        error: 'Failed to create suggestion' 
      });
    }
  });
  
  return router;
};

export default createCategoryRoutes;