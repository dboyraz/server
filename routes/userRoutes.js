import express from 'express';
import { userDb, organizationDb } from '../database/supabase.js';
import { requireJwtAuth, optionalJwtAuth } from '../middleware/jwtAuth.js';

const router = express.Router();

// ================ ROUTE FACTORY ================

/**
 * Create user routes (using JWT authentication)
 */
export const createUserRoutes = () => {
  
  // ================ PUBLIC ENDPOINTS ================
  
  /**
   * Check if user exists by wallet address
   * GET /api/user/exists?address=0x123...
   * Public endpoint - no auth required
   */
  router.get('/exists', async (req, res) => {
    try {
      const { address } = req.query;
      
      if (!address) {
        return res.status(400).json({ 
          error: 'Wallet address is required' 
        });
      }
      
      // Validate address format (basic check)
      if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
        return res.status(400).json({ 
          error: 'Invalid wallet address format' 
        });
      }
      
      const exists = await userDb.exists(address);
      
      res.status(200).json({ 
        exists,
        address: address.toLowerCase()
      });
      
    } catch (error) {
      console.error('Error checking user existence:', error);
      res.status(500).json({ 
        error: 'Failed to check user existence' 
      });
    }
  });
  
  /**
   * Check if unique_id is available
   * GET /api/user/unique-id/check?id=alice123
   * Public endpoint for form validation
   */
  router.get('/unique-id/check', async (req, res) => {
    try {
      const { id } = req.query;
      
      if (!id) {
        return res.status(400).json({ 
          error: 'Unique ID is required' 
        });
      }
      
      // Validate unique_id format
      const uniqueIdPattern = /^[a-zA-Z0-9_]{1,16}$/;
      if (!uniqueIdPattern.test(id)) {
        return res.status(400).json({ 
          error: 'Invalid format. Use only letters, numbers, and underscores (max 16 characters)',
          available: false
        });
      }
      
      const available = await userDb.isUniqueIdAvailable(id);
      
      res.status(200).json({ 
        available,
        unique_id: id.toLowerCase()
      });
      
    } catch (error) {
      console.error('Error checking unique_id availability:', error);
      res.status(500).json({ 
        error: 'Failed to check unique ID availability' 
      });
    }
  });
  
  /**
   * Check if organization exists
   * GET /api/user/organization/check?id=bilgi_university
   * Public endpoint for form validation
   */
  router.get('/organization/check', async (req, res) => {
    try {
      const { id } = req.query;
      
      if (!id) {
        return res.status(400).json({ 
          error: 'Organization ID is required' 
        });
      }
      
      const exists = await organizationDb.exists(id);
      
      res.status(200).json({ 
        exists,
        organization_id: id.toLowerCase()
      });
      
    } catch (error) {
      console.error('Error checking organization existence:', error);
      res.status(500).json({ 
        error: 'Failed to check organization existence' 
      });
    }
  });
  
  /**
   * Get all available organizations
   * GET /api/user/organizations
   * Public endpoint for dropdown/suggestions
   */
  router.get('/organizations', async (req, res) => {
    try {
      const organizations = await organizationDb.getAll();
      
      res.status(200).json({ 
        organizations 
      });
      
    } catch (error) {
      console.error('Error getting organizations:', error);
      res.status(500).json({ 
        error: 'Failed to get organizations' 
      });
    }
  });
  
  // ================ PROTECTED ENDPOINTS (JWT REQUIRED) ================
  
  /**
   * Get current user profile
   * GET /api/user/profile
   * Requires JWT authentication
   */
  router.get('/profile', requireJwtAuth, async (req, res) => {
    try {
      // req.walletAddress comes from JWT middleware
      const user = await userDb.getByWallet(req.walletAddress);
      
      if (!user) {
        return res.status(404).json({ 
          error: 'User profile not found',
          exists: false
        });
      }
      
      res.status(200).json({ 
        user,
        exists: true
      });
      
    } catch (error) {
      console.error('Error getting user profile:', error);
      res.status(500).json({ 
        error: 'Failed to get user profile' 
      });
    }
  });
  
  /**
   * Create new user profile
   * POST /api/user/create
   * Uses optional JWT auth - can be called during setup flow
   */
  router.post('/create', optionalJwtAuth, async (req, res) => {
    try {
      const { unique_id, first_name, last_name, organization_id } = req.body;
      
      // Validate required fields
      if (!unique_id || !first_name || !last_name) {
        return res.status(400).json({ 
          error: 'Missing required fields: unique_id, first_name, last_name' 
        });
      }
      
      // Validate unique_id format
      const uniqueIdPattern = /^[a-zA-Z0-9_]{1,16}$/;
      if (!uniqueIdPattern.test(unique_id)) {
        return res.status(400).json({ 
          error: 'Invalid unique_id format. Use only letters, numbers, and underscores (max 16 characters)' 
        });
      }
      
      // For user creation, need wallet address from JWT or request body
      let walletAddress = req.walletAddress; // From JWT if authenticated
      
      if (!walletAddress) {
        // If no JWT, wallet address should be in request body or headers
        walletAddress = req.body.wallet_address;
        
        if (!walletAddress) {
          return res.status(400).json({ 
            error: 'Wallet address is required for user creation' 
          });
        }
      }
      
      // Check if user already exists
      const userExists = await userDb.exists(walletAddress);
      if (userExists) {
        return res.status(409).json({ 
          error: 'User profile already exists for this wallet address' 
        });
      }
      
      // Check if unique_id is available
      const uniqueIdAvailable = await userDb.isUniqueIdAvailable(unique_id);
      if (!uniqueIdAvailable) {
        return res.status(409).json({ 
          error: 'Unique ID is already taken' 
        });
      }
      
      // Check if organization exists (if provided)
      if (organization_id) {
        const orgExists = await organizationDb.exists(organization_id);
        if (!orgExists) {
          return res.status(400).json({ 
            error: 'Organization does not exist' 
          });
        }
      }
      
      // Create user
      const userData = {
        walletAddress: walletAddress,
        uniqueId: unique_id,
        firstName: first_name,
        lastName: last_name,
        organizationId: organization_id || null
      };
      
      const newUser = await userDb.create(userData);
      
      console.log(`✅ User created: ${newUser.unique_id} (${newUser.wallet_address})`);
      
      res.status(201).json({ 
        success: true,
        user: newUser,
        message: 'User profile created successfully'
      });
      
    } catch (error) {
      console.error('Error creating user:', error);
      
      // Handle specific database errors
      if (error.code === '23505') { // PostgreSQL unique violation
        if (error.detail?.includes('unique_id')) {
          return res.status(409).json({ 
            error: 'Unique ID is already taken' 
          });
        }
        if (error.detail?.includes('wallet_address')) {
          return res.status(409).json({ 
            error: 'User profile already exists for this wallet address' 
          });
        }
      }
      
      res.status(500).json({ 
        error: 'Failed to create user profile' 
      });
    }
  });
  
  return router;
};

export default createUserRoutes;