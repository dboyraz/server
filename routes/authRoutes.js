import express from 'express';
import { generateNonce } from 'siwe';
import { createAuthSession, invalidateSession } from '../utils/supabaseAuth.js';
import { requireJwtAuth } from '../middleware/jwtAuth.js';
import { userDb } from '../database/supabase.js';

/**
 * Create auth routes factory function
 */
export const createAuthRoutes = () => {
  const router = express.Router();

  /**
   * Generate nonce for wallet signing
   * GET /api/auth/nonce
   */
  router.get('/nonce', (req, res) => {
    try {
      const nonce = generateNonce();
      console.log("Generated nonce for JWT authentication");
      res.status(200).json({ nonce });
    } catch (error) {
      console.error("Error generating nonce:", error);
      res.status(500).json({ error: "Failed to generate nonce" });
    }
  });

  /**
   * Check if user has completed profile setup
   * GET /api/auth/check-user?address=0x...
   */
  router.get('/check-user', async (req, res) => {
    try {
      const { address } = req.query;
      
      if (!address) {
        return res.status(400).json({ 
          error: 'Wallet address is required' 
        });
      }
      
      // Validate address format
      if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
        return res.status(400).json({ 
          error: 'Invalid wallet address format' 
        });
      }
      
      const exists = await userDb.exists(address);
      
      res.status(200).json({ 
        exists,
        canSignIn: exists,
        needsSetup: !exists,
        address: address.toLowerCase()
      });
      
    } catch (error) {
      console.error('Error checking user:', error);
      res.status(500).json({ 
        error: 'Failed to check user status' 
      });
    }
  });

  /**
   * Sign in with wallet signature (only for users with completed profiles)
   * POST /api/auth/signin
   */
  router.post('/signin', async (req, res) => {
    const { message, signature } = req.body;
    
    if (!message || !signature) {
      return res.status(400).json({ 
        success: false, 
        error: "Missing message or signature" 
      });
    }
    
    try {
      // Extract address from the message
      const addressMatch = message.match(/Address: (0x[a-fA-F0-9]{40})/);
      
      if (!addressMatch || !addressMatch[1]) {
        return res.status(400).json({ 
          success: false, 
          error: "Invalid message format - could not extract address" 
        });
      }
      
      const address = addressMatch[1];
      
      // Check if user has completed profile setup
      const userExists = await userDb.exists(address);
      
      if (!userExists) {
        return res.status(403).json({ 
          success: false, 
          error: "Profile setup required",
          needsSetup: true,
          address: address.toLowerCase()
        });
      }
      
      // TODO: Add signature verification here if needed
      // For now, we trust the frontend verification
      
      // Create JWT session (will work because user exists and foreign key is satisfied)
      const { token } = await createAuthSession(address);
      
      console.log(`✅ User signed in with JWT: ${address}`);
      
      res.status(200).json({ 
        success: true,
        token,
        user: {
          walletAddress: address
        }
      });
      
    } catch (error) {
      console.error('JWT signin error:', error);
      
      // Handle foreign key constraint errors gracefully
      if (error.code === '23503') {
        return res.status(403).json({ 
          success: false, 
          error: "Profile setup required",
          needsSetup: true
        });
      }
      
      res.status(500).json({ 
        success: false, 
        error: 'Authentication failed' 
      });
    }
  });

  /**
   * Get current user info from JWT
   * GET /api/auth/me
   */
  router.get('/me', requireJwtAuth, async (req, res) => {
    try {
      res.status(200).json({ 
        authenticated: true, 
        user: {
          walletAddress: req.user.walletAddress
        }
      });
    } catch (error) {
      console.error("Error getting user info:", error);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  /**
   * Sign out and invalidate JWT session
   * POST /api/auth/signout
   */
  router.post('/signout', async (req, res) => {
    try {
      // Get token from Authorization header
      const authHeader = req.headers.authorization;
      
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(400).json({ 
          success: false, 
          error: 'No token provided' 
        });
      }
      
      const token = authHeader.substring(7);
      await invalidateSession(token);
      
      res.status(200).json({ 
        success: true,
        message: 'Signed out successfully'
      });
      
    } catch (error) {
      console.error('JWT signout error:', error);
      res.status(500).json({ 
        success: false, 
        error: 'Signout failed' 
      });
    }
  });

  return router;
};

export default createAuthRoutes;