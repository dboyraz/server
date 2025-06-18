import { verifyAuthToken } from '../utils/supabaseAuth.js';

/**
 * JWT Authentication Middleware
 * Verifies JWT tokens from Authorization header only
 */
export const requireJwtAuth = async (req, res, next) => {
  try {
    // Get token from Authorization header only
    const authHeader = req.headers.authorization;
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ 
        error: 'Authentication required - Bearer token missing',
        authenticated: false 
      });
    }
    
    const token = authHeader.substring(7);
    
    // Verify token
    const authResult = await verifyAuthToken(token);
    
    if (!authResult || !authResult.isValid) {
      return res.status(401).json({ 
        error: 'Invalid or expired token',
        authenticated: false 
      });
    }
    
    // Add user info to request for use in routes
    req.user = {
      walletAddress: authResult.walletAddress
    };
    req.walletAddress = authResult.walletAddress; // For compatibility with existing routes
    
    next();
    
  } catch (error) {
    console.error('JWT Auth middleware error:', error);
    res.status(500).json({ error: 'Authentication check failed' });
  }
};

/**
 * Optional JWT Auth Middleware
 * Adds user info if token is valid, but doesn't reject if missing
 */
export const optionalJwtAuth = async (req, res, next) => {
  try {
    // Get token from Authorization header
    const authHeader = req.headers.authorization;
    
    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.substring(7);
      const authResult = await verifyAuthToken(token);
      
      if (authResult && authResult.isValid) {
        req.user = {
          walletAddress: authResult.walletAddress
        };
        req.walletAddress = authResult.walletAddress;
      }
    }
    
    next();
    
  } catch (error) {
    console.error('Optional JWT Auth middleware error:', error);
    // Don't fail the request, just continue without auth
    next();
  }
};