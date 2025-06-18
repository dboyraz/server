import { supabase } from '../database/supabase.js';
import jwt from 'jsonwebtoken';

// Session duration in seconds (36 hours)
const SESSION_DURATION = 129600;

/**
 * Create a custom JWT using Supabase Auth
 */
export const createAuthSession = async (walletAddress) => {
  try {
    // Create a custom user payload for JWT
    const userPayload = {
      wallet_address: walletAddress.toLowerCase(),
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + SESSION_DURATION,
      iss: 'cheshire-auth',
      sub: walletAddress.toLowerCase()
    };

    // Sign JWT with a secret key
    const jwtSecret = process.env.JWT_SECRET || process.env.SUPABASE_SERVICE_KEY || 'fallback-secret';
    const token = jwt.sign(userPayload, jwtSecret);

    // Store session info in Supabase
    const { error } = await supabase
      .from('user_sessions')
      .insert([{
        jwt_token: token,
        wallet_address: walletAddress.toLowerCase(),
        expires_at: new Date(Date.now() + (SESSION_DURATION * 1000)).toISOString(),
        created_at: new Date().toISOString()
      }]);

    if (error) {
      console.error('Error storing session:', error);
      throw error;
    }

    console.log(`✅ JWT session created for: ${walletAddress}`);
    return { token };

  } catch (error) {
    console.error('Error creating auth session:', error);
    throw error;
  }
};

/**
 * Verify JWT token
 */
export const verifyAuthToken = async (token) => {
  try {
    const jwtSecret = process.env.JWT_SECRET || process.env.SUPABASE_SERVICE_KEY || 'fallback-secret';
    
    // Verify JWT signature and expiration
    const decoded = jwt.verify(token, jwtSecret);
    
    // Check if session exists in database
    const { data: session, error } = await supabase
      .from('user_sessions')
      .select('*')
      .eq('jwt_token', token)
      .eq('wallet_address', decoded.wallet_address)
      .single();

    if (error || !session) {
      return null;
    }

    // Check if session is expired
    if (new Date(session.expires_at) < new Date()) {
      // Clean up expired session
      await supabase
        .from('user_sessions')
        .delete()
        .eq('jwt_token', token);
      return null;
    }

    return {
      walletAddress: decoded.wallet_address,
      isValid: true
    };

  } catch (error) {
    console.error('Error verifying token:', error);
    return null;
  }
};

/**
 * Invalidate session (logout)
 */
export const invalidateSession = async (token) => {
  try {
    const jwtSecret = process.env.JWT_SECRET || process.env.SUPABASE_SERVICE_KEY || 'fallback-secret';
    const decoded = jwt.verify(token, jwtSecret);
    
    // Remove session from database
    const { error } = await supabase
      .from('user_sessions')
      .delete()
      .eq('jwt_token', token)
      .eq('wallet_address', decoded.wallet_address);

    if (error) {
      console.error('Error invalidating session:', error);
      return false;
    }

    console.log(`✅ Session invalidated for: ${decoded.wallet_address}`);
    return true;

  } catch (error) {
    console.error('Error invalidating session:', error);
    return false;
  }
};

/**
 * Clean up expired sessions (utility function)
 */
export const cleanupExpiredSessions = async () => {
  try {
    const { error } = await supabase
      .from('user_sessions')
      .delete()
      .lt('expires_at', new Date().toISOString());

    if (error) {
      console.error('Error cleaning up sessions:', error);
    } else {
    }
  } catch (error) {
    console.error('Error cleaning up sessions:', error);
  }
};