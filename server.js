import express from "express";
import cors from "cors";
import dotenv from 'dotenv';
import { Redis } from '@upstash/redis';
import { testConnection } from './database/supabase.js';
import { createAuthRoutes } from './routes/authRoutes.js';
import { createUserRoutes } from './routes/userRoutes.js';
import { createProposalRoutes } from './routes/proposalRoutes.js';
import { createCategoryRoutes } from './routes/categoryRoutes.js';
import { createVotingRoutes, initializeVotingOps } from './routes/votingRoutes.js';
import { requireJwtAuth } from './middleware/jwtAuth.js';
import { cleanupExpiredSessions } from './utils/supabaseAuth.js';
import { createVotingOps } from './utils/redisKeys.js';
import { createBackupManager, initializeBackupSystem } from './utils/redisBackup.js';
import { startProposalScheduler } from './utils/proposalScheduler.js';

// Load environment variables
dotenv.config();

// Initialize Express app
const app = express();

// Server start time
const serverStartTime = Date.now();

// ================ DATABASE AND REDIS SETUP ================

// Create Upstash Redis connection for future voting system
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// Test Upstash Redis connection
let redisConnected = false;
try {
  const pingResult = await redis.ping();
  if (pingResult === 'PONG') {
    redisConnected = true;
  } else {
    console.log('⚠️ Upstash Redis responded but not with PONG');
  }
} catch (error) {
  console.error('⚠️ Failed to connect to Upstash Redis:', error);
  console.error('Redis will be needed for voting calculations, but server can start without it');
  redisConnected = false;
}

// Test Supabase connection
try {
  const supabaseConnected = await testConnection();
  if (!supabaseConnected) {
    console.error('❌ Failed to connect to Supabase');
    console.error('Check your SUPABASE_URL and SUPABASE_SERVICE_KEY in .env file');
    process.exit(1);
  }
} catch (error) {
  console.error('❌ Supabase connection error:', error);
  process.exit(1);
}

// Clean up expired sessions on start and periodically

// Run cleanup immediately on server start
(async () => {
  try {
    await cleanupExpiredSessions();
    console.log('🧹 Initial cleanup completed on server start');
  } catch (error) {
    console.error('⚠️ Error in initial cleanup:', error);
  }
})();

// Cleanup every 12 hours
setInterval(async () => {
  try {
    await cleanupExpiredSessions();
    console.log('✅ Periodic cleanup completed');
  } catch (error) {
    console.error('⚠️ Error in periodic cleanup:', error);
  }
}, 12 * 60 * 60 * 1000); // 12 hours

// ================ VOTING SYSTEM SETUP ================

// Initialize liquid democracy utilities
export const votingOps = createVotingOps(redis);
export const backupManager = createBackupManager(redis);
const cleanupBackups = initializeBackupSystem(redis);
initializeVotingOps(redis);

app.get("/api/debug/redis-vote/:proposalId/:wallet", async (req, res) => {
  try {
    const { proposalId, wallet } = req.params;
    
    // Test direct Redis operations
    const allVotes = await redis.hgetall(`proposal:${proposalId}:votes`);
    const userVote = await redis.hget(`proposal:${proposalId}:votes`, wallet.toLowerCase());
    const participants = await redis.smembers(`proposal:${proposalId}:participants`);
    
    res.json({
      debug_info: {
        all_votes: allVotes,
        user_vote: userVote,
        user_vote_type: typeof userVote,
        user_vote_null_check: userVote === null,
        user_vote_undefined_check: userVote === undefined,
        user_vote_empty_check: userVote === '',
        participants: participants,
        wallet_key_used: wallet.toLowerCase(),
        redis_key: `proposal:${proposalId}:votes`
      }
    });
  } catch (error) {
    res.status(500).json({ error: error.message, stack: error.stack });
  }
});

// ================ DELEGATION RESOLUTION SETUP ================

startProposalScheduler(redis); // Every 5 minutes check for expired proposals

// ================ MIDDLEWARE SETUP ================

// Set up CORS
app.use(cors({
  origin: process.env.FRONTEND_URL,
  credentials: true
}));

app.use(express.json({ limit: "16kb" }));
app.use(express.urlencoded({ extended: true, limit: "16kb" }));

// ================ API ENDPOINTS ================

// Health check endpoint
app.get("/", (req, res) => {
  res.json({
    message: "Cheshire API Server with JWT Authentication",
    version: "0.7.0",
    auth: "JWT",
    database: "Supabase PostgreSQL",
    redis: "Upstash Redis (voting calculations)"
  });
});

// JWT-based /api/me endpoint
app.get("/api/me", requireJwtAuth, async (req, res) => {
  try {
    res.status(200).json({ 
      authenticated: true, 
      user: {
        walletAddress: req.user.walletAddress
      }
    });
  } catch (error) {
    console.error("Error in /api/me:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Debug endpoint to see Upstash Redis status (for voting system)
app.get("/api/debug/redis", async (req, res) => {
  try {
    // Test connection with ping
    const pingResult = await redis.ping();
    
    if (pingResult !== 'PONG') {
      return res.status(503).json({ 
        connected: false,
        message: "Upstash Redis not responding - needed for voting calculations",
        provider: "Upstash Redis"
      });
    }
    
    // Get database size (number of keys)
    const dbSize = await redis.dbsize();
    
    res.status(200).json({ 
      connected: true,
      database_keys: dbSize,
      ping: pingResult,
      purpose: "Ready for voting calculations",
      provider: "Upstash Redis",
      url: process.env.UPSTASH_REDIS_REST_URL || "Not configured"
    });
  } catch (error) {
    res.status(500).json({ 
      error: error.message,
      connected: false,
      provider: "Upstash Redis"
    });
  }
});

// System status endpoint
app.get("/api/status", async (req, res) => {
  try {
    // Test Supabase connection
    const supabaseHealth = await testConnection();
    
    // Test Upstash Redis connection
    let redisHealth = false;
    try {
      const pingResult = await redis.ping();
      redisHealth = pingResult === 'PONG';
    } catch (error) {
      console.log('Upstash Redis health check failed:', error.message);
    }

    // Calculate uptime
    const uptimeSeconds = Math.floor((Date.now() - serverStartTime) / 1000); // in seconds
    const uptimeHours = Math.floor(uptimeSeconds / 3600);
    const uptimeMinutes = Math.floor((uptimeSeconds % 3600) / 60);
    const uptime = `${uptimeHours}h ${uptimeMinutes}m`;
    
    res.status(200).json({
      status: "healthy",
      timestamp: new Date().toISOString(),
      uptime: uptime,
      services: {
        api: true,
        supabase: supabaseHealth,
        redis: {
          connected: redisHealth,
          purpose: "voting_calculations",
          provider: "Upstash",
          required: false
        }
      },
      auth: "JWT",
      version: "0.7.0"
    });
  } catch (error) {
    res.status(500).json({
      status: "unhealthy",
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// ================ ROUTE MOUNTING ================

// Auth routes (JWT-based)
const authRoutes = createAuthRoutes();
app.use('/api/auth', authRoutes);

// User routes (JWT-protected)
const userRoutes = createUserRoutes();
app.use('/api/user', userRoutes);

// Proposal routes (JWT-protected) 
const proposalRoutes = createProposalRoutes();
app.use('/api/proposals', proposalRoutes);

// Category routes (JWT-protected)
const categoryRoutes = createCategoryRoutes();
app.use('/api/categories', categoryRoutes);

// Voting routes (JWT-protected, Redis-based)
const votingRoutes = createVotingRoutes();
app.use('/api/proposals', votingRoutes);

// ================ GRACEFUL SHUTDOWN ================

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully...');
  
  cleanupBackups();

  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully...');
  
  cleanupBackups();

  process.exit(0);
});

// ================ START SERVER ================

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 Cheshire API Server running on port ${PORT}`);
  console.log(`🔐 Authentication: JWT with Supabase sessions`);
  console.log(`📡 CORS configured for: ${process.env.FRONTEND_URL}`);
  console.log(`💾 Database: Supabase PostgreSQL`);
  console.log(`⚡ Redis: ${redisConnected ? 'Upstash connected (voting ready)' : 'Upstash disconnected (voting unavailable)'}`);
});