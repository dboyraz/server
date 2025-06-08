import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

// Supabase configuration
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY;

if (!supabaseUrl || !supabaseAnonKey || !supabaseServiceKey) {
  console.error('❌ Missing Supabase environment variables');
  console.error('Please set SUPABASE_URL, SUPABASE_ANON_KEY, and SUPABASE_SERVICE_KEY in your .env file');
  process.exit(1);
}

// Create Supabase client with service role key (bypasses RLS)
export const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Also export anon client for future frontend operations if needed
export const supabaseAnon = createClient(supabaseUrl, supabaseAnonKey);

// Test connection function
export const testConnection = async () => {
  try {
    const { data, error } = await supabase
      .from('organizations')
      .select('count')
      .limit(1);
      
    if (error) {
      console.error('❌ Supabase connection failed:', error.message);
      return false;
    }
    
    return true;
  } catch (error) {
    console.error('❌ Supabase connection error:', error.message);
    return false;
  }
};

// User database functions
export const userDb = {
  // Check if user exists by wallet address
  async exists(walletAddress) {
    try {
      const { data, error } = await supabase
        .from('users')
        .select('wallet_address')
        .eq('wallet_address', walletAddress.toLowerCase())
        .single();
        
      if (error && error.code !== 'PGRST116') { // PGRST116 = no rows found
        throw error;
      }
      
      return !!data;
    } catch (error) {
      console.error('Error checking user existence:', error);
      throw error;
    }
  },

  // Get user by wallet address
  async getByWallet(walletAddress) {
    try {
      const { data, error } = await supabase
        .from('users')
        .select(`
          wallet_address,
          unique_id,
          first_name,
          last_name,
          organization_id,
          organizations (
            organization_name
          ),
          created_at
        `)
        .eq('wallet_address', walletAddress.toLowerCase())
        .single();
        
      if (error) {
        throw error;
      }
      
      return data;
    } catch (error) {
      console.error('Error getting user by wallet:', error);
      throw error;
    }
  },

  // Check if unique_id is available
  async isUniqueIdAvailable(uniqueId) {
    try {
      const { data, error } = await supabase
        .from('users')
        .select('unique_id')
        .eq('unique_id', uniqueId.toLowerCase())
        .single();
        
      if (error && error.code !== 'PGRST116') { // PGRST116 = no rows found
        throw error;
      }
      
      return !data; // Available if no data found
    } catch (error) {
      console.error('Error checking unique_id availability:', error);
      throw error;
    }
  },

  // Create new user
  async create(userData) {
    try {
      const { data, error } = await supabase
        .from('users')
        .insert([{
          wallet_address: userData.walletAddress.toLowerCase(),
          unique_id: userData.uniqueId.toLowerCase(),
          first_name: userData.firstName,
          last_name: userData.lastName,
          organization_id: userData.organizationId || null
        }])
        .select()
        .single();
        
      if (error) {
        throw error;
      }
      
      return data;
    } catch (error) {
      console.error('Error creating user:', error);
      throw error;
    }
  }
};

// Organization database functions
export const organizationDb = {
  // Check if organization exists
  async exists(organizationId) {
    try {
      const { data, error } = await supabase
        .from('organizations')
        .select('organization_id')
        .eq('organization_id', organizationId.toLowerCase())
        .single();
        
      if (error && error.code !== 'PGRST116') { // PGRST116 = no rows found
        throw error;
      }
      
      return !!data;
    } catch (error) {
      console.error('Error checking organization existence:', error);
      throw error;
    }
  },

  // Get all organizations
  async getAll() {
    try {
      const { data, error } = await supabase
        .from('organizations')
        .select('organization_id, organization_name')
        .order('organization_name');
        
      if (error) {
        throw error;
      }
      
      return data;
    } catch (error) {
      console.error('Error getting organizations:', error);
      throw error;
    }
  }
};

// Proposal database functions
export const proposalDb = {
  // Create new proposal
  async create(proposalData) {
  try {
    // Start a transaction by creating the proposal first
    const { data: proposal, error: proposalError } = await supabase
      .from('proposals')
      .insert([{
        title: proposalData.title.trim(),
        description: proposalData.description.trim(),
        voting_deadline: proposalData.voting_deadline,
        organization_id: proposalData.organization_id,
        created_by: proposalData.created_by.toLowerCase()
      }])
      .select()
      .single();
      
    if (proposalError) {
      throw proposalError;
    }
    
    // Create the options
    if (proposalData.options && proposalData.options.length > 0) {
      const optionsData = proposalData.options.map((option, index) => ({
        proposal_id: proposal.proposal_id,
        option_number: index + 1,
        option_text: option.trim()
      }));
      
      const { error: optionsError } = await supabase
        .from('proposal_options')
        .insert(optionsData);
        
      if (optionsError) {
        // If options failed, we should delete the proposal to maintain consistency
        await supabase
          .from('proposals')
          .delete()
          .eq('proposal_id', proposal.proposal_id);
        throw optionsError;
      }
    }
    
    // Return the complete proposal with options
    return await this.getById(proposal.proposal_id);
    
  } catch (error) {
    console.error('Error creating proposal:', error);
    throw error;
  }
},

  // Get proposal by ID
  async getById(proposalId) {
  try {
    // Get the proposal
    const { data: proposal, error: proposalError } = await supabase
      .from('proposals')
      .select(`
        proposal_id,
        title,
        description,
        voting_deadline,
        organization_id,
        created_by,
        created_at,
        organizations (
          organization_name
        ),
        users (
          unique_id,
          first_name,
          last_name
        )
      `)
      .eq('proposal_id', proposalId)
      .single();
      
    if (proposalError) {
      throw proposalError;
    }
    
    // Get the options
    const { data: options, error: optionsError } = await supabase
      .from('proposal_options')
      .select('option_number, option_text')
      .eq('proposal_id', proposalId)
      .order('option_number');
      
    if (optionsError) {
      throw optionsError;
    }
    
    // Combine proposal and options
    return {
      ...proposal,
      options: options || []
    };
    
  } catch (error) {
    console.error('Error getting proposal by ID:', error);
    throw error;
  }
},

  // Get proposals by organization
  async getByOrganization(organizationId, limit = 50, offset = 0) {
  try {
    // Get the proposals first
    const { data: proposals, error: proposalsError } = await supabase
      .from('proposals')
      .select(`
        proposal_id,
        title,
        description,
        voting_deadline,
        organization_id,
        created_by,
        created_at,
        organizations (
          organization_name
        ),
        users (
          unique_id,
          first_name,
          last_name
        )
      `)
      .eq('organization_id', organizationId)
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1);
      
    if (proposalsError) {
      throw proposalsError;
    }
    
    if (!proposals || proposals.length === 0) {
      return proposals || [];
    }
    
    // Get proposal IDs to fetch options
    const proposalIds = proposals.map(p => p.proposal_id);
    
    // Get all options for these proposals
    const { data: options, error: optionsError } = await supabase
      .from('proposal_options')
      .select('proposal_id, option_number, option_text')
      .in('proposal_id', proposalIds)
      .order('option_number');
      
    if (optionsError) {
      throw optionsError;
    }
    
    // Group options by proposal_id
    const optionsByProposal = {};
    if (options) {
      options.forEach(option => {
        if (!optionsByProposal[option.proposal_id]) {
          optionsByProposal[option.proposal_id] = [];
        }
        optionsByProposal[option.proposal_id].push({
          option_number: option.option_number,
          option_text: option.option_text
        });
      });
    }
    
    // Combine proposals with their options
    const proposalsWithOptions = proposals.map(proposal => ({
      ...proposal,
      options: optionsByProposal[proposal.proposal_id] || []
    }));
    
    return proposalsWithOptions;
    
  } catch (error) {
    console.error('Error getting proposals by organization:', error);
    throw error;
  }
},

  // Get proposals by user
  async getByUser(walletAddress, limit = 50, offset = 0) {
    try {
      const { data, error } = await supabase
        .from('proposals')
        .select(`
          proposal_id,
          title,
          description,
          voting_deadline,
          organization_id,
          created_by,
          created_at,
          organizations (
            organization_name
          )
        `)
        .eq('created_by', walletAddress.toLowerCase())
        .order('created_at', { ascending: false })
        .range(offset, offset + limit - 1);
        
      if (error) {
        throw error;
      }
      
      return data;
    } catch (error) {
      console.error('Error getting proposals by user:', error);
      throw error;
    }
  },

  // Check if user can create proposal (has completed profile and organization)
  async canUserCreateProposal(walletAddress) {
    try {
      const { data, error } = await supabase
        .from('users')
        .select('organization_id')
        .eq('wallet_address', walletAddress.toLowerCase())
        .single();
        
      if (error) {
        throw error;
      }
      
      // User must have an organization to create proposals
      return !!data?.organization_id;
    } catch (error) {
      console.error('Error checking if user can create proposal:', error);
      throw error;
    }
  }
};

// Clean up expired sessions (call periodically)
export const cleanupExpiredSessions = async () => {
  try {
    const { error } = await supabase
      .from('user_sessions')
      .delete()
      .lt('expires_at', new Date().toISOString());

    if (error) {
      console.error('Error cleaning up sessions:', error);
    } else {
      console.log('✅ Expired sessions cleaned up');
    }
  } catch (error) {
    console.error('Error cleaning up sessions:', error);
  }
};