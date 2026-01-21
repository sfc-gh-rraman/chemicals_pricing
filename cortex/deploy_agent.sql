-- =============================================================================
-- Deploy Cortex Agent for Chemicals Pricing & Supply Chain
-- =============================================================================
-- This script documents the deployment process for:
-- 1. Semantic View (for Cortex Analyst tool)
-- 2. Cortex Search Service (for Market Intelligence tool)
-- 3. Cortex Agent (combines both tools)
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- STEP 1: Verify Prerequisites
-- =============================================================================

-- Check that semantic model YAML is in the stage
LIST @CHEMICALS_DB.CHEMICAL_OPS.SEMANTIC_MODELS;

-- Check that Cortex Search service exists
SHOW CORTEX SEARCH SERVICES IN SCHEMA CHEMICALS_DB.CHEMICAL_OPS;

-- Verify data in source tables
SELECT 'MARGIN_ANALYZER' as source, COUNT(*) as row_count FROM CHEMICALS_DB.CHEMICAL_OPS.MARGIN_ANALYZER
UNION ALL
SELECT 'MARKET_REPORT_CHUNKS', COUNT(*) FROM CHEMICALS_DB.CHEMICAL_OPS.MARKET_REPORT_CHUNKS;

-- =============================================================================
-- STEP 2: Deploy Cortex Agent via REST API
-- =============================================================================
-- The Cortex Agent must be deployed via REST API using sf_cortex_agent_ops.py
-- 
-- Prerequisites:
-- 1. Set up your PAT (Personal Access Token) in Snowsight:
--    - Go to User Menu → Profile → Personal Access Tokens
--    - Create a new token with appropriate permissions
--
-- 2. Set environment variables:
--    export SNOWFLAKE_PAT_TOKEN="your-personal-access-token"
--    export SNOWFLAKE_ACCOUNT="your-account-identifier"
--    (e.g., SFPSCOGS-RRAMAN_AWS_SI)
--
-- 3. Run the import command:
--    python3 /Users/rraman/Documents/energy_weather_event_util/sf_cortex_agent_ops.py import \
--        --input cortex/chemicals_pricing_agent.json \
--        --database CHEMICALS_DB \
--        --schema CHEMICAL_OPS \
--        --name CHEMICALS_PRICING_AGENT \
--        --replace \
--        --pat-token "$SNOWFLAKE_PAT_TOKEN" \
--        --account "$SNOWFLAKE_ACCOUNT"
--
-- =============================================================================

-- =============================================================================
-- STEP 3: Verify Agent Deployment
-- =============================================================================

-- After deploying via REST API, verify the agent:
SHOW AGENTS IN SCHEMA CHEMICALS_DB.CHEMICAL_OPS;

-- Describe the agent configuration:
-- DESCRIBE AGENT CHEMICALS_DB.CHEMICAL_OPS.CHEMICALS_PRICING_AGENT;

-- =============================================================================
-- STEP 4: Test the Agent
-- =============================================================================
-- Navigate to Snowsight:
-- 1. Go to AI & ML → Agents
-- 2. Find CHEMICALS_PRICING_AGENT
-- 3. Click to open the agent chat interface
-- 4. Try sample questions:
--    - "Show me the average margin for Ethylene products in Europe vs North America for Q3"
--    - "How many deals were underpriced this month?"
--    - "What is the market outlook for polyethylene?"
--    - "What should we charge for Polypropylene in North America?"

SELECT 'Agent deployment script ready. Run the Python command above to deploy.' AS status;
