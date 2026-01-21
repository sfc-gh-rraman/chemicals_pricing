-- =============================================================================
-- DDL Script: 001_DATABASE_SCHEMAS.sql
-- Demo: Chemicals Integrated Pricing & Supply Chain Optimization
-- Description: Create database and schemas for the demo
-- Dependencies: None (this is the first script)
-- =============================================================================

-- Create the demo database
CREATE DATABASE IF NOT EXISTS CHEMICALS_DB
    COMMENT = 'Chemicals Integrated Pricing & Supply Chain Optimization Demo - Dynamic pricing and demand sensing';

-- Use the database
USE DATABASE CHEMICALS_DB;

-- Drop the default PUBLIC schema (optional, keeps things clean)
DROP SCHEMA IF EXISTS PUBLIC;

-- =============================================================================
-- RAW Schema: Landing zone for source data
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Landing zone for raw source data: Market feeds, ERP sales, Plant costs';

-- =============================================================================
-- ATOMIC Schema: Normalized entity model (core + extensions)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS ATOMIC
    COMMENT = 'Normalized entity model with core entities and project-specific extensions';

-- =============================================================================
-- CHEMICAL_OPS Schema: Data mart for consumption
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS CHEMICAL_OPS
    COMMENT = 'Data mart for dashboards and analytics: margin analysis, pricing, forecasts';

-- =============================================================================
-- Create internal stages for data loading and documents
-- =============================================================================
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS DATA_STAGE
    COMMENT = 'Stage for loading CSV data files';

CREATE STAGE IF NOT EXISTS MARKET_DOCUMENTS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Storage for market reports, analyst PDFs, trade journals';

-- =============================================================================
-- Create warehouse if not exists
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Compute warehouse for Chemicals Pricing demo';

-- =============================================================================
-- Grant statements (adjust roles as needed for your environment)
-- =============================================================================
-- These are examples - uncomment and modify for your specific roles
-- GRANT USAGE ON DATABASE CHEMICALS_DB TO ROLE ANALYST_ROLE;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE CHEMICALS_DB TO ROLE ANALYST_ROLE;
-- GRANT SELECT ON ALL TABLES IN DATABASE CHEMICALS_DB TO ROLE ANALYST_ROLE;
-- GRANT SELECT ON ALL VIEWS IN DATABASE CHEMICALS_DB TO ROLE ANALYST_ROLE;

-- =============================================================================
-- Verification
-- =============================================================================
SHOW SCHEMAS IN DATABASE CHEMICALS_DB;
