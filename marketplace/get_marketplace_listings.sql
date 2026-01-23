-- =============================================================================
-- Marketplace Data Integration for Chemicals Pricing Solution
-- =============================================================================
-- This script helps discover and install relevant free marketplace data
-- 
-- Recommended Free Listings for Chemicals Pricing:
-- 1. Cybersyn - Economic indicators, commodities, industry data
-- 2. Knoema - World economic data
-- 3. FRED Economic Data - Federal Reserve economic indicators
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- STEP 1: Check current marketplace/share setup
-- =============================================================================

-- View current shares and databases from marketplace
SHOW DATABASES;
SHOW SHARES;

-- =============================================================================
-- STEP 2: Install Free Marketplace Listings
-- =============================================================================

-- To get free marketplace data, use the Snowflake GUI:
-- 1. Go to Data > Marketplace in Snowsight
-- 2. Search for providers like "Cybersyn" or "Knoema"
-- 3. Click "Get" on free listings
-- 4. Accept terms and create database

-- After installation, the database will appear in your account
-- Common names after install:
--   CYBERSYN  (if you get Cybersyn data)
--   KNOEMA_WORLD_DATA  (if you get Knoema)

-- =============================================================================
-- STEP 3: Verify Installed Marketplace Data
-- =============================================================================

-- After getting a listing, verify it exists:
-- SHOW DATABASES LIKE '%CYBERSYN%';
-- SHOW SCHEMAS IN DATABASE <MARKETPLACE_DATABASE>;
-- SHOW TABLES IN SCHEMA <MARKETPLACE_DATABASE>.<SCHEMA>;

-- =============================================================================
-- STEP 4: Useful queries once data is installed
-- =============================================================================

-- Example: If Cybersyn commodities data is installed
-- SELECT * 
-- FROM CYBERSYN.COMMODITIES.FRED_SERIES_ATTRIBUTES 
-- WHERE SERIES_ID LIKE '%OIL%' OR SERIES_ID LIKE '%CRUDE%'
-- LIMIT 20;

-- Example: Get crude oil prices
-- SELECT date, value as price
-- FROM CYBERSYN.COMMODITIES.FRED_TIMESERIES 
-- WHERE series_id = 'DCOILWTICO'  -- WTI Crude Oil
-- ORDER BY date DESC
-- LIMIT 365;

-- =============================================================================
-- STEP 5: Create integration views in CHEMICALS_DB
-- =============================================================================

-- After marketplace data is available, we create views that join it
-- See: marketplace/create_marketplace_views.sql

