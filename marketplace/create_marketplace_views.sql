-- =============================================================================
-- Marketplace Integration Views for Chemicals Pricing
-- =============================================================================
-- This script creates views that integrate marketplace data with our solution
-- 
-- Prerequisites: 
-- 1. Install Cybersyn free data from Snowflake Marketplace
-- 2. The database will typically be named CYBERSYN or similar
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- CONFIGURATION: Set the marketplace database name
-- =============================================================================
-- After installing from marketplace, update this variable:
-- SET MARKETPLACE_DB = 'CYBERSYN';

-- =============================================================================
-- VIEW 1: Crude Oil Price Index (feedstock proxy)
-- Maps to BRENT, WTI crude prices from FRED/Cybersyn
-- =============================================================================

-- Template view - uncomment and modify once Cybersyn is installed
/*
CREATE OR REPLACE VIEW MARKETPLACE_CRUDE_OIL_PRICES AS
SELECT 
    date AS price_date,
    series_id,
    CASE series_id
        WHEN 'DCOILWTICO' THEN 'WTI_CRUDE'
        WHEN 'DCOILBRENTEU' THEN 'BRENT_CRUDE'
        ELSE series_id
    END AS commodity_code,
    value AS price_usd,
    'USD/BBL' AS unit
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id IN ('DCOILWTICO', 'DCOILBRENTEU')
  AND date >= DATEADD(year, -3, CURRENT_DATE())
ORDER BY date DESC;
*/

-- =============================================================================
-- VIEW 2: Natural Gas Prices (energy cost driver)
-- =============================================================================

/*
CREATE OR REPLACE VIEW MARKETPLACE_NATURAL_GAS_PRICES AS
SELECT 
    date AS price_date,
    'NATURAL_GAS' AS commodity_code,
    value AS price_usd,
    'USD/MMBTU' AS unit
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id = 'DHHNGSP'  -- Henry Hub Natural Gas Spot Price
  AND date >= DATEADD(year, -3, CURRENT_DATE())
ORDER BY date DESC;
*/

-- =============================================================================
-- VIEW 3: Producer Price Index - Chemicals
-- Industry-specific inflation/cost driver
-- =============================================================================

/*
CREATE OR REPLACE VIEW MARKETPLACE_CHEMICAL_PPI AS
SELECT 
    date AS price_date,
    'CHEMICAL_PPI' AS index_code,
    value AS index_value,
    ROUND((value / LAG(value, 12) OVER (ORDER BY date) - 1) * 100, 2) AS yoy_change_pct
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id = 'PCU325325'  -- PPI: Chemical Manufacturing
  AND date >= DATEADD(year, -5, CURRENT_DATE())
ORDER BY date DESC;
*/

-- =============================================================================
-- VIEW 4: Economic Indicators (demand drivers)
-- =============================================================================

/*
CREATE OR REPLACE VIEW MARKETPLACE_ECONOMIC_INDICATORS AS
SELECT 
    date AS indicator_date,
    series_id,
    CASE series_id
        WHEN 'INDPRO' THEN 'Industrial_Production_Index'
        WHEN 'DGORDER' THEN 'Durable_Goods_Orders'
        WHEN 'HOUST' THEN 'Housing_Starts'
        WHEN 'UMCSENT' THEN 'Consumer_Sentiment'
    END AS indicator_name,
    value AS indicator_value
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id IN ('INDPRO', 'DGORDER', 'HOUST', 'UMCSENT')
  AND date >= DATEADD(year, -3, CURRENT_DATE())
ORDER BY date DESC, series_id;
*/

-- =============================================================================
-- VIEW 5: Unified Feedstock Cost View
-- Joins marketplace crude oil with our MARKET_INDEX table
-- =============================================================================

/*
CREATE OR REPLACE VIEW INTEGRATED_FEEDSTOCK_COSTS AS
WITH marketplace_prices AS (
    SELECT 
        price_date,
        commodity_code,
        price_usd,
        unit,
        'MARKETPLACE' AS data_source
    FROM MARKETPLACE_CRUDE_OIL_PRICES
),
synthetic_prices AS (
    SELECT 
        index_date AS price_date,
        feedstock_code AS commodity_code,
        price_per_mt AS price_usd,
        'USD/MT' AS unit,
        'SYNTHETIC' AS data_source
    FROM CHEMICALS_DB.ATOMIC.MARKET_INDEX
)
-- Use marketplace when available, fallback to synthetic
SELECT COALESCE(m.price_date, s.price_date) AS price_date,
       COALESCE(m.commodity_code, s.commodity_code) AS commodity_code,
       COALESCE(m.price_usd, s.price_usd) AS price_usd,
       COALESCE(m.unit, s.unit) AS unit,
       COALESCE(m.data_source, s.data_source) AS data_source
FROM marketplace_prices m
FULL OUTER JOIN synthetic_prices s 
    ON m.price_date = s.price_date 
    AND m.commodity_code = s.commodity_code
ORDER BY price_date DESC, commodity_code;
*/

-- =============================================================================
-- VIEW 6: Correlation Analysis - Crude Oil vs Chemical Demand
-- This creates the "Hidden Discovery" with real market data
-- =============================================================================

/*
CREATE OR REPLACE VIEW CRUDE_DEMAND_CORRELATION AS
WITH crude_prices AS (
    SELECT 
        price_date,
        price_usd AS crude_price
    FROM MARKETPLACE_CRUDE_OIL_PRICES
    WHERE commodity_code = 'WTI_CRUDE'
),
demand AS (
    SELECT 
        demand_date,
        SUM(demand_mt) AS total_demand
    FROM CHEMICALS_DB.ATOMIC.DEMAND_HISTORY
    GROUP BY demand_date
),
lagged AS (
    SELECT 
        d.demand_date,
        d.total_demand,
        c.crude_price,
        LAG(c.crude_price, 30) OVER (ORDER BY d.demand_date) AS crude_price_lag_30d,
        LAG(c.crude_price, 60) OVER (ORDER BY d.demand_date) AS crude_price_lag_60d,
        LAG(c.crude_price, 90) OVER (ORDER BY d.demand_date) AS crude_price_lag_90d
    FROM demand d
    LEFT JOIN crude_prices c ON d.demand_date = c.price_date
)
SELECT 
    demand_date,
    total_demand,
    crude_price,
    crude_price_lag_30d,
    crude_price_lag_60d,
    crude_price_lag_90d,
    -- Correlation proxy: normalized values
    ROUND((total_demand - AVG(total_demand) OVER ()) / STDDEV(total_demand) OVER (), 3) AS demand_zscore,
    ROUND((crude_price_lag_60d - AVG(crude_price_lag_60d) OVER ()) / NULLIF(STDDEV(crude_price_lag_60d) OVER (), 0), 3) AS crude_zscore_60d
FROM lagged
WHERE crude_price_lag_60d IS NOT NULL
ORDER BY demand_date DESC;
*/

-- =============================================================================
-- STATUS CHECK: Run this to see what's available
-- =============================================================================

SELECT 'Marketplace Integration Status' AS info;
SELECT 'Run the queries below after installing Cybersyn from Marketplace' AS note;

-- Check if Cybersyn database exists
-- SHOW DATABASES LIKE '%CYBERSYN%';

