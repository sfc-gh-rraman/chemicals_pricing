-- =============================================================================
-- Activate Marketplace Integration for Chemicals Pricing
-- =============================================================================
-- Run this script AFTER installing Cybersyn from Snowflake Marketplace
-- 
-- This creates production-ready views that integrate real market data
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- STEP 1: Verify Cybersyn is installed
-- =============================================================================

-- This will error if Cybersyn is not installed
SELECT 'Checking Cybersyn installation...' AS status;
SELECT COUNT(*) AS fred_timeseries_count FROM CYBERSYN.ECONOMY.FRED_TIMESERIES LIMIT 1;

-- =============================================================================
-- STEP 2: Create Crude Oil Price View
-- =============================================================================

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
    'USD/BBL' AS unit,
    'CYBERSYN_FRED' AS data_source
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id IN ('DCOILWTICO', 'DCOILBRENTEU')
  AND date >= DATEADD(year, -3, CURRENT_DATE())
ORDER BY date DESC;

SELECT 'Created MARKETPLACE_CRUDE_OIL_PRICES' AS status, COUNT(*) AS rows FROM MARKETPLACE_CRUDE_OIL_PRICES;

-- =============================================================================
-- STEP 3: Create Natural Gas Price View
-- =============================================================================

CREATE OR REPLACE VIEW MARKETPLACE_NATURAL_GAS_PRICES AS
SELECT 
    date AS price_date,
    'NATURAL_GAS' AS commodity_code,
    value AS price_usd,
    'USD/MMBTU' AS unit,
    'CYBERSYN_FRED' AS data_source
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id = 'DHHNGSP'  -- Henry Hub Natural Gas Spot Price
  AND date >= DATEADD(year, -3, CURRENT_DATE())
ORDER BY date DESC;

SELECT 'Created MARKETPLACE_NATURAL_GAS_PRICES' AS status, COUNT(*) AS rows FROM MARKETPLACE_NATURAL_GAS_PRICES;

-- =============================================================================
-- STEP 4: Create Chemical PPI View (Producer Price Index)
-- =============================================================================

CREATE OR REPLACE VIEW MARKETPLACE_CHEMICAL_PPI AS
SELECT 
    date AS price_date,
    'CHEMICAL_PPI' AS index_code,
    value AS index_value,
    ROUND((value / LAG(value, 12) OVER (ORDER BY date) - 1) * 100, 2) AS yoy_change_pct,
    'CYBERSYN_FRED' AS data_source
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id = 'PCU325325'  -- PPI: Chemical Manufacturing
  AND date >= DATEADD(year, -5, CURRENT_DATE())
ORDER BY date DESC;

SELECT 'Created MARKETPLACE_CHEMICAL_PPI' AS status, COUNT(*) AS rows FROM MARKETPLACE_CHEMICAL_PPI;

-- =============================================================================
-- STEP 5: Create Economic Indicators View
-- =============================================================================

CREATE OR REPLACE VIEW MARKETPLACE_ECONOMIC_INDICATORS AS
SELECT 
    date AS indicator_date,
    series_id,
    CASE series_id
        WHEN 'INDPRO' THEN 'Industrial_Production_Index'
        WHEN 'DGORDER' THEN 'Durable_Goods_Orders'
        WHEN 'HOUST' THEN 'Housing_Starts'
        WHEN 'UMCSENT' THEN 'Consumer_Sentiment'
        WHEN 'UNRATE' THEN 'Unemployment_Rate'
        WHEN 'CPIAUCSL' THEN 'Consumer_Price_Index'
    END AS indicator_name,
    value AS indicator_value,
    'CYBERSYN_FRED' AS data_source
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES
WHERE series_id IN ('INDPRO', 'DGORDER', 'HOUST', 'UMCSENT', 'UNRATE', 'CPIAUCSL')
  AND date >= DATEADD(year, -3, CURRENT_DATE())
ORDER BY date DESC, series_id;

SELECT 'Created MARKETPLACE_ECONOMIC_INDICATORS' AS status, COUNT(*) AS rows FROM MARKETPLACE_ECONOMIC_INDICATORS;

-- =============================================================================
-- STEP 6: Create Unified Feedstock Cost View
-- Joins marketplace crude oil with our synthetic MARKET_INDEX table
-- =============================================================================

CREATE OR REPLACE VIEW INTEGRATED_FEEDSTOCK_COSTS AS
WITH marketplace_crude AS (
    -- Real crude oil from marketplace
    SELECT 
        price_date,
        commodity_code,
        price_usd,
        unit,
        data_source
    FROM MARKETPLACE_CRUDE_OIL_PRICES
    WHERE commodity_code = 'WTI_CRUDE'
),
marketplace_gas AS (
    -- Real natural gas from marketplace
    SELECT 
        price_date,
        commodity_code,
        price_usd,
        unit,
        data_source
    FROM MARKETPLACE_NATURAL_GAS_PRICES
),
synthetic_feedstocks AS (
    -- Our synthetic feedstock data
    SELECT 
        index_date AS price_date,
        feedstock_code AS commodity_code,
        price_per_mt AS price_usd,
        'USD/MT' AS unit,
        'SYNTHETIC' AS data_source
    FROM CHEMICALS_DB.ATOMIC.MARKET_INDEX
)
-- Combine all sources
SELECT * FROM marketplace_crude
UNION ALL
SELECT * FROM marketplace_gas
UNION ALL
SELECT * FROM synthetic_feedstocks
ORDER BY price_date DESC, commodity_code;

SELECT 'Created INTEGRATED_FEEDSTOCK_COSTS' AS status, COUNT(*) AS rows FROM INTEGRATED_FEEDSTOCK_COSTS;

-- =============================================================================
-- STEP 7: Enhanced Margin Analyzer with Real Market Data
-- =============================================================================

CREATE OR REPLACE VIEW MARGIN_ANALYZER_ENHANCED AS
WITH latest_crude AS (
    SELECT price_usd AS current_crude_price
    FROM MARKETPLACE_CRUDE_OIL_PRICES
    WHERE commodity_code = 'WTI_CRUDE'
    ORDER BY price_date DESC
    LIMIT 1
),
latest_gas AS (
    SELECT price_usd AS current_gas_price
    FROM MARKETPLACE_NATURAL_GAS_PRICES
    ORDER BY price_date DESC
    LIMIT 1
),
crude_trend AS (
    SELECT 
        AVG(CASE WHEN price_date >= DATEADD(day, -7, CURRENT_DATE()) THEN price_usd END) AS avg_7d,
        AVG(CASE WHEN price_date >= DATEADD(day, -30, CURRENT_DATE()) THEN price_usd END) AS avg_30d
    FROM MARKETPLACE_CRUDE_OIL_PRICES
    WHERE commodity_code = 'WTI_CRUDE'
)
SELECT 
    ma.*,
    lc.current_crude_price,
    lg.current_gas_price,
    ct.avg_7d AS crude_7d_avg,
    ct.avg_30d AS crude_30d_avg,
    CASE 
        WHEN ct.avg_7d > ct.avg_30d * 1.05 THEN 'RISING'
        WHEN ct.avg_7d < ct.avg_30d * 0.95 THEN 'FALLING'
        ELSE 'STABLE'
    END AS crude_trend,
    'MARKETPLACE_ENHANCED' AS data_quality
FROM CHEMICALS_DB.CHEMICAL_OPS.MARGIN_ANALYZER ma
CROSS JOIN latest_crude lc
CROSS JOIN latest_gas lg
CROSS JOIN crude_trend ct;

SELECT 'Created MARGIN_ANALYZER_ENHANCED' AS status, COUNT(*) AS rows FROM MARGIN_ANALYZER_ENHANCED;

-- =============================================================================
-- STEP 8: Crude-Demand Correlation View (The "Hidden Discovery")
-- =============================================================================

CREATE OR REPLACE VIEW CRUDE_DEMAND_CORRELATION AS
WITH crude_prices AS (
    SELECT 
        price_date,
        price_usd AS crude_price
    FROM MARKETPLACE_CRUDE_OIL_PRICES
    WHERE commodity_code = 'WTI_CRUDE'
),
daily_demand AS (
    SELECT 
        demand_date,
        SUM(demand_mt) AS total_demand
    FROM CHEMICALS_DB.ATOMIC.DEMAND_HISTORY
    GROUP BY demand_date
),
lagged_analysis AS (
    SELECT 
        d.demand_date,
        d.total_demand,
        c.crude_price AS same_day_crude,
        c30.crude_price AS crude_lag_30d,
        c60.crude_price AS crude_lag_60d,
        c90.crude_price AS crude_lag_90d
    FROM daily_demand d
    LEFT JOIN crude_prices c ON d.demand_date = c.price_date
    LEFT JOIN crude_prices c30 ON d.demand_date = DATEADD(day, 30, c30.price_date)
    LEFT JOIN crude_prices c60 ON d.demand_date = DATEADD(day, 60, c60.price_date)
    LEFT JOIN crude_prices c90 ON d.demand_date = DATEADD(day, 90, c90.price_date)
    WHERE d.demand_date >= '2024-01-01'
)
SELECT 
    demand_date,
    total_demand,
    same_day_crude,
    crude_lag_30d,
    crude_lag_60d,
    crude_lag_90d,
    -- Standardized values for correlation visualization
    ROUND((total_demand - AVG(total_demand) OVER ()) / NULLIF(STDDEV(total_demand) OVER (), 0), 3) AS demand_zscore,
    ROUND((crude_lag_60d - AVG(crude_lag_60d) OVER ()) / NULLIF(STDDEV(crude_lag_60d) OVER (), 0), 3) AS crude_60d_zscore
FROM lagged_analysis
WHERE crude_lag_60d IS NOT NULL
ORDER BY demand_date DESC;

SELECT 'Created CRUDE_DEMAND_CORRELATION' AS status;

-- =============================================================================
-- STEP 9: Summary
-- =============================================================================

SELECT '=== MARKETPLACE INTEGRATION COMPLETE ===' AS status;

SELECT view_name, row_count
FROM (
    SELECT 'MARKETPLACE_CRUDE_OIL_PRICES' AS view_name, COUNT(*) AS row_count FROM MARKETPLACE_CRUDE_OIL_PRICES
    UNION ALL SELECT 'MARKETPLACE_NATURAL_GAS_PRICES', COUNT(*) FROM MARKETPLACE_NATURAL_GAS_PRICES
    UNION ALL SELECT 'MARKETPLACE_CHEMICAL_PPI', COUNT(*) FROM MARKETPLACE_CHEMICAL_PPI
    UNION ALL SELECT 'MARKETPLACE_ECONOMIC_INDICATORS', COUNT(*) FROM MARKETPLACE_ECONOMIC_INDICATORS
    UNION ALL SELECT 'INTEGRATED_FEEDSTOCK_COSTS', COUNT(*) FROM INTEGRATED_FEEDSTOCK_COSTS
    UNION ALL SELECT 'MARGIN_ANALYZER_ENHANCED', COUNT(*) FROM MARGIN_ANALYZER_ENHANCED
);

