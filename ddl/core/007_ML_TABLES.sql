-- =============================================================================
-- ML Tables and Views for Price Elasticity & Optimization
-- =============================================================================
-- Creates:
-- 1. ML_TRAINING_DATA - Unified view for model training
-- 2. PRICE_ELASTICITY_LINEAR - Linear model results
-- 3. ELASTICITY_MATRIX - Cross-price elasticity matrix (SUR)
-- 4. OPTIMAL_PRICING - Optimization results
-- 5. MODEL_METADATA - Model versioning and tracking
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- VIEW 1: ML_TRAINING_DATA
-- Unified dataset joining sales, products, costs, and market indicators
-- =============================================================================

CREATE OR REPLACE VIEW ML_TRAINING_DATA AS
WITH 
-- Aggregate daily sales by product and region
daily_sales AS (
    SELECT 
        so.order_date,
        so.product_id,
        p.product_name,
        p.product_family,
        p.product_category,
        so.sales_region AS region,
        COUNT(DISTINCT so.order_id) AS num_orders,
        COUNT(DISTINCT so.customer_id) AS num_customers,
        SUM(so.quantity_mt) AS total_quantity_mt,
        SUM(so.total_revenue) AS total_revenue,
        AVG(so.unit_price) AS avg_price_per_mt,
        STDDEV(so.unit_price) AS price_stddev
    FROM CHEMICALS_DB.ATOMIC.SALES_ORDER so
    JOIN CHEMICALS_DB.ATOMIC.PRODUCT p ON so.product_id = p.product_id
    GROUP BY 
        so.order_date, so.product_id, p.product_name, 
        p.product_family, p.product_category, so.sales_region
),

-- Get variable costs by product
product_costs AS (
    SELECT 
        product_id,
        AVG(total_cost_to_serve_per_mt) AS avg_variable_cost
    FROM CHEMICALS_DB.CHEMICAL_OPS.COST_TO_SERVE
    GROUP BY product_id
),

-- Get market indicators with forward fill for missing dates
market_indicators AS (
    SELECT DISTINCT
        f.price_date,
        f.index_value AS fuel_oil_cpi,
        c.index_value AS core_cpi,
        i.index_value AS industrial_production
    FROM MARKETPLACE_FUEL_OIL_CPI f
    LEFT JOIN MARKETPLACE_CORE_CPI c ON f.price_date = c.price_date
    LEFT JOIN MARKETPLACE_INDUSTRIAL_PRODUCTION i ON f.price_date = i.price_date
),

-- Compute lagged prices for each product (30-day rolling average)
price_lags AS (
    SELECT 
        order_date,
        product_id,
        AVG(avg_price_per_mt) OVER (
            PARTITION BY product_id 
            ORDER BY order_date 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS price_avg_30d,
        AVG(avg_price_per_mt) OVER (
            PARTITION BY product_id 
            ORDER BY order_date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS price_avg_7d
    FROM daily_sales
)

SELECT 
    -- Identifiers
    ds.order_date,
    ds.product_id,
    ds.product_name,
    ds.product_family,
    ds.product_category,
    ds.region,
    
    -- Volume metrics
    ds.num_orders,
    ds.num_customers,
    ds.total_quantity_mt,
    ds.total_revenue,
    
    -- Price metrics
    ds.avg_price_per_mt,
    ds.price_stddev,
    pl.price_avg_7d,
    pl.price_avg_30d,
    
    -- Log transforms for elasticity estimation
    LN(NULLIF(ds.total_quantity_mt, 0)) AS ln_quantity,
    LN(NULLIF(ds.avg_price_per_mt, 0)) AS ln_price,
    
    -- Price relatives
    ds.avg_price_per_mt / NULLIF(pl.price_avg_30d, 0) AS price_vs_avg_30d,
    (ds.avg_price_per_mt - pl.price_avg_7d) / NULLIF(pl.price_avg_7d, 0) AS price_change_7d_pct,
    
    -- Cost metrics
    pc.avg_variable_cost,
    ds.avg_price_per_mt - pc.avg_variable_cost AS margin_per_mt,
    (ds.avg_price_per_mt - pc.avg_variable_cost) / NULLIF(ds.avg_price_per_mt, 0) AS margin_pct,
    
    -- Market indicators (join to closest available date)
    mi.fuel_oil_cpi,
    mi.core_cpi,
    mi.industrial_production,
    
    -- Temporal features
    MONTH(ds.order_date) AS month,
    QUARTER(ds.order_date) AS quarter,
    YEAR(ds.order_date) AS year,
    DAYOFWEEK(ds.order_date) AS day_of_week,
    CASE WHEN QUARTER(ds.order_date) = 4 THEN 1 ELSE 0 END AS is_q4,
    CASE WHEN MONTH(ds.order_date) IN (12, 1, 2) THEN 1 ELSE 0 END AS is_winter
    
FROM daily_sales ds
LEFT JOIN product_costs pc ON ds.product_id = pc.product_id
LEFT JOIN price_lags pl ON ds.order_date = pl.order_date AND ds.product_id = pl.product_id
LEFT JOIN market_indicators mi ON DATE_TRUNC('month', ds.order_date) = DATE_TRUNC('month', mi.price_date)
WHERE ds.total_quantity_mt > 0
  AND ds.avg_price_per_mt > 0
ORDER BY ds.order_date, ds.product_id;

-- =============================================================================
-- TABLE 2: PRICE_ELASTICITY_LINEAR
-- Stores per-product elasticity from linear regression
-- =============================================================================

CREATE TABLE IF NOT EXISTS PRICE_ELASTICITY_LINEAR (
    model_version VARCHAR(50) NOT NULL,
    estimation_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100),
    product_family VARCHAR(50),
    
    -- Elasticity estimates
    own_elasticity FLOAT,
    own_elasticity_se FLOAT,
    own_elasticity_tstat FLOAT,
    own_elasticity_pvalue FLOAT,
    
    -- Control variable coefficients
    cpi_coefficient FLOAT,
    cpi_pvalue FLOAT,
    ip_coefficient FLOAT,
    ip_pvalue FLOAT,
    
    -- Model diagnostics
    n_observations INT,
    r_squared FLOAT,
    adj_r_squared FLOAT,
    rmse FLOAT,
    aic FLOAT,
    
    -- Metadata
    training_start_date DATE,
    training_end_date DATE,
    
    PRIMARY KEY (model_version, product_id)
);

-- =============================================================================
-- TABLE 3: ELASTICITY_MATRIX
-- Stores cross-price elasticity matrix from SUR model
-- =============================================================================

CREATE TABLE IF NOT EXISTS ELASTICITY_MATRIX (
    model_version VARCHAR(50) NOT NULL,
    estimation_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Matrix cell identifier
    demand_product_id VARCHAR(50) NOT NULL,    -- Row: whose demand changes
    demand_product_name VARCHAR(100),
    price_product_id VARCHAR(50) NOT NULL,     -- Column: whose price changed
    price_product_name VARCHAR(100),
    
    -- Elasticity estimate
    elasticity FLOAT,
    std_error FLOAT,
    t_statistic FLOAT,
    p_value FLOAT,
    
    -- Interpretation flags
    is_own_price BOOLEAN,                       -- Diagonal element
    is_significant BOOLEAN,                     -- p < 0.05
    relationship_type VARCHAR(20),              -- 'own', 'substitute', 'complement', 'independent'
    
    PRIMARY KEY (model_version, demand_product_id, price_product_id)
);

-- =============================================================================
-- TABLE 4: OPTIMAL_PRICING
-- Stores optimization results
-- =============================================================================

CREATE TABLE IF NOT EXISTS OPTIMAL_PRICING (
    scenario_id VARCHAR(50) NOT NULL,
    run_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Product info
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100),
    region VARCHAR(50),
    
    -- Current state
    current_price FLOAT,
    current_volume FLOAT,
    current_revenue FLOAT,
    current_margin FLOAT,
    
    -- Optimal recommendations
    optimal_price FLOAT,
    predicted_volume FLOAT,
    predicted_revenue FLOAT,
    predicted_margin FLOAT,
    
    -- Changes
    price_change_dollars FLOAT,
    price_change_pct FLOAT,
    volume_change_pct FLOAT,
    margin_change_pct FLOAT,
    
    -- Constraint status
    is_at_floor BOOLEAN,
    is_at_ceiling BOOLEAN,
    is_margin_binding BOOLEAN,
    binding_constraints ARRAY,
    
    PRIMARY KEY (scenario_id, product_id, region)
);

-- =============================================================================
-- TABLE 5: OPTIMIZATION_SCENARIOS
-- Stores scenario configurations
-- =============================================================================

CREATE TABLE IF NOT EXISTS OPTIMIZATION_SCENARIOS (
    scenario_id VARCHAR(50) PRIMARY KEY,
    scenario_name VARCHAR(200),
    created_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Objective
    objective_type VARCHAR(20),                 -- 'profit', 'revenue', 'volume'
    
    -- Scope
    product_scope ARRAY,                        -- List of product_ids
    region_scope ARRAY,                         -- List of regions
    
    -- Constraints configuration
    min_margin_pct FLOAT,
    max_price_increase_pct FLOAT,
    max_price_decrease_pct FLOAT,
    respect_capacity BOOLEAN,
    honor_contracts BOOLEAN,
    maintain_price_ladder BOOLEAN,
    
    -- Market conditions
    include_cpi_adjustment BOOLEAN,
    cpi_value FLOAT,
    include_ip_factor BOOLEAN,
    ip_value FLOAT,
    
    -- Results summary
    solver_status VARCHAR(50),
    solve_time_seconds FLOAT,
    total_current_profit FLOAT,
    total_optimal_profit FLOAT,
    profit_improvement_pct FLOAT,
    total_current_revenue FLOAT,
    total_optimal_revenue FLOAT,
    
    -- Model reference
    elasticity_model_version VARCHAR(50)
);

-- =============================================================================
-- TABLE 6: MODEL_METADATA
-- Tracks model versions and training metadata
-- =============================================================================

CREATE TABLE IF NOT EXISTS MODEL_METADATA (
    model_version VARCHAR(50) PRIMARY KEY,
    model_type VARCHAR(50) NOT NULL,            -- 'linear', 'sur', '2sls', 'blp'
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100),
    
    -- Training info
    training_start_date DATE,
    training_end_date DATE,
    n_products INT,
    n_observations INT,
    
    -- Model quality
    avg_r_squared FLOAT,
    avg_rmse FLOAT,
    
    -- Features used
    features_used ARRAY,
    market_indicators_used ARRAY,
    
    -- Status
    is_active BOOLEAN DEFAULT FALSE,
    is_production BOOLEAN DEFAULT FALSE,
    
    -- Notes
    description VARCHAR(1000),
    notes VARCHAR(2000)
);

-- =============================================================================
-- VERIFY CREATION
-- =============================================================================

SELECT 'ML_TRAINING_DATA' AS object_name, 'VIEW' AS object_type, COUNT(*) AS row_count 
FROM ML_TRAINING_DATA
UNION ALL
SELECT 'PRICE_ELASTICITY_LINEAR', 'TABLE', 0
UNION ALL
SELECT 'ELASTICITY_MATRIX', 'TABLE', 0
UNION ALL
SELECT 'OPTIMAL_PRICING', 'TABLE', 0
UNION ALL
SELECT 'OPTIMIZATION_SCENARIOS', 'TABLE', 0
UNION ALL
SELECT 'MODEL_METADATA', 'TABLE', 0;
