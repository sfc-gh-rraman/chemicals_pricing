-- =============================================================================
-- DDL Script: 004_ATOMIC_EXTENSIONS.sql
-- Demo: Chemicals Integrated Pricing & Supply Chain Optimization
-- Description: Create ATOMIC layer extension tables (project-specific)
-- Dependencies: 003_ATOMIC_CORE.sql
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA ATOMIC;

-- =============================================================================
-- MARKET_INDEX: External market price indices (normalized)
-- DRD Reference: Section 3.3 - ATOMIC.MARKET_INDEX (Extension)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.MARKET_INDEX (
    index_id                INTEGER         NOT NULL,
    index_name              VARCHAR(100)    NOT NULL,      -- e.g., 'Crude Oil Brent', 'Ethylene CFR NEA'
    index_code              VARCHAR(50),                   -- Standardized code
    index_date              DATE            NOT NULL,
    price_value             FLOAT           NOT NULL,
    currency                VARCHAR(10)     DEFAULT 'USD',
    unit                    VARCHAR(20)     DEFAULT 'MT',
    price_type              VARCHAR(20),                   -- SPOT, CONTRACT, FUTURES
    region                  VARCHAR(50),
    source                  VARCHAR(50),
    
    -- Price changes for trend analysis
    price_change_1d         FLOAT,                         -- 1-day change
    price_change_7d         FLOAT,                         -- 7-day change
    price_change_30d        FLOAT,                         -- 30-day change
    price_change_pct_1d     FLOAT,
    price_change_pct_7d     FLOAT,
    price_change_pct_30d    FLOAT,
    
    -- Rolling averages
    price_avg_7d            FLOAT,
    price_avg_30d           FLOAT,
    price_volatility_30d    FLOAT,                         -- 30-day standard deviation
    
    PRIMARY KEY (index_id)
);

COMMENT ON TABLE ATOMIC.MARKET_INDEX IS 'Normalized market indices with calculated trends (DRD Extension)';

-- =============================================================================
-- CONTRACT_TERM: Customer pricing contracts and formulas
-- DRD Reference: Section 3.3 - ATOMIC.CONTRACT_TERM (Extension)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.CONTRACT_TERM (
    term_id                 INTEGER         NOT NULL,
    contract_id             VARCHAR(50)     NOT NULL,
    customer_id             VARCHAR(50)     NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    effective_date          DATE            NOT NULL,
    expiration_date         DATE,
    
    -- Pricing formula (e.g., "Index + $50/MT")
    price_formula           VARCHAR(200),                  -- Text description
    base_index_code         VARCHAR(50),                   -- Reference index
    index_adjustment        FLOAT           DEFAULT 0,     -- +/- adjustment to index
    fixed_price             FLOAT,                         -- If fixed price contract
    floor_price             FLOAT,                         -- Minimum price
    ceiling_price           FLOAT,                         -- Maximum price
    
    -- Volume commitments
    min_volume_mt           FLOAT,                         -- Minimum annual volume
    max_volume_mt           FLOAT,                         -- Maximum annual volume
    volume_tier_1_mt        FLOAT,                         -- Volume tier breakpoints
    volume_tier_1_discount  FLOAT,
    volume_tier_2_mt        FLOAT,
    volume_tier_2_discount  FLOAT,
    
    -- Payment terms
    payment_terms_days      INTEGER         DEFAULT 30,
    early_pay_discount_pct  FLOAT           DEFAULT 0,
    
    -- Contract status
    contract_status         VARCHAR(50)     DEFAULT 'ACTIVE',
    contract_type           VARCHAR(50),                   -- SPOT, ANNUAL, MULTI_YEAR
    
    PRIMARY KEY (term_id)
);

COMMENT ON TABLE ATOMIC.CONTRACT_TERM IS 'Customer contract terms with pricing formulas (DRD Extension)';

-- =============================================================================
-- FEEDSTOCK_MAPPING: Maps products to their feedstock dependencies
-- Used for cost-to-serve calculations
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.FEEDSTOCK_MAPPING (
    mapping_id              INTEGER         NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    feedstock_index_code    VARCHAR(50)     NOT NULL,      -- Link to MARKET_INDEX
    conversion_factor       FLOAT           DEFAULT 1.0,   -- Units feedstock per unit product
    cost_weight_pct         FLOAT,                         -- Percentage of total feedstock cost
    effective_date          DATE            DEFAULT CURRENT_DATE(),
    
    PRIMARY KEY (mapping_id)
);

COMMENT ON TABLE ATOMIC.FEEDSTOCK_MAPPING IS 'Maps products to feedstock indices for cost calculations';

-- =============================================================================
-- LOGISTICS_COST: Freight and logistics costs by lane
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.LOGISTICS_COST (
    cost_id                 INTEGER         NOT NULL,
    origin_site_id          VARCHAR(50)     NOT NULL,
    destination_region      VARCHAR(50)     NOT NULL,
    transport_mode          VARCHAR(50),                   -- TRUCK, RAIL, VESSEL, PIPELINE
    cost_per_mt             FLOAT           NOT NULL,
    transit_days            INTEGER,
    effective_date          DATE,
    expiration_date         DATE,
    carrier                 VARCHAR(100),
    
    PRIMARY KEY (cost_id)
);

COMMENT ON TABLE ATOMIC.LOGISTICS_COST IS 'Freight costs by origin-destination lane';

-- =============================================================================
-- DEMAND_HISTORY: Historical demand for forecasting
-- Aggregated from sales orders for ML training
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.DEMAND_HISTORY (
    demand_id               INTEGER         NOT NULL,
    demand_date             DATE            NOT NULL,      -- Weekly or monthly bucket
    product_id              VARCHAR(50)     NOT NULL,
    region                  VARCHAR(50)     NOT NULL,
    customer_segment        VARCHAR(50),
    
    -- Volume metrics
    volume_mt               FLOAT           NOT NULL,
    order_count             INTEGER,
    customer_count          INTEGER,
    
    -- Price metrics
    avg_price_per_mt        FLOAT,
    min_price_per_mt        FLOAT,
    max_price_per_mt        FLOAT,
    
    -- External factors (for ML features)
    crude_oil_price         FLOAT,                         -- Brent crude on that date
    gdp_index               FLOAT,                         -- Regional GDP indicator
    manufacturing_pmi       FLOAT,                         -- PMI indicator
    
    PRIMARY KEY (demand_id)
);

COMMENT ON TABLE ATOMIC.DEMAND_HISTORY IS 'Aggregated demand history for ML demand forecasting';

-- =============================================================================
-- Enable change tracking for Cortex Search
-- =============================================================================
-- ALTER TABLE RAW.MARKET_REPORTS SET CHANGE_TRACKING = TRUE;

-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN SCHEMA ATOMIC;
