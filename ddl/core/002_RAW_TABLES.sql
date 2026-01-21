-- =============================================================================
-- DDL Script: 002_RAW_TABLES.sql
-- Demo: Chemicals Integrated Pricing & Supply Chain Optimization
-- Description: Create RAW layer tables (landing zone)
-- Dependencies: 001_DATABASE_SCHEMAS.sql
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA RAW;

-- =============================================================================
-- MARKET_FEEDSTOCK: External price indices (API feed simulation)
-- Source: ICIS, Platts, commodity exchanges
-- =============================================================================
CREATE TABLE IF NOT EXISTS RAW.MARKET_FEEDSTOCK (
    record_id               INTEGER         NOT NULL,
    index_name              VARCHAR(100)    NOT NULL,      -- e.g., 'Ethylene CFR NEA', 'Propylene FOB USG'
    index_code              VARCHAR(50),                   -- e.g., 'ICIS-ETH-NEA', 'PLATTS-PP-USG'
    price_date              DATE            NOT NULL,
    price_value             FLOAT           NOT NULL,
    price_currency          VARCHAR(10)     DEFAULT 'USD',
    price_unit              VARCHAR(20)     DEFAULT 'MT',  -- Metric Ton
    price_type              VARCHAR(20)     DEFAULT 'SPOT', -- SPOT, CONTRACT, FUTURES
    region                  VARCHAR(50),                   -- NEA, SEA, USG, ARA
    source                  VARCHAR(50),                   -- ICIS, PLATTS, CME
    load_timestamp          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (record_id)
);

COMMENT ON TABLE RAW.MARKET_FEEDSTOCK IS 'External market price indices from ICIS/Platts - daily feedstock prices';

-- =============================================================================
-- ERP_SALES_ORDERS: Transaction history from ERP system
-- Source: SAP, Oracle, or other ERP
-- =============================================================================
CREATE TABLE IF NOT EXISTS RAW.ERP_SALES_ORDERS (
    order_id                VARCHAR(50)     NOT NULL,
    order_line_id           INTEGER         NOT NULL,
    order_date              DATE            NOT NULL,
    ship_date               DATE,
    customer_id             VARCHAR(50)     NOT NULL,
    customer_name           VARCHAR(200),
    product_id              VARCHAR(50)     NOT NULL,
    product_name            VARCHAR(200),
    quantity_mt             FLOAT           NOT NULL,      -- Quantity in Metric Tons
    unit_price              FLOAT           NOT NULL,      -- Price per MT
    total_amount            FLOAT,
    currency                VARCHAR(10)     DEFAULT 'USD',
    sales_region            VARCHAR(50),                   -- North America, Europe, Asia Pacific
    sales_channel           VARCHAR(50),                   -- Direct, Distributor, Spot
    customer_segment        VARCHAR(50),                   -- Industrial, Consumer, Agricultural
    contract_id             VARCHAR(50),                   -- Link to contract if applicable
    discount_pct            FLOAT           DEFAULT 0,
    rebate_amount           FLOAT           DEFAULT 0,
    freight_cost            FLOAT           DEFAULT 0,
    ship_from_site          VARCHAR(50),
    ship_to_location        VARCHAR(100),
    incoterms               VARCHAR(20),                   -- FOB, CIF, DDP
    load_timestamp          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (order_id, order_line_id)
);

COMMENT ON TABLE RAW.ERP_SALES_ORDERS IS 'Sales transaction history from ERP system';

-- =============================================================================
-- PLANT_COSTS: Allocated fixed/variable costs from manufacturing
-- Source: Cost accounting system
-- =============================================================================
CREATE TABLE IF NOT EXISTS RAW.PLANT_COSTS (
    cost_record_id          INTEGER         NOT NULL,
    cost_date               DATE            NOT NULL,
    plant_id                VARCHAR(50)     NOT NULL,
    plant_name              VARCHAR(100),
    product_id              VARCHAR(50)     NOT NULL,
    cost_type               VARCHAR(50)     NOT NULL,      -- FEEDSTOCK, ENERGY, LABOR, OVERHEAD, DEPRECIATION
    cost_category           VARCHAR(50),                   -- VARIABLE, FIXED
    cost_amount             FLOAT           NOT NULL,
    cost_per_mt             FLOAT,                         -- Cost per metric ton produced
    production_volume_mt    FLOAT,                         -- Volume this cost applies to
    currency                VARCHAR(10)     DEFAULT 'USD',
    allocation_method       VARCHAR(50),                   -- DIRECT, ACTIVITY_BASED, VOLUME_BASED
    load_timestamp          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (cost_record_id)
);

COMMENT ON TABLE RAW.PLANT_COSTS IS 'Manufacturing cost allocations by plant and product';

-- =============================================================================
-- MARKET_REPORTS: Unstructured market intelligence documents
-- Source: Analyst reports, trade journals, earnings transcripts
-- For Cortex Search indexing
-- =============================================================================
CREATE TABLE IF NOT EXISTS RAW.MARKET_REPORTS (
    report_id               INTEGER         NOT NULL,
    report_date             DATE            NOT NULL,
    report_type             VARCHAR(50),                   -- ANALYST_REPORT, EARNINGS_CALL, TRADE_JOURNAL
    report_title            VARCHAR(500),
    report_source           VARCHAR(100),                  -- Goldman Sachs, ICIS, ChemWeek
    chemical_name           VARCHAR(100),                  -- Primary chemical discussed
    region                  VARCHAR(50),
    report_summary          VARCHAR(2000),
    report_content          TEXT,                          -- Full text content
    file_path               VARCHAR(500),                  -- Path to PDF if applicable
    author                  VARCHAR(100),
    load_timestamp          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (report_id)
);

COMMENT ON TABLE RAW.MARKET_REPORTS IS 'Market intelligence documents for Cortex Search';

-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN SCHEMA RAW;
