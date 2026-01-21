-- =============================================================================
-- DDL Script: 003_ATOMIC_CORE.sql
-- Demo: Chemicals Integrated Pricing & Supply Chain Optimization
-- Description: Create ATOMIC layer core entity tables
-- Dependencies: 001_DATABASE_SCHEMAS.sql
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA ATOMIC;

-- =============================================================================
-- PRODUCT: Chemical product master
-- DRD Reference: Section 3.3 - ATOMIC.PRODUCT
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.PRODUCT (
    product_id              VARCHAR(50)     NOT NULL,
    product_name            VARCHAR(200)    NOT NULL,
    cas_number              VARCHAR(20),                   -- Chemical Abstract Service number
    product_family          VARCHAR(100),                  -- Olefins, Aromatics, Polymers, Intermediates
    product_category        VARCHAR(100),                  -- Ethylene, Propylene, Polyethylene, etc.
    product_grade           VARCHAR(50),                   -- Grade A, Grade B, Industrial, Food Grade
    molecular_weight        FLOAT,
    density_kg_m3           FLOAT,
    hazard_class            VARCHAR(50),
    un_number               VARCHAR(20),
    base_unit               VARCHAR(20)     DEFAULT 'MT',  -- Metric Ton
    standard_cost_per_mt    FLOAT,                         -- Standard cost for planning
    target_margin_pct       FLOAT           DEFAULT 15,    -- Target margin percentage
    min_order_quantity_mt   FLOAT           DEFAULT 1,
    lead_time_days          INTEGER         DEFAULT 7,
    is_active               BOOLEAN         DEFAULT TRUE,
    created_date            DATE            DEFAULT CURRENT_DATE(),
    
    PRIMARY KEY (product_id)
);

COMMENT ON TABLE ATOMIC.PRODUCT IS 'Chemical product master with CAS numbers and product families';

-- =============================================================================
-- CUSTOMER: Customer master
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.CUSTOMER (
    customer_id             VARCHAR(50)     NOT NULL,
    customer_name           VARCHAR(200)    NOT NULL,
    customer_segment        VARCHAR(50),                   -- Industrial, Consumer, Agricultural
    customer_tier           VARCHAR(20),                   -- Platinum, Gold, Silver, Standard
    credit_limit            FLOAT,
    payment_terms_days      INTEGER         DEFAULT 30,
    primary_region          VARCHAR(50),
    primary_contact         VARCHAR(100),
    annual_volume_mt        FLOAT,                         -- Historical annual volume
    customer_since          DATE,
    is_active               BOOLEAN         DEFAULT TRUE,
    
    PRIMARY KEY (customer_id)
);

COMMENT ON TABLE ATOMIC.CUSTOMER IS 'Customer master with segmentation and credit info';

-- =============================================================================
-- SITE: Manufacturing and distribution sites
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.SITE (
    site_id                 VARCHAR(50)     NOT NULL,
    site_name               VARCHAR(100)    NOT NULL,
    site_type               VARCHAR(50),                   -- MANUFACTURING, DISTRIBUTION, TERMINAL
    region                  VARCHAR(50),
    country                 VARCHAR(50),
    city                    VARCHAR(100),
    latitude                FLOAT,
    longitude               FLOAT,
    capacity_mt_year        FLOAT,                         -- Annual capacity
    utilization_target_pct  FLOAT           DEFAULT 85,
    storage_capacity_mt     FLOAT,
    is_active               BOOLEAN         DEFAULT TRUE,
    
    PRIMARY KEY (site_id)
);

COMMENT ON TABLE ATOMIC.SITE IS 'Manufacturing plants and distribution sites';

-- =============================================================================
-- PRODUCTION_RUN: Batch/production run data
-- DRD Reference: Section 3.3 - ATOMIC.PRODUCTION_RUN
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.PRODUCTION_RUN (
    run_id                  VARCHAR(50)     NOT NULL,
    site_id                 VARCHAR(50)     NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    start_time              TIMESTAMP_NTZ   NOT NULL,
    end_time                TIMESTAMP_NTZ,
    planned_quantity_mt     FLOAT,
    actual_yield_mt         FLOAT,                         -- Actual production
    yield_pct               FLOAT,                         -- Yield percentage
    feedstock_consumed_mt   FLOAT,
    feedstock_cost          FLOAT,
    energy_cost             FLOAT,
    labor_cost              FLOAT,
    overhead_cost           FLOAT,
    total_variable_cost     FLOAT,
    total_fixed_cost        FLOAT,
    cost_per_mt             FLOAT,                         -- Calculated cost per MT
    quality_grade           VARCHAR(50),
    batch_status            VARCHAR(50),                   -- PLANNED, IN_PROGRESS, COMPLETED, QC_HOLD
    carbon_footprint_kg     FLOAT,                         -- CO2 equivalent per batch
    
    PRIMARY KEY (run_id)
);

COMMENT ON TABLE ATOMIC.PRODUCTION_RUN IS 'Production batch data with yields and costs (DRD requirement)';

-- =============================================================================
-- INVENTORY_BALANCE: Inventory levels by product and site
-- DRD Reference: Section 3.3 - ATOMIC.INVENTORY_BALANCE
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.INVENTORY_BALANCE (
    inventory_balance_id    INTEGER         NOT NULL,
    snapshot_date           DATE            NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    site_id                 VARCHAR(50)     NOT NULL,
    quantity_mt             FLOAT           NOT NULL,
    quantity_allocated_mt   FLOAT           DEFAULT 0,     -- Allocated to orders
    quantity_available_mt   FLOAT,                         -- Available for sale
    inventory_value         FLOAT,                         -- Value at standard cost
    days_of_supply          FLOAT,                         -- Based on avg daily demand
    reorder_point_mt        FLOAT,
    safety_stock_mt         FLOAT,
    storage_cost_daily      FLOAT,
    inventory_age_days      INTEGER,
    quality_status          VARCHAR(50)     DEFAULT 'RELEASED',
    
    PRIMARY KEY (inventory_balance_id)
);

COMMENT ON TABLE ATOMIC.INVENTORY_BALANCE IS 'Daily inventory snapshots by product and site';

-- =============================================================================
-- SALES_ORDER: Cleaned/transformed sales orders
-- =============================================================================
CREATE TABLE IF NOT EXISTS ATOMIC.SALES_ORDER (
    order_id                VARCHAR(50)     NOT NULL,
    order_line_id           INTEGER         NOT NULL,
    order_date              DATE            NOT NULL,
    ship_date               DATE,
    customer_id             VARCHAR(50)     NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    site_id                 VARCHAR(50),                   -- Ship from site
    quantity_mt             FLOAT           NOT NULL,
    unit_price              FLOAT           NOT NULL,
    total_revenue           FLOAT,
    discount_amount         FLOAT           DEFAULT 0,
    rebate_amount           FLOAT           DEFAULT 0,
    freight_cost            FLOAT           DEFAULT 0,
    net_revenue             FLOAT,                         -- Revenue after discounts
    sales_region            VARCHAR(50),
    sales_channel           VARCHAR(50),
    contract_id             VARCHAR(50),
    order_status            VARCHAR(50),
    
    PRIMARY KEY (order_id, order_line_id)
);

COMMENT ON TABLE ATOMIC.SALES_ORDER IS 'Transformed sales orders with calculated fields';

-- =============================================================================
-- Foreign Key Constraints (optional - uncomment if needed)
-- =============================================================================
-- ALTER TABLE ATOMIC.PRODUCTION_RUN ADD CONSTRAINT fk_run_site 
--     FOREIGN KEY (site_id) REFERENCES ATOMIC.SITE(site_id);
-- ALTER TABLE ATOMIC.PRODUCTION_RUN ADD CONSTRAINT fk_run_product 
--     FOREIGN KEY (product_id) REFERENCES ATOMIC.PRODUCT(product_id);
-- ALTER TABLE ATOMIC.INVENTORY_BALANCE ADD CONSTRAINT fk_inv_product 
--     FOREIGN KEY (product_id) REFERENCES ATOMIC.PRODUCT(product_id);
-- ALTER TABLE ATOMIC.INVENTORY_BALANCE ADD CONSTRAINT fk_inv_site 
--     FOREIGN KEY (site_id) REFERENCES ATOMIC.SITE(site_id);
-- ALTER TABLE ATOMIC.SALES_ORDER ADD CONSTRAINT fk_order_customer 
--     FOREIGN KEY (customer_id) REFERENCES ATOMIC.CUSTOMER(customer_id);
-- ALTER TABLE ATOMIC.SALES_ORDER ADD CONSTRAINT fk_order_product 
--     FOREIGN KEY (product_id) REFERENCES ATOMIC.PRODUCT(product_id);

-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN SCHEMA ATOMIC;
