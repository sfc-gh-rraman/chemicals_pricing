-- =============================================================================
-- Data Loading Script: load_all_data.sql
-- Demo: Chemicals Integrated Pricing & Supply Chain Optimization
-- Description: Load synthetic data from CSV files into Snowflake tables
-- Prerequisites: Run DDL scripts first, upload CSV files to stage
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- Step 1: Create file formats
-- =============================================================================
CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE;

-- =============================================================================
-- Step 2: Load ATOMIC layer tables
-- =============================================================================

-- Products
COPY INTO ATOMIC.PRODUCT (
    product_id, product_name, cas_number, product_family, product_category,
    product_grade, molecular_weight, density_kg_m3, hazard_class, un_number,
    base_unit, standard_cost_per_mt, target_margin_pct, min_order_quantity_mt,
    lead_time_days, is_active
)
FROM @RAW.DATA_STAGE/products.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Customers
COPY INTO ATOMIC.CUSTOMER (
    customer_id, customer_name, customer_segment, customer_tier, credit_limit,
    payment_terms_days, primary_region, primary_contact, annual_volume_mt,
    customer_since, is_active
)
FROM @RAW.DATA_STAGE/customers.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Sites
COPY INTO ATOMIC.SITE (
    site_id, site_name, site_type, region, country, city, latitude, longitude,
    capacity_mt_year, utilization_target_pct, storage_capacity_mt, is_active
)
FROM @RAW.DATA_STAGE/sites.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Market Index (extension)
COPY INTO ATOMIC.MARKET_INDEX (
    index_id, index_name, index_code, index_date, price_value, currency, unit,
    price_type, region, source, price_change_1d, price_change_7d, price_change_30d,
    price_change_pct_1d, price_change_pct_7d, price_change_pct_30d,
    price_avg_7d, price_avg_30d, price_volatility_30d
)
FROM @RAW.DATA_STAGE/market_indices.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Production Runs
COPY INTO ATOMIC.PRODUCTION_RUN (
    run_id, site_id, product_id, start_time, end_time, planned_quantity_mt,
    actual_yield_mt, yield_pct, feedstock_consumed_mt, feedstock_cost,
    energy_cost, labor_cost, overhead_cost, total_variable_cost, total_fixed_cost,
    cost_per_mt, quality_grade, batch_status, carbon_footprint_kg
)
FROM @RAW.DATA_STAGE/production_runs.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Inventory Balances
COPY INTO ATOMIC.INVENTORY_BALANCE (
    inventory_balance_id, snapshot_date, product_id, site_id, quantity_mt,
    quantity_allocated_mt, quantity_available_mt, inventory_value, days_of_supply,
    reorder_point_mt, safety_stock_mt, storage_cost_daily, inventory_age_days,
    quality_status
)
FROM @RAW.DATA_STAGE/inventory_balances.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Contract Terms
COPY INTO ATOMIC.CONTRACT_TERM (
    term_id, contract_id, customer_id, product_id, effective_date, expiration_date,
    price_formula, base_index_code, index_adjustment, fixed_price, floor_price,
    ceiling_price, min_volume_mt, max_volume_mt, volume_tier_1_mt, volume_tier_1_discount,
    volume_tier_2_mt, volume_tier_2_discount, payment_terms_days, early_pay_discount_pct,
    contract_status, contract_type
)
FROM @RAW.DATA_STAGE/contract_terms.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Feedstock Mapping
COPY INTO ATOMIC.FEEDSTOCK_MAPPING (
    mapping_id, product_id, feedstock_index_code, conversion_factor,
    cost_weight_pct, effective_date
)
FROM @RAW.DATA_STAGE/feedstock_mapping.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Logistics Costs
COPY INTO ATOMIC.LOGISTICS_COST (
    cost_id, origin_site_id, destination_region, transport_mode, cost_per_mt,
    transit_days, effective_date, expiration_date, carrier
)
FROM @RAW.DATA_STAGE/logistics_costs.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Demand History
COPY INTO ATOMIC.DEMAND_HISTORY (
    demand_id, demand_date, product_id, region, customer_segment, volume_mt,
    order_count, customer_count, avg_price_per_mt, min_price_per_mt,
    max_price_per_mt, crude_oil_price, gdp_index, manufacturing_pmi
)
FROM @RAW.DATA_STAGE/demand_history.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- =============================================================================
-- Step 3: Load RAW layer tables
-- =============================================================================

-- Sales Orders (to RAW first, then transform to ATOMIC)
COPY INTO RAW.ERP_SALES_ORDERS (
    order_id, order_line_id, order_date, ship_date, customer_id, customer_name,
    product_id, product_name, quantity_mt, unit_price, total_amount, currency,
    sales_region, sales_channel, customer_segment, contract_id, discount_pct,
    rebate_amount, freight_cost, ship_from_site, ship_to_location, incoterms
)
FROM @RAW.DATA_STAGE/sales_orders.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Market Reports (for Cortex Search)
COPY INTO RAW.MARKET_REPORTS (
    report_id, report_date, report_type, report_title, report_source,
    chemical_name, region, report_summary, report_content, file_path, author
)
FROM @RAW.DATA_STAGE/market_reports.csv
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- =============================================================================
-- Step 4: Transform RAW sales orders to ATOMIC
-- =============================================================================
INSERT INTO ATOMIC.SALES_ORDER (
    order_id, order_line_id, order_date, ship_date, customer_id, product_id,
    site_id, quantity_mt, unit_price, total_revenue, discount_amount,
    rebate_amount, freight_cost, net_revenue, sales_region, sales_channel,
    contract_id, order_status
)
SELECT
    order_id,
    order_line_id,
    TO_DATE(order_date),
    TO_DATE(ship_date),
    customer_id,
    product_id,
    ship_from_site,
    quantity_mt,
    unit_price,
    total_amount,
    total_amount * discount_pct / 100,
    rebate_amount,
    freight_cost,
    total_amount - (total_amount * discount_pct / 100) - rebate_amount,
    sales_region,
    sales_channel,
    contract_id,
    'COMPLETED'
FROM RAW.ERP_SALES_ORDERS;

-- =============================================================================
-- Step 5: Verification
-- =============================================================================
SELECT 'ATOMIC.PRODUCT' as table_name, COUNT(*) as row_count FROM ATOMIC.PRODUCT
UNION ALL
SELECT 'ATOMIC.CUSTOMER', COUNT(*) FROM ATOMIC.CUSTOMER
UNION ALL
SELECT 'ATOMIC.SITE', COUNT(*) FROM ATOMIC.SITE
UNION ALL
SELECT 'ATOMIC.MARKET_INDEX', COUNT(*) FROM ATOMIC.MARKET_INDEX
UNION ALL
SELECT 'ATOMIC.PRODUCTION_RUN', COUNT(*) FROM ATOMIC.PRODUCTION_RUN
UNION ALL
SELECT 'ATOMIC.INVENTORY_BALANCE', COUNT(*) FROM ATOMIC.INVENTORY_BALANCE
UNION ALL
SELECT 'ATOMIC.CONTRACT_TERM', COUNT(*) FROM ATOMIC.CONTRACT_TERM
UNION ALL
SELECT 'ATOMIC.SALES_ORDER', COUNT(*) FROM ATOMIC.SALES_ORDER
UNION ALL
SELECT 'ATOMIC.FEEDSTOCK_MAPPING', COUNT(*) FROM ATOMIC.FEEDSTOCK_MAPPING
UNION ALL
SELECT 'ATOMIC.LOGISTICS_COST', COUNT(*) FROM ATOMIC.LOGISTICS_COST
UNION ALL
SELECT 'ATOMIC.DEMAND_HISTORY', COUNT(*) FROM ATOMIC.DEMAND_HISTORY
UNION ALL
SELECT 'RAW.MARKET_REPORTS', COUNT(*) FROM RAW.MARKET_REPORTS
ORDER BY table_name;

SELECT 'Data loading complete!' as status;
