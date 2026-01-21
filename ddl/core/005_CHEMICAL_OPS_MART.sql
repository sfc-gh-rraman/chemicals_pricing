-- =============================================================================
-- DDL Script: 005_CHEMICAL_OPS_MART.sql
-- Demo: Chemicals Integrated Pricing & Supply Chain Optimization
-- Description: Create CHEMICAL_OPS data mart views
-- Dependencies: 003_ATOMIC_CORE.sql, 004_ATOMIC_EXTENSIONS.sql
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- MARGIN_ANALYZER: Pre-computed profitability per transaction
-- DRD Reference: Section 3.4 - CHEMICAL_OPS.MARGIN_ANALYZER
-- =============================================================================
CREATE OR REPLACE VIEW CHEMICAL_OPS.MARGIN_ANALYZER AS
SELECT
    so.order_id,
    so.order_line_id,
    so.order_date,
    DATE_TRUNC('month', so.order_date) AS order_month,
    DATE_TRUNC('quarter', so.order_date) AS order_quarter,
    YEAR(so.order_date) AS order_year,
    
    -- Customer dimensions
    so.customer_id,
    c.customer_name,
    c.customer_segment,
    c.customer_tier,
    
    -- Product dimensions
    so.product_id,
    p.product_name,
    p.product_family,
    p.product_category,
    p.product_grade,
    
    -- Geography
    so.sales_region AS region,
    so.sales_channel,
    so.site_id AS ship_from_site,
    
    -- Volume
    so.quantity_mt AS sales_volume_mt,
    
    -- Revenue breakdown
    so.unit_price AS selling_price_per_mt,
    so.total_revenue AS gross_revenue,
    so.discount_amount,
    so.rebate_amount,
    so.freight_cost,
    so.net_revenue,
    
    -- Cost breakdown (from most recent production run)
    pr.cost_per_mt AS manufacturing_cost_per_mt,
    pr.feedstock_cost / NULLIF(pr.actual_yield_mt, 0) AS feedstock_cost_per_mt,
    pr.energy_cost / NULLIF(pr.actual_yield_mt, 0) AS energy_cost_per_mt,
    (pr.labor_cost + pr.overhead_cost) / NULLIF(pr.actual_yield_mt, 0) AS conversion_cost_per_mt,
    
    -- Total cost
    COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt AS total_product_cost,
    so.freight_cost AS total_freight_cost,
    (COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt) + so.freight_cost AS total_cost,
    
    -- Margin calculations
    so.net_revenue - ((COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt) + so.freight_cost) AS gross_margin,
    CASE 
        WHEN so.net_revenue > 0 
        THEN ((so.net_revenue - ((COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt) + so.freight_cost)) / so.net_revenue) * 100 
        ELSE 0 
    END AS gross_margin_pct,
    
    -- Margin vs target
    p.target_margin_pct,
    CASE 
        WHEN so.net_revenue > 0 
        THEN ((so.net_revenue - ((COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt) + so.freight_cost)) / so.net_revenue) * 100 - p.target_margin_pct
        ELSE -p.target_margin_pct 
    END AS margin_variance_to_target,
    
    -- Deal classification
    CASE 
        WHEN so.net_revenue < ((COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt) + so.freight_cost) THEN 'LOSS'
        WHEN ((so.net_revenue - ((COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt) + so.freight_cost)) / NULLIF(so.net_revenue, 0)) * 100 < p.target_margin_pct * 0.5 THEN 'UNDERPRICED'
        WHEN ((so.net_revenue - ((COALESCE(pr.cost_per_mt, p.standard_cost_per_mt) * so.quantity_mt) + so.freight_cost)) / NULLIF(so.net_revenue, 0)) * 100 < p.target_margin_pct THEN 'BELOW_TARGET'
        ELSE 'ON_TARGET'
    END AS margin_status
    
FROM ATOMIC.SALES_ORDER so
LEFT JOIN ATOMIC.CUSTOMER c ON so.customer_id = c.customer_id
LEFT JOIN ATOMIC.PRODUCT p ON so.product_id = p.product_id
LEFT JOIN (
    -- Get most recent production cost for each product
    SELECT * FROM ATOMIC.PRODUCTION_RUN
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY end_time DESC) = 1
) pr ON so.product_id = pr.product_id;

COMMENT ON VIEW CHEMICAL_OPS.MARGIN_ANALYZER IS 'Pre-computed profitability per transaction (DRD Section 3.4)';

-- =============================================================================
-- COST_TO_SERVE: Real-time cost breakdown for pricing decisions
-- DRD Reference: "Wow Moment" - calculate real-time marginal cost
-- =============================================================================
CREATE OR REPLACE VIEW CHEMICAL_OPS.COST_TO_SERVE AS
SELECT
    p.product_id,
    p.product_name,
    p.product_family,
    p.product_category,
    s.site_id,
    s.site_name,
    s.region AS manufacturing_region,
    
    -- Current feedstock cost (from latest market index)
    fm.feedstock_index_code,
    mi.price_value AS current_feedstock_price,
    mi.price_change_pct_7d AS feedstock_7d_change_pct,
    mi.price_value * fm.conversion_factor AS feedstock_cost_per_mt,
    
    -- Manufacturing costs (from latest production)
    pr.energy_cost / NULLIF(pr.actual_yield_mt, 0) AS energy_cost_per_mt,
    (pr.labor_cost + pr.overhead_cost) / NULLIF(pr.actual_yield_mt, 0) AS conversion_cost_per_mt,
    
    -- Total manufacturing cost
    (mi.price_value * fm.conversion_factor) + 
    (pr.energy_cost / NULLIF(pr.actual_yield_mt, 0)) + 
    ((pr.labor_cost + pr.overhead_cost) / NULLIF(pr.actual_yield_mt, 0)) AS total_manufacturing_cost_per_mt,
    
    -- Logistics costs by destination
    lc.destination_region,
    lc.cost_per_mt AS freight_cost_per_mt,
    lc.transit_days,
    
    -- Total cost to serve
    (mi.price_value * fm.conversion_factor) + 
    (pr.energy_cost / NULLIF(pr.actual_yield_mt, 0)) + 
    ((pr.labor_cost + pr.overhead_cost) / NULLIF(pr.actual_yield_mt, 0)) +
    lc.cost_per_mt AS total_cost_to_serve_per_mt,
    
    -- Suggested floor price (cost + min margin)
    ((mi.price_value * fm.conversion_factor) + 
    (pr.energy_cost / NULLIF(pr.actual_yield_mt, 0)) + 
    ((pr.labor_cost + pr.overhead_cost) / NULLIF(pr.actual_yield_mt, 0)) +
    lc.cost_per_mt) * (1 + 0.05) AS floor_price_per_mt,  -- 5% minimum margin
    
    -- Suggested target price (cost + target margin)
    ((mi.price_value * fm.conversion_factor) + 
    (pr.energy_cost / NULLIF(pr.actual_yield_mt, 0)) + 
    ((pr.labor_cost + pr.overhead_cost) / NULLIF(pr.actual_yield_mt, 0)) +
    lc.cost_per_mt) * (1 + p.target_margin_pct/100) AS target_price_per_mt,
    
    -- Data freshness
    mi.index_date AS feedstock_price_date,
    pr.end_time AS production_cost_date
    
FROM ATOMIC.PRODUCT p
CROSS JOIN ATOMIC.SITE s
LEFT JOIN ATOMIC.FEEDSTOCK_MAPPING fm ON p.product_id = fm.product_id
LEFT JOIN (
    -- Latest feedstock prices
    SELECT * FROM ATOMIC.MARKET_INDEX
    QUALIFY ROW_NUMBER() OVER (PARTITION BY index_code ORDER BY index_date DESC) = 1
) mi ON fm.feedstock_index_code = mi.index_code
LEFT JOIN (
    -- Latest production costs
    SELECT * FROM ATOMIC.PRODUCTION_RUN
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, site_id ORDER BY end_time DESC) = 1
) pr ON p.product_id = pr.product_id AND s.site_id = pr.site_id
LEFT JOIN ATOMIC.LOGISTICS_COST lc ON s.site_id = lc.origin_site_id
WHERE s.site_type = 'MANUFACTURING' AND p.is_active = TRUE;

COMMENT ON VIEW CHEMICAL_OPS.COST_TO_SERVE IS 'Real-time cost-to-serve calculation for pricing decisions';

-- =============================================================================
-- PRICE_GUIDANCE: Pricing recommendations with market context
-- =============================================================================
CREATE OR REPLACE VIEW CHEMICAL_OPS.PRICE_GUIDANCE AS
SELECT
    cts.product_id,
    cts.product_name,
    cts.product_family,
    cts.site_id,
    cts.destination_region,
    
    -- Cost components
    cts.feedstock_cost_per_mt,
    cts.energy_cost_per_mt,
    cts.conversion_cost_per_mt,
    cts.freight_cost_per_mt,
    cts.total_cost_to_serve_per_mt,
    
    -- Price recommendations
    cts.floor_price_per_mt,
    cts.target_price_per_mt,
    
    -- Market context
    cts.feedstock_7d_change_pct,
    CASE 
        WHEN cts.feedstock_7d_change_pct > 5 THEN 'RISING'
        WHEN cts.feedstock_7d_change_pct < -5 THEN 'FALLING'
        ELSE 'STABLE'
    END AS feedstock_trend,
    
    -- Historical pricing (from recent sales)
    hist.avg_selling_price_30d,
    hist.min_selling_price_30d,
    hist.max_selling_price_30d,
    hist.sales_volume_30d_mt,
    
    -- Price drivers narrative
    CASE 
        WHEN cts.feedstock_7d_change_pct > 5 THEN 'Feedstock prices up ' || ROUND(cts.feedstock_7d_change_pct, 1) || '% - consider price increase'
        WHEN cts.feedstock_7d_change_pct < -5 THEN 'Feedstock prices down ' || ROUND(ABS(cts.feedstock_7d_change_pct), 1) || '% - competitive pricing opportunity'
        ELSE 'Feedstock prices stable - maintain current pricing'
    END AS pricing_recommendation
    
FROM CHEMICAL_OPS.COST_TO_SERVE cts
LEFT JOIN (
    -- 30-day sales history by product/region
    SELECT 
        product_id,
        region,
        AVG(selling_price_per_mt) AS avg_selling_price_30d,
        MIN(selling_price_per_mt) AS min_selling_price_30d,
        MAX(selling_price_per_mt) AS max_selling_price_30d,
        SUM(sales_volume_mt) AS sales_volume_30d_mt
    FROM CHEMICAL_OPS.MARGIN_ANALYZER
    WHERE order_date >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY product_id, region
) hist ON cts.product_id = hist.product_id AND cts.destination_region = hist.region;

COMMENT ON VIEW CHEMICAL_OPS.PRICE_GUIDANCE IS 'Pricing recommendations with cost drivers and market context';

-- =============================================================================
-- INVENTORY_HEALTH: Inventory analysis with days of supply
-- =============================================================================
CREATE OR REPLACE VIEW CHEMICAL_OPS.INVENTORY_HEALTH AS
SELECT
    ib.snapshot_date,
    ib.product_id,
    p.product_name,
    p.product_family,
    ib.site_id,
    s.site_name,
    s.region,
    
    -- Inventory levels
    ib.quantity_mt AS total_inventory_mt,
    ib.quantity_allocated_mt,
    ib.quantity_available_mt,
    ib.inventory_value,
    
    -- Days of supply
    ib.days_of_supply,
    ib.reorder_point_mt,
    ib.safety_stock_mt,
    
    -- Status classification
    CASE 
        WHEN ib.quantity_available_mt <= ib.safety_stock_mt THEN 'CRITICAL'
        WHEN ib.quantity_available_mt <= ib.reorder_point_mt THEN 'LOW'
        WHEN ib.days_of_supply > 60 THEN 'EXCESS'
        ELSE 'HEALTHY'
    END AS inventory_status,
    
    -- Working capital impact
    ib.inventory_value AS working_capital_tied,
    ib.storage_cost_daily * ib.days_of_supply AS storage_cost_exposure,
    
    -- Aging
    ib.inventory_age_days,
    CASE 
        WHEN ib.inventory_age_days > 90 THEN 'AGED'
        WHEN ib.inventory_age_days > 60 THEN 'MATURING'
        ELSE 'FRESH'
    END AS age_status

FROM ATOMIC.INVENTORY_BALANCE ib
LEFT JOIN ATOMIC.PRODUCT p ON ib.product_id = p.product_id
LEFT JOIN ATOMIC.SITE s ON ib.site_id = s.site_id
WHERE ib.snapshot_date = (SELECT MAX(snapshot_date) FROM ATOMIC.INVENTORY_BALANCE);

COMMENT ON VIEW CHEMICAL_OPS.INVENTORY_HEALTH IS 'Inventory health analysis with days of supply and working capital';

-- =============================================================================
-- MARGIN_LEAKAGE: Analysis of margin leakage sources
-- DRD Reference: Page 1 - Leakage Waterfall
-- =============================================================================
CREATE OR REPLACE VIEW CHEMICAL_OPS.MARGIN_LEAKAGE AS
SELECT
    order_month,
    product_family,
    region,
    customer_segment,
    
    -- Revenue waterfall
    SUM(gross_revenue) AS gross_revenue,
    SUM(discount_amount) AS total_discounts,
    SUM(rebate_amount) AS total_rebates,
    SUM(freight_cost) AS total_freight,
    SUM(net_revenue) AS net_revenue,
    
    -- Cost waterfall  
    SUM(total_product_cost) AS total_product_cost,
    SUM(total_freight_cost) AS total_freight_cost,
    SUM(total_cost) AS total_cost,
    
    -- Margin
    SUM(gross_margin) AS total_margin,
    AVG(gross_margin_pct) AS avg_margin_pct,
    
    -- Leakage analysis
    SUM(discount_amount) / NULLIF(SUM(gross_revenue), 0) * 100 AS discount_leakage_pct,
    SUM(rebate_amount) / NULLIF(SUM(gross_revenue), 0) * 100 AS rebate_leakage_pct,
    SUM(freight_cost) / NULLIF(SUM(gross_revenue), 0) * 100 AS freight_leakage_pct,
    
    -- Deal quality
    COUNT(*) AS total_deals,
    SUM(CASE WHEN margin_status = 'LOSS' THEN 1 ELSE 0 END) AS loss_deals,
    SUM(CASE WHEN margin_status = 'UNDERPRICED' THEN 1 ELSE 0 END) AS underpriced_deals,
    SUM(CASE WHEN margin_status = 'ON_TARGET' THEN 1 ELSE 0 END) AS on_target_deals
    
FROM CHEMICAL_OPS.MARGIN_ANALYZER
GROUP BY order_month, product_family, region, customer_segment;

COMMENT ON VIEW CHEMICAL_OPS.MARGIN_LEAKAGE IS 'Margin leakage analysis by source (discounts, rebates, freight)';

-- =============================================================================
-- MARKET_TRENDS: Market index trends and analysis
-- =============================================================================
CREATE OR REPLACE VIEW CHEMICAL_OPS.MARKET_TRENDS AS
SELECT
    index_name,
    index_code,
    index_date,
    price_value AS current_price,
    currency,
    region,
    
    -- Trends
    price_change_1d,
    price_change_7d,
    price_change_30d,
    price_change_pct_1d,
    price_change_pct_7d,
    price_change_pct_30d,
    
    -- Moving averages
    price_avg_7d,
    price_avg_30d,
    price_volatility_30d,
    
    -- Trend classification
    CASE 
        WHEN price_change_pct_7d > 10 THEN 'SPIKE'
        WHEN price_change_pct_7d > 5 THEN 'RISING'
        WHEN price_change_pct_7d < -10 THEN 'CRASH'
        WHEN price_change_pct_7d < -5 THEN 'FALLING'
        ELSE 'STABLE'
    END AS price_trend,
    
    -- Volatility classification
    CASE 
        WHEN price_volatility_30d > price_avg_30d * 0.15 THEN 'HIGH'
        WHEN price_volatility_30d > price_avg_30d * 0.08 THEN 'MODERATE'
        ELSE 'LOW'
    END AS volatility_level

FROM ATOMIC.MARKET_INDEX
WHERE index_date >= DATEADD(day, -90, CURRENT_DATE())
ORDER BY index_name, index_date DESC;

COMMENT ON VIEW CHEMICAL_OPS.MARKET_TRENDS IS 'Market index trends and volatility analysis';

-- =============================================================================
-- Verification
-- =============================================================================
SHOW VIEWS IN SCHEMA CHEMICAL_OPS;
