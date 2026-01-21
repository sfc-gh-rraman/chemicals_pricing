-- =============================================================================
-- CORTEX ANALYST DEPLOYMENT
-- Chemicals Integrated Pricing & Supply Chain Optimization Demo
-- =============================================================================
-- DRD Reference: Section 4.1 - Cortex Analyst (Structured)
-- Semantic Model Scope:
--   Measures: Gross_Margin, Sales_Volume_MT, Avg_Selling_Price, Feedstock_Cost
--   Dimensions: Product_Family, Region, Customer_Segment, Sales_Channel
-- Golden Query: "Show me the average margin for 'Ethylene' products in 'Europe' vs 'North America' for Q3"
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- STEP 1: Create Stage for Semantic Model YAML
-- =============================================================================
CREATE STAGE IF NOT EXISTS SEMANTIC_MODELS
    COMMENT = 'Stage for Cortex Analyst semantic model YAML files';

-- Upload the YAML file to stage:
-- snow stage copy cortex/chemicals_semantic_model.yaml @SEMANTIC_MODELS/ --connection demo

-- =============================================================================
-- STEP 2: Deploy Semantic View from YAML
-- =============================================================================
-- The semantic view enables Cortex Analyst to understand natural language queries
-- about margins, pricing, demand forecasts, and market trends.

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'CHEMICALS_DB.CHEMICAL_OPS',
  $$
name: CHEMICALS_PRICING_MODEL

tables:
  - name: margins
    description: Sales transaction profitability analysis with full cost breakdown
    base_table:
      database: CHEMICALS_DB
      schema: CHEMICAL_OPS
      table: MARGIN_ANALYZER
    primary_key:
      columns: [order_id, order_line_id]
    dimensions:
      - name: order_id
        expr: order_id
        data_type: TEXT
      - name: product_name
        synonyms: ["product", "chemical"]
        description: Chemical product name
        expr: product_name
        data_type: TEXT
      - name: product_family
        synonyms: ["family", "product type"]
        description: Product family
        expr: product_family
        data_type: TEXT
        sample_values: ["Olefins", "Polymers", "Aromatics", "Intermediates"]
      - name: product_category
        description: Product category
        expr: product_category
        data_type: TEXT
      - name: customer_name
        synonyms: ["customer"]
        description: Customer name
        expr: customer_name
        data_type: TEXT
      - name: customer_segment
        synonyms: ["segment"]
        description: Customer segment
        expr: customer_segment
        data_type: TEXT
        sample_values: ["Industrial", "Consumer", "Agricultural", "Automotive"]
      - name: customer_tier
        synonyms: ["tier"]
        description: Customer tier
        expr: customer_tier
        data_type: TEXT
        sample_values: ["Platinum", "Gold", "Silver", "Standard"]
      - name: region
        synonyms: ["sales region", "geography", "market"]
        description: Sales region
        expr: region
        data_type: TEXT
        sample_values: ["North America", "Europe", "Asia Pacific", "Latin America"]
      - name: sales_channel
        synonyms: ["channel"]
        description: Sales channel
        expr: sales_channel
        data_type: TEXT
        sample_values: ["Direct", "Distributor", "Spot", "Contract"]
      - name: margin_status
        synonyms: ["deal status"]
        description: Margin classification
        expr: margin_status
        data_type: TEXT
        sample_values: ["LOSS", "UNDERPRICED", "BELOW_TARGET", "ON_TARGET"]
    time_dimensions:
      - name: order_date
        synonyms: ["date", "transaction date"]
        description: Order date
        expr: order_date
        data_type: DATE
      - name: order_month
        description: Order month
        expr: order_month
        data_type: DATE
      - name: order_quarter
        description: Order quarter
        expr: order_quarter
        data_type: DATE
    facts:
      - name: sales_volume_mt
        synonyms: ["volume", "quantity", "tons"]
        description: Sales volume in metric tons
        expr: sales_volume_mt
        data_type: NUMBER
      - name: selling_price_per_mt
        synonyms: ["price", "selling price", "ASP"]
        description: Selling price per metric ton
        expr: selling_price_per_mt
        data_type: NUMBER
      - name: net_revenue
        synonyms: ["revenue"]
        description: Net revenue
        expr: net_revenue
        data_type: NUMBER
      - name: feedstock_cost_per_mt
        synonyms: ["feedstock cost", "raw material cost"]
        description: Feedstock cost per MT
        expr: feedstock_cost_per_mt
        data_type: NUMBER
      - name: gross_margin
        synonyms: ["margin", "profit"]
        description: Gross margin in dollars
        expr: gross_margin
        data_type: NUMBER
      - name: gross_margin_pct
        synonyms: ["margin percent", "margin %"]
        description: Gross margin percentage
        expr: gross_margin_pct
        data_type: NUMBER
    metrics:
      - name: total_margin
        description: Total gross margin
        expr: SUM(margins.gross_margin)
      - name: avg_margin_pct
        description: Average margin percentage
        expr: AVG(margins.gross_margin_pct)
      - name: total_volume
        description: Total volume in MT
        expr: SUM(margins.sales_volume_mt)
      - name: total_revenue
        description: Total revenue
        expr: SUM(margins.net_revenue)

  - name: market_prices
    description: Market feedstock price indices and trends
    base_table:
      database: CHEMICALS_DB
      schema: CHEMICAL_OPS
      table: MARKET_TRENDS
    dimensions:
      - name: index_name
        synonyms: ["feedstock", "index", "commodity"]
        description: Market index name
        expr: index_name
        data_type: TEXT
      - name: index_code
        description: Index code
        expr: index_code
        data_type: TEXT
      - name: region
        description: Index region
        expr: region
        data_type: TEXT
      - name: price_trend
        synonyms: ["trend"]
        description: Price trend
        expr: price_trend
        data_type: TEXT
        sample_values: ["SPIKE", "RISING", "STABLE", "FALLING", "CRASH"]
    time_dimensions:
      - name: index_date
        synonyms: ["date"]
        description: Price date
        expr: index_date
        data_type: DATE
    facts:
      - name: current_price
        synonyms: ["price"]
        description: Current price
        expr: current_price
        data_type: NUMBER
      - name: price_change_pct_7d
        synonyms: ["weekly change"]
        description: 7-day price change %
        expr: price_change_pct_7d
        data_type: NUMBER
      - name: price_change_pct_30d
        synonyms: ["monthly change"]
        description: 30-day price change %
        expr: price_change_pct_30d
        data_type: NUMBER

  - name: inventory
    description: Inventory health and days of supply
    base_table:
      database: CHEMICALS_DB
      schema: CHEMICAL_OPS
      table: INVENTORY_HEALTH
    dimensions:
      - name: product_name
        synonyms: ["product"]
        description: Product name
        expr: product_name
        data_type: TEXT
      - name: site_name
        synonyms: ["site", "location"]
        description: Site name
        expr: site_name
        data_type: TEXT
      - name: region
        description: Region
        expr: region
        data_type: TEXT
      - name: inventory_status
        synonyms: ["status"]
        description: Inventory status
        expr: inventory_status
        data_type: TEXT
        sample_values: ["CRITICAL", "LOW", "HEALTHY", "EXCESS"]
    facts:
      - name: total_inventory_mt
        synonyms: ["inventory", "stock"]
        description: Total inventory MT
        expr: total_inventory_mt
        data_type: NUMBER
      - name: days_of_supply
        synonyms: ["DOS", "days supply"]
        description: Days of supply
        expr: days_of_supply
        data_type: NUMBER
      - name: inventory_value
        synonyms: ["value"]
        description: Inventory value
        expr: inventory_value
        data_type: NUMBER

verified_queries:
  - name: golden_query_margin_comparison
    question: "Show me the average margin for Ethylene products in Europe vs North America for Q3"
    use_as_onboarding_question: true
    sql: |
      SELECT region, AVG(gross_margin_pct) as avg_margin_pct, SUM(gross_margin) as total_margin
      FROM __margins
      WHERE product_family = 'Olefins' AND region IN ('Europe', 'North America') AND QUARTER(order_date) = 3
      GROUP BY region ORDER BY avg_margin_pct DESC

  - name: underpriced_deals_count
    question: "How many deals were underpriced?"
    use_as_onboarding_question: true
    sql: |
      SELECT margin_status, COUNT(*) as deal_count, SUM(gross_margin) as margin_impact
      FROM __margins GROUP BY margin_status
      ORDER BY CASE margin_status WHEN 'LOSS' THEN 1 WHEN 'UNDERPRICED' THEN 2 ELSE 3 END

  - name: feedstock_prices
    question: "What is the trend in Propylene prices?"
    use_as_onboarding_question: true
    sql: |
      SELECT index_name, current_price, price_change_pct_7d, price_trend
      FROM __market_prices WHERE index_name LIKE '%Propylene%'
      ORDER BY index_date DESC LIMIT 5

  - name: critical_inventory
    question: "Which products have low inventory?"
    use_as_onboarding_question: true
    sql: |
      SELECT product_name, site_name, days_of_supply, inventory_status
      FROM __inventory WHERE inventory_status IN ('CRITICAL', 'LOW')
      ORDER BY days_of_supply ASC

custom_instructions: >
  This model covers Chemicals Pricing & Supply Chain. Key measures: Gross_Margin, Sales_Volume_MT, Feedstock_Cost.
  Dimensions: Product_Family (Olefins, Polymers, Aromatics), Region, Customer_Segment.
  Margin status: LOSS, UNDERPRICED, BELOW_TARGET, ON_TARGET.
  Price trends: SPIKE, RISING, STABLE, FALLING, CRASH.
$$
);

-- =============================================================================
-- STEP 3: Verify Semantic View
-- =============================================================================
SHOW VIEWS LIKE 'CHEMICALS_PRICING_MODEL' IN SCHEMA CHEMICALS_DB.CHEMICAL_OPS;

SELECT 'Cortex Analyst semantic view deployed successfully!' AS status;

-- =============================================================================
-- STEP 4: Test Queries (via Snowsight or Cortex Analyst UI)
-- =============================================================================
-- Golden Query from DRD:
-- "Show me the average margin for 'Ethylene' products in 'Europe' vs 'North America' for Q3"

-- Navigate to: Snowsight > AI & ML > Cortex Analyst
-- Select the CHEMICALS_PRICING_MODEL semantic view
-- Ask natural language questions about margins, pricing, and inventory
