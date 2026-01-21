-- =============================================================================
-- DDL Script: 006_ML_PREDICTIONS.sql
-- Demo: Chemicals Integrated Pricing & Supply Chain Optimization
-- Description: Create tables for ML prediction outputs
-- Dependencies: 005_CHEMICAL_OPS_MART.sql
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- DEMAND_FORECAST: ML prediction of sales volume
-- DRD Reference: Section 3.4 - CHEMICAL_OPS.DEMAND_FORECAST
-- =============================================================================
CREATE TABLE IF NOT EXISTS CHEMICAL_OPS.DEMAND_FORECAST (
    forecast_id             INTEGER         NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    region                  VARCHAR(50)     NOT NULL,
    customer_segment        VARCHAR(50),
    
    -- Forecast period
    forecast_date           DATE            NOT NULL,      -- Date forecast was generated
    forecast_period_start   DATE            NOT NULL,      -- Start of forecast period
    forecast_period_end     DATE            NOT NULL,      -- End of forecast period
    forecast_horizon        VARCHAR(20),                   -- WEEKLY, MONTHLY, QUARTERLY
    
    -- Forecast values
    predicted_volume_mt     FLOAT           NOT NULL,
    prediction_lower_bound  FLOAT,                         -- 95% CI lower
    prediction_upper_bound  FLOAT,                         -- 95% CI upper
    prediction_confidence   FLOAT,                         -- Model confidence score
    
    -- Comparison to history
    historical_avg_volume   FLOAT,
    forecast_vs_history_pct FLOAT,                         -- % change vs historical
    
    -- Key drivers (feature importance)
    top_driver_1            VARCHAR(100),                  -- e.g., 'CRUDE_OIL_PRICE'
    top_driver_1_impact     FLOAT,                         -- Contribution to forecast
    top_driver_2            VARCHAR(100),
    top_driver_2_impact     FLOAT,
    top_driver_3            VARCHAR(100),
    top_driver_3_impact     FLOAT,
    
    -- Model metadata
    model_name              VARCHAR(100)    DEFAULT 'DEMAND_XGBOOST',
    model_version           VARCHAR(50)     DEFAULT 'v1.0',
    model_mape              FLOAT,                         -- Mean Absolute Percentage Error
    
    PRIMARY KEY (forecast_id)
);

COMMENT ON TABLE CHEMICAL_OPS.DEMAND_FORECAST IS 'ML demand predictions by product/region (DRD Section 3.4)';

-- =============================================================================
-- OPTIMAL_PRICING: Recommended price points from optimization
-- DRD Reference: Section 3.4 - CHEMICAL_OPS.OPTIMAL_PRICING
-- =============================================================================
CREATE TABLE IF NOT EXISTS CHEMICAL_OPS.OPTIMAL_PRICING (
    pricing_id              INTEGER         NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    region                  VARCHAR(50)     NOT NULL,
    customer_segment        VARCHAR(50),
    
    -- Timing
    recommendation_date     DATE            NOT NULL,
    valid_from              DATE,
    valid_to                DATE,
    
    -- Cost inputs
    current_cost_per_mt     FLOAT           NOT NULL,
    feedstock_cost_per_mt   FLOAT,
    conversion_cost_per_mt  FLOAT,
    freight_cost_per_mt     FLOAT,
    
    -- Price recommendations
    floor_price_per_mt      FLOAT           NOT NULL,      -- Minimum price (cost + min margin)
    optimal_price_per_mt    FLOAT           NOT NULL,      -- Profit-maximizing price
    ceiling_price_per_mt    FLOAT,                         -- Maximum market price
    current_market_price    FLOAT,                         -- Competitor/market reference
    
    -- Optimization outputs
    predicted_volume_at_optimal FLOAT,                     -- Volume at optimal price
    predicted_margin_at_optimal FLOAT,                     -- Margin at optimal price
    predicted_revenue_at_optimal FLOAT,
    
    -- Elasticity
    price_elasticity        FLOAT,                         -- Demand elasticity coefficient
    elasticity_confidence   FLOAT,
    
    -- Scenario analysis
    price_scenario_low      FLOAT,                         -- Conservative price
    margin_at_low           FLOAT,
    volume_at_low           FLOAT,
    price_scenario_high     FLOAT,                         -- Aggressive price
    margin_at_high          FLOAT,
    volume_at_high          FLOAT,
    
    -- Market context
    feedstock_trend         VARCHAR(20),                   -- RISING, STABLE, FALLING
    competitive_position    VARCHAR(20),                   -- ABOVE, AT, BELOW market
    
    -- Model metadata
    model_name              VARCHAR(100)    DEFAULT 'PRICE_OPTIMIZER',
    model_version           VARCHAR(50)     DEFAULT 'v1.0',
    optimization_method     VARCHAR(50)     DEFAULT 'SCIPY_MINIMIZE',
    
    PRIMARY KEY (pricing_id)
);

COMMENT ON TABLE CHEMICAL_OPS.OPTIMAL_PRICING IS 'Optimal price recommendations from ML optimization (DRD Section 3.4)';

-- =============================================================================
-- PRICE_ELASTICITY: Calculated elasticity coefficients
-- DRD Reference: Section 4.3 - Price Elasticity calculation
-- =============================================================================
CREATE TABLE IF NOT EXISTS CHEMICAL_OPS.PRICE_ELASTICITY (
    elasticity_id           INTEGER         NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    region                  VARCHAR(50)     NOT NULL,
    customer_segment        VARCHAR(50),
    
    -- Calculation period
    calculation_date        DATE            NOT NULL,
    period_start            DATE,
    period_end              DATE,
    data_points             INTEGER,                       -- Number of observations
    
    -- Elasticity metrics
    elasticity_coefficient  FLOAT           NOT NULL,      -- Price elasticity of demand
    elasticity_std_error    FLOAT,
    elasticity_p_value      FLOAT,
    r_squared               FLOAT,                         -- Model fit
    
    -- Interpretation
    elasticity_category     VARCHAR(20),                   -- ELASTIC, UNIT_ELASTIC, INELASTIC
    
    -- Cross-price elasticity (optional)
    substitute_product_id   VARCHAR(50),
    cross_elasticity        FLOAT,
    
    -- Model metadata
    model_name              VARCHAR(100)    DEFAULT 'ELASTICITY_LINEAR_REGRESSION',
    model_version           VARCHAR(50)     DEFAULT 'v1.0',
    
    PRIMARY KEY (elasticity_id)
);

COMMENT ON TABLE CHEMICAL_OPS.PRICE_ELASTICITY IS 'Price elasticity coefficients by product/region (DRD Section 4.3)';

-- =============================================================================
-- LAGGED_CORRELATION: Hidden discovery - demand vs crude price lag
-- DRD Reference: Hidden Discovery - 3-week lagged correlation
-- =============================================================================
CREATE TABLE IF NOT EXISTS CHEMICAL_OPS.LAGGED_CORRELATION (
    correlation_id          INTEGER         NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    region                  VARCHAR(50)     NOT NULL,
    
    -- Correlation analysis
    analysis_date           DATE            NOT NULL,
    demand_metric           VARCHAR(50),                   -- e.g., 'WEEKLY_VOLUME'
    driver_metric           VARCHAR(50),                   -- e.g., 'CRUDE_OIL_BRENT'
    
    -- Lag analysis (1-8 weeks)
    lag_weeks               INTEGER         NOT NULL,
    correlation_coefficient FLOAT           NOT NULL,
    correlation_p_value     FLOAT,
    is_significant          BOOLEAN,                       -- p < 0.05
    
    -- Best lag identification
    is_optimal_lag          BOOLEAN         DEFAULT FALSE,
    
    -- Business impact
    predicted_demand_change_pct FLOAT,                     -- If driver changes by 10%
    inventory_positioning_days  INTEGER,                   -- Lead time for inventory
    
    PRIMARY KEY (correlation_id)
);

COMMENT ON TABLE CHEMICAL_OPS.LAGGED_CORRELATION IS 'Lagged correlation analysis - hidden demand drivers (DRD Hidden Discovery)';

-- =============================================================================
-- MARGIN_AT_RISK: Risk analysis for pricing decisions
-- =============================================================================
CREATE TABLE IF NOT EXISTS CHEMICAL_OPS.MARGIN_AT_RISK (
    risk_id                 INTEGER         NOT NULL,
    analysis_date           DATE            NOT NULL,
    product_id              VARCHAR(50)     NOT NULL,
    region                  VARCHAR(50),
    
    -- Current state
    current_margin_pct      FLOAT,
    current_volume_mt       FLOAT,
    current_revenue         FLOAT,
    
    -- Risk scenarios
    feedstock_spike_pct     FLOAT           DEFAULT 10,    -- Assumed feedstock increase
    margin_at_risk          FLOAT,                         -- $ margin lost if spike occurs
    margin_pct_at_risk      FLOAT,                         -- % margin compression
    
    -- Pass-through analysis
    price_increase_needed   FLOAT,                         -- To maintain margin
    volume_loss_if_passed   FLOAT,                         -- Expected volume loss
    net_impact              FLOAT,                         -- Net margin impact
    
    -- Probability
    spike_probability_30d   FLOAT,                         -- P(spike) in 30 days
    expected_loss           FLOAT,                         -- Probability-weighted loss
    
    PRIMARY KEY (risk_id)
);

COMMENT ON TABLE CHEMICAL_OPS.MARGIN_AT_RISK IS 'Margin at risk analysis under feedstock scenarios';

-- =============================================================================
-- Create indexes for performance
-- =============================================================================
-- CREATE INDEX idx_demand_forecast_product ON CHEMICAL_OPS.DEMAND_FORECAST(product_id, region, forecast_period_start);
-- CREATE INDEX idx_optimal_pricing_product ON CHEMICAL_OPS.OPTIMAL_PRICING(product_id, region, recommendation_date);
-- CREATE INDEX idx_lagged_correlation_product ON CHEMICAL_OPS.LAGGED_CORRELATION(product_id, region, lag_weeks);

-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN SCHEMA CHEMICAL_OPS;
