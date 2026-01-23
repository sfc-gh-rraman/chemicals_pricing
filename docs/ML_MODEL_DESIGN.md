# ML Model Design: Price Elasticity & Demand Sensing

## Overview

This document outlines the machine learning architecture for the Chemicals Pricing solution, focusing on econometrically rigorous price elasticity models that capture own-price and cross-price effects.

---

## 1. Business Objectives

### Primary Goals
1. **Quantify Price Sensitivity**: How does demand respond to price changes?
2. **Capture Substitution Patterns**: When we raise PE prices, how much demand shifts to PP?
3. **Integrate Market Conditions**: How do macro variables (CPI, industrial production) affect demand?
4. **Enable Optimal Pricing**: Provide elasticity inputs for the pricing optimization engine

### Key Questions to Answer
- "If we raise Polyethylene prices by 5%, what happens to volume?"
- "Are Polypropylene and PVC substitutes or independent?"
- "How should we adjust prices when feedstock costs rise?"
- "Which customer segments are most price-sensitive?"

---

## 2. Data Architecture

### 2.1 Internal Data Sources

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `ATOMIC.SALES_ORDER` | Transaction history | product_id, customer_id, quantity, price, date |
| `ATOMIC.DEMAND_HISTORY` | Historical demand | product_id, demand_mt, demand_date |
| `ATOMIC.PRODUCT` | Product master | product_id, family, category |
| `ATOMIC.CUSTOMER` | Customer attributes | customer_id, tier, region |
| `CHEMICAL_OPS.COST_TO_SERVE` | Variable costs | product_id, destination, total_cost |

### 2.2 Marketplace Data Sources

| View | Source | Purpose |
|------|--------|---------|
| `MARKETPLACE_FUEL_OIL_CPI` | BLS via Snowflake Public Data | Energy cost proxy |
| `MARKETPLACE_CORE_CPI` | BLS via Snowflake Public Data | Inflation indicator |
| `MARKETPLACE_INDUSTRIAL_PRODUCTION` | Federal Reserve | Manufacturing activity |

### 2.3 Feature Engineering

#### Price Features
- `ln(P_own)`: Log of own product price
- `ln(P_substitute)`: Log of substitute product prices
- `price_vs_avg`: Price relative to 30-day moving average
- `price_change_pct`: Period-over-period price change

#### Market Condition Features
- `fuel_cpi`: Current Fuel Oil CPI
- `fuel_cpi_lag30/60/90`: Lagged CPI values
- `fuel_cpi_change`: Month-over-month CPI change
- `core_cpi`: Core CPI (less food & energy)
- `industrial_production`: Manufacturing activity index
- `cpi_spread`: Fuel Oil CPI - Core CPI (energy premium)

#### Temporal Features
- `month`: Seasonality capture
- `quarter`: Quarterly patterns
- `is_q4`: Year-end demand surge indicator

#### Customer Features (for heterogeneity models)
- `customer_tier`: Gold/Silver/Bronze
- `region`: Geographic segment
- `customer_volume`: Historical purchase volume

---

## 3. Model Hierarchy

### Model 1: Linear Regression (Baseline)

**Purpose**: Simple benchmark with interpretable coefficients

**Specification**:
```
ln(Q_i) = α + β₁·ln(P_i) + β₂·CPI + β₃·IP + γ·X + ε
```

**Interpretation**:
- β₁ = Own-price elasticity (expected: -1 to -3)
- β₂ = CPI sensitivity
- β₃ = Industrial production sensitivity

**Pros**:
- Fast estimation
- Easy to explain to business users
- Good for initial benchmarking

**Cons**:
- Ignores cross-product substitution
- Assumes constant elasticity
- Potential endogeneity bias

**Output**: Single elasticity per product (diagonal only)

---

### Model 2: Seemingly Unrelated Regressions (SUR)

**Purpose**: Capture cross-product correlations and estimate full elasticity matrix

**Specification**: System of N demand equations
```
ln(Q₁) = α₁ + β₁₁·ln(P₁) + β₁₂·ln(P₂) + ... + β₁ₙ·ln(Pₙ) + γ₁·X + ε₁
ln(Q₂) = α₂ + β₂₁·ln(P₁) + β₂₂·ln(P₂) + ... + β₂ₙ·ln(Pₙ) + γ₂·X + ε₂
...
ln(Qₙ) = αₙ + βₙ₁·ln(P₁) + βₙ₂·ln(P₂) + ... + βₙₙ·ln(Pₙ) + γₙ·X + εₙ
```

**Key Insight**: Error terms are correlated across equations → GLS estimation improves efficiency

**Elasticity Matrix Output**:
```
                Price Change In:
             PE      PP      PVC     Benzene
Demand  PE  [-1.2]  +0.3    +0.1    +0.05
For:    PP   +0.4  [-1.5]   +0.2    +0.1
        PVC  +0.1   +0.15  [-1.8]   +0.2
        Benzene +0.02 +0.05 +0.1   [-2.0]
```

**Interpretation**:
- Diagonal (negative): Own-price elasticity
- Off-diagonal positive: Substitutes
- Off-diagonal negative: Complements
- Near zero: Independent products

**Pros**:
- Full cross-elasticity matrix
- Captures substitution patterns
- Efficient estimation via GLS
- Still interpretable

**Cons**:
- Assumes log-linear functional form
- Doesn't address endogeneity
- No consumer heterogeneity

**Implementation**: `statsmodels.regression.linear_model.SUR` or `linearmodels`

---

### Model 3: Two-Stage Least Squares (2SLS)

**Purpose**: Address price endogeneity (prices respond to demand)

**The Problem**: OLS/SUR may be biased because:
- High demand → Firms raise prices → Observed correlation is wrong sign
- Simultaneity bias in price-quantity relationship

**Solution**: Instrumental Variables (IV)

**Valid Instruments** (affect supply/price, not demand directly):
- Feedstock costs (crude oil, naphtha prices)
- Competitor prices in other regions
- Exchange rates
- Production cost shocks

**Specification**:
```
Stage 1: ln(P_i) = π₀ + π₁·Feedstock_Cost + π₂·Z + v
Stage 2: ln(Q_i) = α + β·ln(P̂_i) + γ·X + ε
```

**Diagnostics Required**:
- First-stage F-statistic > 10 (instrument relevance)
- Sargan/Hansen test (instrument validity)
- Hausman test (OLS vs 2SLS comparison)

**Pros**:
- Consistent estimates under endogeneity
- More credible causal interpretation

**Cons**:
- Requires valid instruments (hard to find)
- Lower statistical power
- More complex implementation

---

### Model 4: Random Coefficient Logit (BLP-Style)

**Purpose**: Gold standard for demand estimation with heterogeneous preferences

**Key Innovation**: Different customers have different price sensitivities

**Utility Specification**:
```
U_ijt = δ_jt + σ_p·ν_i·P_jt + ε_ijt

where:
  δ_jt = Mean utility of product j at time t
  σ_p = Std dev of price sensitivity across customers
  ν_i ~ N(0,1) = Individual taste shock
  ε_ijt ~ Type 1 Extreme Value (logit error)
```

**Market Share** (requires simulation):
```
s_j = ∫ exp(δ_j + σ_p·ν·P_j) / (1 + Σ_k exp(δ_k + σ_p·ν·P_k)) dF(ν)
```

**Estimation Method**:
1. BLP contraction mapping for δ (match predicted to observed shares)
2. GMM with moment conditions from cost shifters
3. Simulate integrals via Monte Carlo

**Outputs**:
- Mean elasticity
- Elasticity distribution (heterogeneity)
- Segment-specific elasticities (Gold vs Bronze customers)
- Realistic substitution patterns (no IIA problem)

**Pros**:
- Flexible substitution patterns
- Captures customer heterogeneity
- Allows segment-level pricing
- Publication-quality methodology

**Cons**:
- Computationally intensive
- Requires careful implementation
- Harder to explain to business
- Needs sufficient data variation

**Implementation**: `PyBLP` library or custom implementation

---

## 4. Model Selection Criteria

| Criterion | Linear | SUR | 2SLS | Random Coef |
|-----------|--------|-----|------|-------------|
| Ease of implementation | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| Business interpretability | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| Cross-elasticity capture | ⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Endogeneity handling | ⭐ | ⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Heterogeneity capture | ⭐ | ⭐ | ⭐ | ⭐⭐⭐⭐⭐ |
| Computational speed | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |

**Recommendation**: Implement Linear + SUR first, add 2SLS for robustness check, consider Random Coefficient as advanced extension.

---

## 5. Notebook Structure

### `notebooks/INTEGRATED_DEMAND_PRICING_ENGINE.ipynb`

#### Section 1: Data Ingestion & Preparation
- Load ATOMIC tables (sales, demand, products)
- Load marketplace data (CPI, industrial production)
- Merge and clean datasets
- Feature engineering pipeline
- Train/test split (temporal, no leakage)

#### Section 2: Exploratory Data Analysis
- Price-quantity scatter plots by product
- Price correlation matrix across products
- Time series of market indicators
- Lagged correlation analysis (hidden discovery)

#### Section 3: Model 1 - Linear Regression
- Per-product elasticity estimation
- Coefficient interpretation
- Model diagnostics (R², residual plots)
- Output: Simple elasticity table

#### Section 4: Model 2 - SUR System
- Define product families
- Estimate system of equations
- Extract elasticity matrix
- Significance tests on cross-elasticities
- Output: Full N×N elasticity matrix

#### Section 5: Model 3 - IV/2SLS (Optional)
- Define instruments (feedstock costs)
- First-stage regression
- Second-stage estimation
- Compare with OLS estimates
- Output: IV-corrected elasticities

#### Section 6: Model 4 - Random Coefficient (Advanced)
- Choice model data preparation
- BLP estimation
- Elasticity distribution simulation
- Segment-level analysis
- Output: Heterogeneous elasticity estimates

#### Section 7: Model Comparison
- Side-by-side elasticity comparison
- Cross-validation metrics
- Business plausibility assessment
- Final model selection

#### Section 8: Persistence & Deployment
- Save elasticity matrix to Snowflake table
- Write model metadata
- Generate documentation

---

## 6. Output Tables

### `CHEMICAL_OPS.ELASTICITY_MATRIX`
```sql
CREATE TABLE ELASTICITY_MATRIX (
    model_version VARCHAR,
    estimation_date TIMESTAMP,
    product_i VARCHAR,      -- Demand product
    product_j VARCHAR,      -- Price product
    elasticity FLOAT,       -- εᵢⱼ
    std_error FLOAT,
    t_statistic FLOAT,
    p_value FLOAT,
    is_significant BOOLEAN
);
```

### `CHEMICAL_OPS.PRICE_ELASTICITY`
```sql
CREATE TABLE PRICE_ELASTICITY (
    model_version VARCHAR,
    product_id VARCHAR,
    own_elasticity FLOAT,
    elasticity_lower_95 FLOAT,
    elasticity_upper_95 FLOAT,
    cpi_sensitivity FLOAT,
    ip_sensitivity FLOAT
);
```

### `CHEMICAL_OPS.MODEL_METADATA`
```sql
CREATE TABLE MODEL_METADATA (
    model_version VARCHAR,
    model_type VARCHAR,
    training_date TIMESTAMP,
    training_rows INT,
    r_squared FLOAT,
    rmse FLOAT,
    notes VARCHAR
);
```

---

## 7. Success Metrics

### Statistical Metrics
- R² > 0.7 for demand models
- Elasticity signs correct (own-price negative)
- Cross-elasticities economically plausible
- Forecast MAPE < 15%

### Business Metrics
- Pricing recommendations improve margin by X%
- Model predictions align with observed market behavior
- Business users trust and adopt recommendations

---

## 8. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Insufficient price variation | Use longer time series, include promotions |
| Endogeneity bias | Implement 2SLS with cost instruments |
| Multicollinearity in SUR | Aggregate to product families |
| Overfitting | Time-based cross-validation |
| Data quality issues | Robust preprocessing, outlier handling |

---

## 9. Timeline

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| Data Prep | 1 week | Clean dataset, features |
| Linear Model | 1 week | Baseline elasticities |
| SUR Model | 1 week | Elasticity matrix |
| Integration | 1 week | Dashboard, tables |
| Advanced Models | 2 weeks | 2SLS, Random Coef (optional) |

---

## 10. References

- Berry, Levinsohn, Pakes (1995) - BLP methodology
- Nevo (2000) - Practitioner's guide to BLP
- Zellner (1962) - SUR estimation
- Stock & Watson - Econometrics textbook for IV methods
