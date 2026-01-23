# Implementation Roadmap: ML + Optimization Integration

## Executive Summary

This document outlines the implementation plan for integrating advanced ML models (price elasticity) with an Operations Research optimization engine to create a decision intelligence system for chemicals pricing.

---

## Current State

### ✅ Completed Components
- Database structure (CHEMICALS_DB with RAW, ATOMIC, CHEMICAL_OPS schemas)
- Synthetic data generation (19 products, 200 customers, 13K+ orders)
- Core views (MARGIN_ANALYZER, COST_TO_SERVE, PRICE_GUIDANCE)
- Marketplace integration (Fuel Oil CPI, Core CPI, Industrial Production)
- Cortex Agent deployment
- Streamlit app with 6 pages

### 🔲 To Be Built
- Elasticity estimation models (Linear, SUR)
- Cross-price elasticity matrix
- Optimal pricing engine
- Enhanced dashboard with optimization UI

---

## Phase 1: Data Preparation & Feature Engineering

**Duration**: 3-4 days

### 1.1 Create Analysis Dataset

```sql
-- New view: CHEMICAL_OPS.ML_TRAINING_DATA
-- Joins sales, products, customers, costs, market indicators
```

**Fields**:
- Transaction level: order_date, product_id, customer_id, quantity, price
- Product attributes: family, category
- Customer attributes: tier, region
- Cost data: variable_cost, total_cost
- Market data: fuel_cpi, core_cpi, industrial_production
- Lagged prices: price_lag_7d, price_lag_30d

### 1.2 Feature Engineering Pipeline

Create `notebooks/01_data_preparation.ipynb`:
- Load raw data from Snowflake
- Compute log-transforms: ln(P), ln(Q)
- Create price relatives: P / P_avg_30d
- Add temporal features: month, quarter, year
- Merge marketplace indicators with lags
- Train/test split (temporal)

### 1.3 Deliverables
- [ ] `ML_TRAINING_DATA` view in Snowflake
- [ ] Feature engineering notebook
- [ ] Data quality report

---

## Phase 2: Linear Regression Baseline

**Duration**: 2-3 days

### 2.1 Model Implementation

Create `notebooks/02_linear_elasticity.ipynb`:

```python
# Per-product elasticity estimation
for product in products:
    model = OLS(ln(Q) ~ ln(P) + CPI + IP + month_dummies)
    elasticity[product] = model.params['ln_price']
```

### 2.2 Model Validation
- Check elasticity signs (should be negative)
- R² and adjusted R²
- Residual diagnostics
- Out-of-sample MAPE

### 2.3 Output Tables

```sql
CREATE TABLE CHEMICAL_OPS.PRICE_ELASTICITY_LINEAR (
    product_id VARCHAR,
    own_elasticity FLOAT,
    std_error FLOAT,
    cpi_sensitivity FLOAT,
    ip_sensitivity FLOAT,
    r_squared FLOAT,
    estimation_date TIMESTAMP
);
```

### 2.4 Deliverables
- [ ] Linear elasticity notebook
- [ ] `PRICE_ELASTICITY_LINEAR` table populated
- [ ] Model comparison report

---

## Phase 3: SUR Model & Elasticity Matrix

**Duration**: 3-4 days

### 3.1 Model Implementation

Create `notebooks/03_sur_elasticity.ipynb`:

```python
from linearmodels.system import SUR

# Define system of equations
equations = {
    'PE': 'ln_Q_PE ~ ln_P_PE + ln_P_PP + ln_P_PVC + CPI + IP',
    'PP': 'ln_Q_PP ~ ln_P_PE + ln_P_PP + ln_P_PVC + CPI + IP',
    'PVC': 'ln_Q_PVC ~ ln_P_PE + ln_P_PP + ln_P_PVC + CPI + IP',
}

model = SUR.from_formula(equations, data)
results = model.fit()

# Extract elasticity matrix
elasticity_matrix = extract_cross_elasticities(results)
```

### 3.2 Elasticity Matrix Output

```sql
CREATE TABLE CHEMICAL_OPS.ELASTICITY_MATRIX (
    model_version VARCHAR,
    estimation_date TIMESTAMP,
    demand_product VARCHAR,    -- Row: product whose demand is affected
    price_product VARCHAR,     -- Column: product whose price changed
    elasticity FLOAT,
    std_error FLOAT,
    t_stat FLOAT,
    p_value FLOAT,
    is_significant BOOLEAN
);
```

### 3.3 Validation
- Diagonal elements negative (own-price)
- Off-diagonal signs economically plausible
- Symmetry check (optional Slutsky constraint)
- Statistical significance tests

### 3.4 Deliverables
- [ ] SUR elasticity notebook
- [ ] `ELASTICITY_MATRIX` table populated
- [ ] Elasticity matrix heatmap visualization

---

## Phase 4: Optimal Pricing Engine - Core

**Duration**: 4-5 days

### 4.1 Optimizer Implementation

Create `notebooks/04_optimal_pricing.ipynb`:

```python
from scipy.optimize import minimize

def objective(P, Q0, C, elasticity_matrix, P0):
    """Profit maximization objective."""
    Q = demand_function(P, Q0, elasticity_matrix, P0)
    profit = sum((P - C) * Q)
    return -profit  # Minimize negative profit

def demand_function(P, Q0, E, P0):
    """Log-linear demand with cross-elasticities."""
    log_price_ratio = np.log(P / P0)
    log_Q = np.log(Q0) + E @ log_price_ratio
    return np.exp(log_Q)

# Constraints
constraints = [
    {'type': 'ineq', 'fun': margin_constraint},
    {'type': 'ineq', 'fun': price_change_constraint},
]

# Bounds
bounds = [(P_floor[i], P_ceil[i]) for i in range(n_products)]

# Solve
result = minimize(objective, P_current, method='SLSQP',
                  constraints=constraints, bounds=bounds)
```

### 4.2 Constraint Functions

```python
def margin_constraint(P):
    """Minimum margin >= 15%"""
    margins = (P - C) / P
    return margins - 0.15  # Must be >= 0

def price_change_constraint(P):
    """Price change <= 5%"""
    changes = np.abs(P - P0) / P0
    return 0.05 - changes  # Must be >= 0
```

### 4.3 Output Tables

```sql
CREATE TABLE CHEMICAL_OPS.OPTIMAL_PRICING (
    scenario_id VARCHAR,
    run_date TIMESTAMP,
    product_id VARCHAR,
    current_price FLOAT,
    optimal_price FLOAT,
    price_change_pct FLOAT,
    current_volume FLOAT,
    predicted_volume FLOAT,
    current_margin FLOAT,
    predicted_margin FLOAT
);

CREATE TABLE CHEMICAL_OPS.OPTIMIZATION_RUN_LOG (
    scenario_id VARCHAR,
    run_date TIMESTAMP,
    objective_type VARCHAR,
    solver_status VARCHAR,
    solve_time_seconds FLOAT,
    total_profit_current FLOAT,
    total_profit_optimal FLOAT,
    profit_improvement_pct FLOAT,
    binding_constraints ARRAY
);
```

### 4.4 Deliverables
- [ ] Optimization engine notebook
- [ ] `OPTIMAL_PRICING` table
- [ ] `OPTIMIZATION_RUN_LOG` table
- [ ] Unit tests for optimizer

---

## Phase 5: Dashboard Integration

**Duration**: 4-5 days

### 5.1 New Streamlit Page: Pricing Optimizer

Create `streamlit/pages/6_Pricing_Optimizer.py`:

**Components**:
1. **Objective Selector**: Profit / Revenue / Volume radio buttons
2. **Product Scope**: Multi-select checkboxes
3. **Constraint Sliders**: Min margin, max price change
4. **Elasticity Matrix Display**: Heatmap visualization
5. **Results Table**: Current vs Optimal prices
6. **Impact Summary**: Total margin/revenue improvement
7. **Binding Constraints**: Which constraints are active

### 5.2 Backend Integration

```python
# In Streamlit page
def run_optimization(session, params):
    # Load elasticity matrix
    E = load_elasticity_matrix(session)
    
    # Load current prices and costs
    current_data = load_margin_analyzer(session)
    
    # Run optimizer
    result = optimize_prices(E, current_data, params)
    
    # Save results
    save_optimization_results(session, result)
    
    return result
```

### 5.3 Deliverables
- [ ] `6_Pricing_Optimizer.py` Streamlit page
- [ ] Elasticity heatmap component
- [ ] Results visualization
- [ ] Upload to Snowflake stage

---

## Phase 6: Sensitivity & What-If Analysis

**Duration**: 2-3 days

### 6.1 Sensitivity Analysis

Add to optimizer page:
- **Elasticity sliders**: Adjust individual elasticities, see impact
- **Tornado chart**: Which parameters matter most
- **Confidence intervals**: Show range of optimal prices

### 6.2 Pareto Frontier

```python
def compute_pareto_frontier(n_points=20):
    """Generate revenue-margin trade-off curve."""
    frontier = []
    for margin_target in np.linspace(min_margin, max_margin, n_points):
        result = optimize_with_margin_target(margin_target)
        frontier.append({
            'revenue': result.revenue,
            'margin': result.margin,
            'prices': result.optimal_prices
        })
    return frontier
```

### 6.3 Deliverables
- [ ] Sensitivity analysis component
- [ ] Tornado chart visualization
- [ ] Pareto frontier plot
- [ ] Interactive what-if explorer

---

## Phase 7: End-to-End Notebook

**Duration**: 2-3 days

### 7.1 Master Notebook

Create `notebooks/INTEGRATED_DEMAND_PRICING_ENGINE.ipynb`:

**Sections**:
1. Executive Summary
2. Data Ingestion & Preparation
3. Exploratory Data Analysis
4. Model 1: Linear Regression
5. Model 2: SUR System
6. Model Comparison
7. Optimal Pricing Engine
8. Sensitivity Analysis
9. Results & Recommendations
10. Model Persistence

### 7.2 Deliverables
- [ ] Complete end-to-end notebook
- [ ] Executive summary section
- [ ] All visualizations polished
- [ ] Documentation comments

---

## Phase 8: Advanced Extensions (Optional)

**Duration**: Variable

### 8.1 IV/2SLS Estimation
- Implement instrumental variable regression
- Use feedstock costs as instruments
- Compare with OLS estimates

### 8.2 Random Coefficient Logit
- Implement BLP-style model
- Capture customer heterogeneity
- Segment-level elasticities

### 8.3 Robust Optimization
- Uncertainty sets for elasticities
- Worst-case optimization
- Risk-adjusted pricing

### 8.4 Game Theory Extensions
- Competitive response modeling
- Nash equilibrium computation
- Stackelberg games

---

## Timeline Summary

| Phase | Description | Duration | Dependencies |
|-------|-------------|----------|--------------|
| 1 | Data Preparation | 3-4 days | Marketplace data ✅ |
| 2 | Linear Regression | 2-3 days | Phase 1 |
| 3 | SUR Model | 3-4 days | Phase 1 |
| 4 | Optimization Core | 4-5 days | Phase 3 |
| 5 | Dashboard | 4-5 days | Phase 4 |
| 6 | Sensitivity | 2-3 days | Phase 5 |
| 7 | Master Notebook | 2-3 days | Phases 2-4 |
| 8 | Advanced (optional) | Variable | Phase 7 |

**Total Core Implementation**: ~3-4 weeks

---

## Success Criteria

### Technical
- [ ] Elasticity matrix with all significant cross-effects identified
- [ ] Optimizer converges in < 5 seconds
- [ ] Dashboard loads and runs optimization interactively
- [ ] All models documented in master notebook

### Business
- [ ] Elasticity signs match economic intuition
- [ ] Optimal prices improve margin by measurable amount
- [ ] Binding constraints are interpretable
- [ ] Stakeholders can use dashboard independently

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Insufficient price variation | Medium | High | Use longer history, add promotions |
| SUR doesn't converge | Low | Medium | Aggregate products, regularize |
| Optimizer finds local minimum | Medium | Medium | Multi-start, different initializations |
| Dashboard too slow | Low | Medium | Precompute elasticities, cache results |
| Business rejects recommendations | Medium | High | Involve stakeholders early, explain methodology |

---

## File Structure

```
chemicals_pricing_supply_chain/
├── docs/
│   ├── ML_MODEL_DESIGN.md           ← You are here
│   ├── OPTIMAL_PRICING_ENGINE.md
│   └── IMPLEMENTATION_ROADMAP.md
├── notebooks/
│   ├── 01_data_preparation.ipynb
│   ├── 02_linear_elasticity.ipynb
│   ├── 03_sur_elasticity.ipynb
│   ├── 04_optimal_pricing.ipynb
│   └── INTEGRATED_DEMAND_PRICING_ENGINE.ipynb
├── streamlit/
│   └── pages/
│       └── 6_Pricing_Optimizer.py
├── ddl/
│   └── core/
│       └── 007_ML_TABLES.sql        ← New tables for ML outputs
└── scripts/
    └── run_elasticity_estimation.py  ← Optional: batch runner
```

---

## Next Steps

1. **Review this plan** with stakeholders
2. **Begin Phase 1**: Create ML_TRAINING_DATA view
3. **Iterate**: Build, test, refine each phase
4. **Demo**: Show progress at each milestone

---

## Appendix: Key Formulas

### Demand Function
```
Qᵢ(P) = Q₀ᵢ × exp(Σⱼ εᵢⱼ × ln(Pⱼ/P₀ⱼ))
```

### Profit Function
```
Π(P) = Σᵢ (Pᵢ - Cᵢ) × Qᵢ(P)
```

### Elasticity Interpretation
```
εᵢⱼ = (∂Qᵢ/∂Pⱼ) × (Pⱼ/Qᵢ) = % change in Qᵢ per 1% change in Pⱼ
```

### Margin Constraint
```
(Pᵢ - Cᵢ)/Pᵢ ≥ Mᵢ  ⟺  Pᵢ ≥ Cᵢ/(1 - Mᵢ)
```
