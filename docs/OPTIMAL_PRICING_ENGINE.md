# Optimal Pricing Engine: Operations Research Design

## Overview

This document outlines the Operations Research (OR) architecture for the Optimal Pricing Engine, which transforms elasticity estimates into actionable price recommendations through constrained optimization.

---

## 1. Business Objectives

### Primary Goals
1. **Maximize Profitability**: Find optimal price vector across product portfolio
2. **Respect Constraints**: Honor business rules, contracts, and market realities
3. **Enable What-If Analysis**: Let managers explore pricing scenarios interactively
4. **Quantify Trade-offs**: Show revenue vs margin Pareto frontier

### Key Questions to Answer
- "What prices maximize profit given our cost structure?"
- "How much margin am I sacrificing to maintain market share?"
- "If I'm constrained to ±5% price changes, what's the best I can do?"
- "How should I adjust prices when CPI rises 3%?"

---

## 2. Mathematical Formulation

### 2.1 Decision Variables

```
P = (P₁, P₂, ..., Pₙ)  ∈ ℝⁿ₊

where Pᵢ = Price of product i
```

### 2.2 Demand Function

From the SUR elasticity model:
```
Qᵢ(P) = Q₀ᵢ × exp(Σⱼ εᵢⱼ × ln(Pⱼ/P₀ⱼ))

where:
  Q₀ᵢ   = Baseline demand at reference prices
  εᵢⱼ   = Cross-elasticity of product i w.r.t. product j price
  P₀ⱼ   = Reference price for product j
```

### 2.3 Objective Functions

#### Option A: Profit Maximization (Default)
```
max  Π(P) = Σᵢ (Pᵢ - Cᵢ) × Qᵢ(P)
 P

where Cᵢ = Variable cost of product i
```

#### Option B: Revenue Maximization
```
max  R(P) = Σᵢ Pᵢ × Qᵢ(P)
 P
```

#### Option C: Volume Maximization
```
max  V(P) = Σᵢ Qᵢ(P)
 P
```

#### Option D: Multi-Objective (Pareto)
```
max  [Π(P), R(P), V(P)]
 P

→ Generate Pareto frontier of non-dominated solutions
```

### 2.4 Expanded Profit Function

Substituting the demand function:
```
Π(P) = Σᵢ (Pᵢ - Cᵢ) × Q₀ᵢ × exp(Σⱼ εᵢⱼ × ln(Pⱼ/P₀ⱼ))
```

**Note**: This is a non-linear, non-convex optimization problem.

---

## 3. Constraint Taxonomy

### 3.1 Margin Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| Minimum Margin % | `(Pᵢ - Cᵢ)/Pᵢ ≥ Mᵢ` | "Never sell below 15% margin" |
| Minimum Margin $ | `Pᵢ - Cᵢ ≥ mᵢ` | "At least $50/MT contribution" |
| Target Margin | `(Pᵢ - Cᵢ)/Pᵢ ≈ M*ᵢ` | Soft constraint, penalty in objective |

**Equivalent Form** (for optimization):
```
Pᵢ ≥ Cᵢ / (1 - Mᵢ)
```

### 3.2 Price Bound Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| Price Floor | `Pᵢ ≥ P̲ᵢ` | Cost-plus minimum, market floor |
| Price Ceiling | `Pᵢ ≤ P̄ᵢ` | Contractual cap, market resistance |
| Price Range | `P̲ᵢ ≤ Pᵢ ≤ P̄ᵢ` | Combined bounds |

### 3.3 Price Change Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| Max Increase % | `Pᵢ ≤ P₀ᵢ × (1 + Δ⁺)` | "No more than 5% increase" |
| Max Decrease % | `Pᵢ ≥ P₀ᵢ × (1 - Δ⁻)` | "No more than 5% decrease" |
| Symmetric Change | `\|Pᵢ - P₀ᵢ\|/P₀ᵢ ≤ Δmax` | Combined limit |

**Linear Form**:
```
P₀ᵢ × (1 - Δ⁻) ≤ Pᵢ ≤ P₀ᵢ × (1 + Δ⁺)
```

### 3.4 Competitive Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| Competitor Parity | `Pᵢ ≤ Pᶜᵒᵐᵖᵢ × (1 + premium)` | "Stay within 10% of competitor" |
| Market Share Floor | `Qᵢ(P) / Σⱼ Qⱼ(P) ≥ shareᵢ` | "Maintain 20% market share" |
| Demand Floor | `Qᵢ(P) ≥ Q̲ᵢ` | Minimum volume threshold |

### 3.5 Operational Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| Capacity Limit | `Σᵢ Qᵢ(P) ≤ K` | Production ceiling |
| Per-Product Capacity | `Qᵢ(P) ≤ Kᵢ` | Line-specific limits |
| Inventory Constraint | `Qᵢ(P) ≤ Invᵢ + Prodᵢ` | Can't sell what you don't have |
| Product Mix | `Qᵢ(P) / Σⱼ Qⱼ(P) ≥ mixᵢ` | "At least 30% premium products" |

### 3.6 Contract/Volume Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| Contract Volume | `Qᵢ(P) ≥ Qᶜᵒⁿᵗʳᵃᶜᵗᵢ` | Honor committed volumes |
| Contract Price | `Pᵢ = Pᶜᵒⁿᵗʳᵃᶜᵗᵢ` | Fixed price contracts (exclude from optimization) |

### 3.7 Strategic/Structural Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| Price Ladder | `P_premium ≥ P_standard × (1 + tierGap)` | Maintain tier differentiation |
| Regional Parity | `\|Pᵢ,APAC - Pᵢ,EMEA\| ≤ arbitrageLimit` | Prevent gray market |
| Customer Tier | `P_Gold ≤ P_Standard × (1 - discount)` | Honor tier pricing |

### 3.8 Market Condition Constraints

| Constraint | Mathematical Form | Business Rationale |
|------------|-------------------|-------------------|
| CPI Pass-through | `ΔPᵢ ≥ β × ΔCPI` | Inflation adjustment floor |
| Feedstock Linkage | `Pᵢ ≥ α × Feedstock_Cost` | Cost-indexed minimum |
| Downturn Rule | `If IP ↓ 5%, then ΔPᵢ ≤ 0` | Don't raise in recession |

---

## 4. Complete Problem Statement

```
MAXIMIZE    Π(P) = Σᵢ (Pᵢ - Cᵢ) × Q₀ᵢ × exp(Σⱼ εᵢⱼ × ln(Pⱼ/P₀ⱼ))

SUBJECT TO:

  // Margin Constraints
  (Pᵢ - Cᵢ)/Pᵢ ≥ Mᵢ                    ∀i ∈ Products

  // Price Bounds
  P̲ᵢ ≤ Pᵢ ≤ P̄ᵢ                         ∀i ∈ Products

  // Change Limits
  P₀ᵢ × (1 - Δ⁻) ≤ Pᵢ ≤ P₀ᵢ × (1 + Δ⁺)  ∀i ∈ Products

  // Capacity
  Σᵢ Qᵢ(P) ≤ K

  // Contracts
  Qᵢ(P) ≥ Qᶜᵒⁿᵗʳᵃᶜᵗᵢ                    ∀i ∈ Contracted

  // Price Ladder
  Pᵢ ≥ Pⱼ × (1 + tierGapᵢⱼ)            ∀(i,j) ∈ Ladder

  // Market Conditions
  Pᵢ ≤ P₀ᵢ × (1 + β × ΔCPI)           if Industrial_Production declining

DECISION VARIABLES:
  Pᵢ ∈ ℝ⁺  for i = 1, ..., n

PARAMETERS (from data/models):
  εᵢⱼ       = Cross-elasticity matrix (from SUR model)
  Cᵢ        = Variable cost (from COST_TO_SERVE view)
  Q₀ᵢ, P₀ᵢ  = Baseline demand and prices (from MARGIN_ANALYZER)
  CPI, IP   = Market indicators (from Marketplace views)
  Mᵢ        = Minimum margin requirement
  Δ⁺, Δ⁻    = Price change limits
```

---

## 5. Solution Methods

### 5.1 Problem Characteristics
- **Non-linear objective**: Due to exponential demand function
- **Non-convex**: Multiple local optima possible
- **Continuous variables**: Prices are real-valued
- **Dimension**: n products (typically 10-50)

### 5.2 Recommended Solvers

#### For Small Problems (n < 20)
**Method**: Sequential Quadratic Programming (SQP)
```python
from scipy.optimize import minimize
result = minimize(objective, x0, method='SLSQP', 
                  constraints=constraints, bounds=bounds)
```

**Pros**: Fast, handles smooth non-linear constraints
**Cons**: May find local optimum

#### For Medium Problems (20 < n < 100)
**Method**: Interior Point Method (IPOPT)
```python
import cyipopt
# Requires gradient and Hessian
```

**Pros**: Robust convergence, handles large constraint sets
**Cons**: More complex setup

#### For Global Optimization
**Method**: Multi-start + Local Search
```python
from scipy.optimize import differential_evolution, basinhopping
```

**Pros**: Better chance of global optimum
**Cons**: Slower, may not scale

### 5.3 Gradient Computation

For efficiency, compute gradients analytically:
```
∂Π/∂Pₖ = Qₖ(P) + Σᵢ (Pᵢ - Cᵢ) × Qᵢ(P) × (εᵢₖ/Pₖ)
```

---

## 6. Problem Variants

### Variant A: Pure Profit Maximization
```
max Π(P) subject to basic bounds only
```
**Use Case**: Commodity products, price-taking markets

### Variant B: Revenue with Margin Floor
```
max R(P) subject to Π(P) ≥ Π_target
```
**Use Case**: Market share growth phase

### Variant C: Robust Optimization
```
max min Π(P, ε̃) subject to ε̃ ∈ Uncertainty Set
    P   ε̃
```
**Use Case**: Elasticity estimates have confidence intervals

### Variant D: Multi-Period Dynamic Pricing
```
max Σₜ δᵗ × Πₜ(Pₜ) subject to |Pₜ - Pₜ₋₁| ≤ Δ
```
**Use Case**: Quarterly price trajectory optimization

---

## 7. Dashboard Integration

### 7.1 Input Panel: Optimization Configuration

```
┌─────────────────────────────────────────────────────────────┐
│  🎯 OBJECTIVE                                                │
│  ○ Maximize Profit  ● Maximize Revenue  ○ Maximize Volume   │
│                                                              │
│  📊 PRODUCT SCOPE                                            │
│  [✓] Polyethylene  [✓] Polypropylene  [ ] PVC  [✓] Benzene  │
│                                                              │
│  🌍 REGION: [All ▼]    📅 HORIZON: [Next Quarter ▼]         │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 Constraints Panel

```
┌─────────────────────────────────────────────────────────────┐
│  💰 MARGIN CONSTRAINTS                                       │
│  Minimum Margin %:  [=====●====] 15%                        │
│                                                              │
│  📈 PRICE CHANGE LIMITS                                      │
│  Max Increase:  [====●=====] +5%                            │
│  Max Decrease:  [====●=====] -5%                            │
│                                                              │
│  🏭 OPERATIONAL                                              │
│  [✓] Respect capacity limits (Current: 85% utilized)        │
│  [✓] Honor contract volumes                                 │
│  [ ] Maintain price ladder                                  │
│                                                              │
│  🌡️ MARKET CONDITIONS                                        │
│  [✓] Factor in CPI trend: STABLE                            │
│  [✓] Industrial Production: 90.1                            │
└─────────────────────────────────────────────────────────────┘
```

### 7.3 Elasticity Matrix Display

```
┌─────────────────────────────────────────────────────────────┐
│  📊 CROSS-PRICE ELASTICITY MATRIX                            │
│                                                              │
│              PE      PP      PVC     Benzene                │
│     PE    [-1.24]   +0.31   +0.12    +0.05                  │
│     PP     +0.28  [-1.52]   +0.18    +0.09                  │
│     PVC    +0.15   +0.21  [-1.87]    +0.14                  │
│     Benzene +0.04  +0.08   +0.11   [-2.10]                  │
│                                                              │
│  [Heatmap visualization with color scale]                   │
└─────────────────────────────────────────────────────────────┘
```

### 7.4 Results Panel

```
┌─────────────────────────────────────────────────────────────┐
│  🎯 OPTIMIZATION RESULTS                                     │
│                                                              │
│  Status: ✅ OPTIMAL (Solved in 0.3s)                         │
│                                                              │
│  Product      Current   Optimal   Δ%      Impact            │
│  ─────────────────────────────────────────────────────────  │
│  Polyethylene  $1,450   $1,495   +3.1%   +$2.1M margin     │
│  Polypropylene $1,380   $1,355   -1.8%   +$0.8M volume     │
│  Benzene       $1,120   $1,145   +2.2%   +$0.5M margin     │
│  ─────────────────────────────────────────────────────────  │
│  PORTFOLIO     ─────    ─────    ────    +$3.4M total      │
│                                                              │
│  ⚠️ BINDING CONSTRAINTS:                                     │
│     • PP at minimum margin floor (15%)                      │
│     • PE at max price increase limit (+5%)                  │
│                                                              │
│  💡 INSIGHT: Raising PE by 3.1% captures $1.2M from PP      │
│              substitution effect                             │
└─────────────────────────────────────────────────────────────┘
```

### 7.5 What-If Explorer

```
┌─────────────────────────────────────────────────────────────┐
│  🔍 SENSITIVITY ANALYSIS                                     │
│                                                              │
│  "What if PE elasticity is -1.5 instead of -1.24?"          │
│                                                              │
│  Elasticity: [-2.0 ====●==== -1.0]  Value: -1.50            │
│                                                              │
│  Impact:                                                     │
│    Optimal PE Price:  $1,495 → $1,472 (-1.5%)               │
│    Total Margin:      +$3.4M → +$3.1M (-8.8%)               │
│                                                              │
│  ──────────────────────────────────────────────────────────  │
│  📊 TORNADO CHART: Margin Sensitivity                        │
│                                                              │
│  PE own-price ████████████████░░░░  High                    │
│  PP own-price █████████████░░░░░░░  Medium                  │
│  PE-PP cross  ████████░░░░░░░░░░░░  Medium                  │
│  Benzene      ███░░░░░░░░░░░░░░░░░  Low                     │
└─────────────────────────────────────────────────────────────┘
```

### 7.6 Pareto Frontier

```
┌─────────────────────────────────────────────────────────────┐
│  ⚖️ REVENUE vs MARGIN TRADE-OFF                              │
│                                                              │
│  Margin │                                                    │
│   $60M  ┤                              ●A                    │
│         │                           ●                        │
│   $55M  ┤                        ●                           │
│         │                     ★  ← You are here             │
│   $50M  ┤                  ●                                 │
│         │               ●                                    │
│   $45M  ┤            ●                                       │
│         │         ●                                          │
│   $40M  ┤      ●B                                            │
│         └──────────────────────────────────────────────────  │
│           $160M    $180M    $200M    $220M   Revenue        │
│                                                              │
│  Click point to see price configuration                     │
│  Selected: ● Revenue $185M | Margin $52M                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 8. Advanced Features

### 8.1 Scenario Management
- **Save/Load**: Store constraint configurations
- **Compare**: Side-by-side of "aggressive" vs "conservative"
- **History**: What would optimal prices have been last quarter?

### 8.2 Decomposition Analysis
- **Contribution breakdown**: Which constraint is costing the most?
- **Shadow prices**: Value of relaxing each constraint by 1 unit
- **Slack analysis**: How close are we to binding?

### 8.3 Competitive Response
- **Game theory**: If we move, how will competitors respond?
- **Nash equilibrium**: Stable industry pricing
- **Stackelberg**: Leader-follower dynamics

### 8.4 Time-Phased Optimization
- **Rolling horizon**: Optimize 12-month price trajectory
- **Smoothing**: Avoid price whiplash
- **Seasonality**: Different constraints by quarter

---

## 9. Output Tables

### `CHEMICAL_OPS.OPTIMAL_PRICING`
```sql
CREATE TABLE OPTIMAL_PRICING (
    scenario_id VARCHAR,
    optimization_date TIMESTAMP,
    product_id VARCHAR,
    current_price FLOAT,
    optimal_price FLOAT,
    price_change_pct FLOAT,
    expected_volume FLOAT,
    expected_margin FLOAT,
    binding_constraints VARCHAR[]
);
```

### `CHEMICAL_OPS.OPTIMIZATION_SCENARIOS`
```sql
CREATE TABLE OPTIMIZATION_SCENARIOS (
    scenario_id VARCHAR PRIMARY KEY,
    scenario_name VARCHAR,
    created_by VARCHAR,
    created_at TIMESTAMP,
    objective_type VARCHAR,
    constraint_config VARIANT,
    total_margin FLOAT,
    total_revenue FLOAT,
    status VARCHAR
);
```

---

## 10. Implementation Roadmap

| Phase | Component | Deliverable |
|-------|-----------|-------------|
| **1** | Basic Optimizer | Single-objective, basic constraints |
| **2** | Constraint Engine | Full constraint taxonomy |
| **3** | Dashboard UI | Interactive sliders, results display |
| **4** | Elasticity Integration | SUR matrix as input |
| **5** | Sensitivity Analysis | Tornado chart, what-if |
| **6** | Pareto Frontier | Multi-objective visualization |
| **7** | Scenario Management | Save/load/compare |
| **8** | Advanced | Robust optimization, game theory |

---

## 11. Success Metrics

### Technical
- Optimization solves in < 5 seconds
- Solutions satisfy all constraints
- Reproducible results

### Business
- Price recommendations adopted by pricing team
- Margin improvement of X% vs status quo
- Reduction in manual pricing analysis time

---

## 12. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Non-convergence | Multi-start, regularization |
| Unrealistic recommendations | Tighter bounds, business review |
| Elasticity uncertainty | Robust optimization, sensitivity analysis |
| User trust | Transparency in methodology, explainability |

---

## 13. Integration Points

| Component | Integration |
|-----------|-------------|
| `ELASTICITY_MATRIX` | Input εᵢⱼ parameters |
| `COST_TO_SERVE` | Input Cᵢ costs |
| `MARGIN_ANALYZER` | Baseline P₀, Q₀ |
| `MARKETPLACE_*` | Market condition parameters |
| `PRICE_GUIDANCE` | Comparison benchmark |
| Cortex Agent | "What's optimal price for PE?" |
| Streamlit | Interactive dashboard |
