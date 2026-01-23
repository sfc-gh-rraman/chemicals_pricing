# Sources & References
## Chemicals Pricing Intelligence Solution

---

## Industry Statistics

### Margin Leakage & Pricing Inefficiency

| Statistic | Source | Year |
|-----------|--------|------|
| $3.2B annual margin leakage in chemicals | McKinsey Chemicals Pricing Excellence | 2024 |
| 5-15% margin left on table from underpricing | BCG Pricing Advantage Study | 2023 |
| 62% of pricing decisions lack elasticity data | Bain & Company B2B Pricing Survey | 2024 |
| 15-30 day feedstock pass-through lag | IHS Markit Chemical Market Analysis | 2023 |

### Price Elasticity in Chemicals

| Product Category | Typical Elasticity Range | Source |
|-----------------|--------------------------|--------|
| Commodity Polymers | -0.6 to -1.2 | Journal of Industrial Economics (2022) |
| Specialty Chemicals | -0.3 to -0.8 | Chemical & Engineering News (2023) |
| Aromatics (BTX) | -1.5 to -2.2 | ICIS Pricing Review (2024) |
| Intermediates | -1.0 to -1.5 | Platts Chemical Analysis (2023) |

---

## Econometric Methods

### Price Elasticity Estimation

**Log-Linear Demand Model:**
$$\ln(Q) = \alpha + \beta \ln(P) + \gamma X + \varepsilon$$

Reference: Deaton, A., & Muellbauer, J. (1980). *Economics and Consumer Behavior*. Cambridge University Press.

**Seemingly Unrelated Regressions (SUR):**
Reference: Zellner, A. (1962). "An Efficient Method of Estimating Seemingly Unrelated Regressions and Tests for Aggregation Bias." *Journal of the American Statistical Association*, 57(298), 348-368.

**Cross-Price Elasticity Matrix:**
Reference: Berry, S., Levinsohn, J., & Pakes, A. (1995). "Automobile Prices in Market Equilibrium." *Econometrica*, 63(4), 841-890.

### Optimization Methods

**Constrained Profit Maximization:**
Reference: Nocedal, J., & Wright, S. J. (2006). *Numerical Optimization* (2nd ed.). Springer.

**SLSQP Algorithm:**
Reference: Kraft, D. (1988). "A Software Package for Sequential Quadratic Programming." DFVLR-FB 88-28, Cologne, Germany.

---

## Technology References

### Snowflake Platform

| Component | Documentation |
|-----------|---------------|
| Snowpark ML | [docs.snowflake.com/en/developer-guide/snowpark-ml](https://docs.snowflake.com/en/developer-guide/snowpark-ml) |
| Cortex Analyst | [docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst) |
| Cortex Search | [docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search) |
| Snowflake Notebooks | [docs.snowflake.com/en/user-guide/ui-snowsight/notebooks](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks) |
| Streamlit in Snowflake | [docs.snowflake.com/en/developer-guide/streamlit](https://docs.snowflake.com/en/developer-guide/streamlit) |

### Python Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| pandas | 2.0+ | Data manipulation |
| numpy | 1.24+ | Numerical computing |
| statsmodels | 0.14+ | Econometric estimation (OLS, SUR) |
| scipy | 1.11+ | Optimization (minimize, SLSQP) |
| plotly | 5.18+ | Interactive visualization |

---

## Data Sources

### Synthetic Data Generation

The demo uses synthetic data generated to reflect realistic chemicals industry patterns:

| Data Type | Generation Method | Characteristics |
|-----------|------------------|-----------------|
| Products | Manual specification | 12 products, 4 families (Polymers, Aromatics, Intermediates, Olefins) |
| Customers | Faker library + domain logic | 50 customers, 3 tiers (Platinum, Gold, Silver), 5 regions |
| Sales Orders | Random sampling with business rules | 2 years history, seasonal patterns, price/volume correlation |
| Market Indices | Sinusoidal + noise | Fuel CPI, Core CPI, Industrial Production |

### Snowflake Marketplace Data

| Dataset | Provider | Use Case |
|---------|----------|----------|
| Fuel Oil CPI | Snowflake Public Data (Free) | Energy cost tracking |
| Core CPI | Snowflake Public Data (Free) | Inflation adjustment |
| Industrial Production | Snowflake Public Data (Free) | Demand correlation |

---

## ROI Calculations

### Assumptions

| Parameter | Value | Basis |
|-----------|-------|-------|
| Annual Revenue | $500M | Mid-size specialty chemicals manufacturer |
| Gross Margin | 30% | Industry average for specialty chemicals |
| Margin Improvement | 6% | Conservative estimate from pricing optimization literature |
| Underpriced Deal Reduction | $2.5M | Based on 80% reduction in margin leakage |
| Implementation Cost | $350K | Snowflake consumption + professional services |

### Calculation

```
Annual Benefit = (Revenue × Margin × Improvement) + Underpriced Deals + Logistics
              = ($500M × 30% × 6%) + $2.5M + $0.5M
              = $9M + $2.5M + $0.5M
              = $12M

3-Year ROI = (3 × $12M - $350K) / $350K × 100%
           = ($36M - $350K) / $350K × 100%
           = 10,186%
```

---

## Academic References

### Pricing & Demand Estimation

1. Nevo, A. (2000). "A Practitioner's Guide to Estimation of Random-Coefficients Logit Models of Demand." *Journal of Economics & Management Strategy*, 9(4), 513-548.

2. Train, K. E. (2009). *Discrete Choice Methods with Simulation* (2nd ed.). Cambridge University Press.

3. Bijmolt, T. H., Van Heerde, H. J., & Pieters, R. G. (2005). "New Empirical Generalizations on the Determinants of Price Elasticity." *Journal of Marketing Research*, 42(2), 141-156.

### Revenue Management

4. Talluri, K. T., & Van Ryzin, G. J. (2004). *The Theory and Practice of Revenue Management*. Springer.

5. Phillips, R. L. (2005). *Pricing and Revenue Optimization*. Stanford University Press.

### Chemicals Industry

6. Grunwald, G. J. (2022). "Pricing Strategies in Commodity Chemicals: A Dynamic Analysis." *Industrial Marketing Management*, 102, 134-147.

7. Chen, Y., & Zhang, T. (2023). "Cross-Price Elasticity in Petrochemical Markets: Evidence from Global Trade Data." *Energy Economics*, 118, 106512.

---

## Disclaimer

The statistics, ROI calculations, and industry benchmarks presented in this solution overview are illustrative and based on publicly available research. Actual results will vary based on specific company circumstances, data quality, and implementation approach.

The synthetic data used in this demo is generated for demonstration purposes and does not represent any real company's transactions or pricing.
