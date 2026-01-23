# =============================================================================
# 04 - Optimal Pricing Engine
# =============================================================================
# Snowflake Notebook for Chemicals Pricing Solution
# 
# This notebook implements constrained price optimization:
# 1. Load elasticity matrix from SUR model
# 2. Define optimization problem
# 3. Solve with constraints
# 4. Analyze results and sensitivity
# =============================================================================

# %% [markdown]
# # Optimal Pricing Engine
# 
# **Objective**: Maximize total contribution margin across product portfolio
# 
# **Using**: Cross-price elasticity matrix from SUR model

# %% Import libraries
import pandas as pd
import numpy as np
from scipy.optimize import minimize, differential_evolution
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

# Snowflake connection
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# %% [markdown]
# ## 1. Load Data

# %% Load elasticity matrix
query_elasticity = """
SELECT 
    demand_product_id,
    price_product_id,
    elasticity,
    is_own_price,
    is_significant
FROM CHEMICALS_DB.CHEMICAL_OPS.ELASTICITY_MATRIX
WHERE model_version = (
    SELECT MAX(model_version) 
    FROM CHEMICALS_DB.CHEMICAL_OPS.ELASTICITY_MATRIX
)
"""

elast_df = session.sql(query_elasticity).to_pandas()
print(f"Loaded {len(elast_df)} elasticity pairs")

# Pivot to matrix form
products = elast_df['DEMAND_PRODUCT_ID'].unique()
E = pd.DataFrame(index=products, columns=products, dtype=float)

for _, row in elast_df.iterrows():
    E.loc[row['DEMAND_PRODUCT_ID'], row['PRICE_PRODUCT_ID']] = row['ELASTICITY']

print("\nElasticity Matrix:")
print(E.round(3))

# %% Load current prices, costs, and volumes
query_current = """
SELECT 
    product_family,
    AVG(avg_price_per_mt) as current_price,
    AVG(avg_variable_cost) as variable_cost,
    SUM(total_quantity_mt) as total_quantity,
    SUM(total_revenue) as total_revenue
FROM CHEMICALS_DB.CHEMICAL_OPS.ML_TRAINING_DATA
WHERE order_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY product_family
"""

current_df = session.sql(query_current).to_pandas()
current_df['PRODUCT_ID'] = current_df['PRODUCT_FAMILY'].str.replace(' ', '_').str.upper()
current_df = current_df.set_index('PRODUCT_ID')

print("\nCurrent State:")
print(current_df.round(2))

# %% [markdown]
# ## 2. Define Optimization Problem

# %% Demand function
def demand_function(P, Q0, E_matrix, P0):
    """
    Log-linear demand with cross-elasticities.
    
    Q_i(P) = Q0_i * exp(Σ_j E_ij * ln(P_j / P0_j))
    
    Args:
        P: Price vector (n,)
        Q0: Baseline quantity vector (n,)
        E_matrix: Elasticity matrix (n x n)
        P0: Reference price vector (n,)
    
    Returns:
        Q: Predicted quantity vector (n,)
    """
    log_price_ratio = np.log(P / P0)
    log_Q_change = E_matrix @ log_price_ratio
    Q = Q0 * np.exp(log_Q_change)
    return Q


def profit_function(P, Q0, C, E_matrix, P0):
    """
    Total contribution margin.
    
    Π(P) = Σ_i (P_i - C_i) * Q_i(P)
    """
    Q = demand_function(P, Q0, E_matrix, P0)
    profit = np.sum((P - C) * Q)
    return profit


def negative_profit(P, Q0, C, E_matrix, P0):
    """Negative profit for minimization."""
    return -profit_function(P, Q0, C, E_matrix, P0)

# %% [markdown]
# ## 3. Set Up Optimization

# %% Prepare vectors (align with elasticity matrix)
product_order = E.index.tolist()

P0 = np.array([current_df.loc[p, 'CURRENT_PRICE'] for p in product_order])
Q0 = np.array([current_df.loc[p, 'TOTAL_QUANTITY'] for p in product_order])
C = np.array([current_df.loc[p, 'VARIABLE_COST'] for p in product_order])
E_matrix = E.values.astype(float)

print("Optimization Setup:")
print(f"  Products: {product_order}")
print(f"  Current Prices: {P0.round(2)}")
print(f"  Variable Costs: {C.round(2)}")
print(f"  Baseline Quantities: {Q0.round(0)}")

# %% Define constraints
def create_constraints(P0, C, min_margin_pct=0.15, max_price_change_pct=0.10):
    """
    Create optimization constraints.
    
    1. Minimum margin: (P - C) / P >= min_margin_pct
    2. Price change limits: |P - P0| / P0 <= max_price_change_pct
    """
    constraints = []
    n = len(P0)
    
    # Margin constraints: P >= C / (1 - min_margin)
    for i in range(n):
        min_price = C[i] / (1 - min_margin_pct)
        constraints.append({
            'type': 'ineq',
            'fun': lambda P, i=i, mp=min_price: P[i] - mp
        })
    
    return constraints


def create_bounds(P0, C, max_price_change_pct=0.10, min_margin_pct=0.15):
    """
    Create price bounds.
    
    Lower bound: max(P0 * (1 - change), C / (1 - margin))
    Upper bound: P0 * (1 + change)
    """
    bounds = []
    for i in range(len(P0)):
        floor_from_change = P0[i] * (1 - max_price_change_pct)
        floor_from_margin = C[i] / (1 - min_margin_pct)
        lower = max(floor_from_change, floor_from_margin)
        upper = P0[i] * (1 + max_price_change_pct)
        bounds.append((lower, upper))
    
    return bounds

# %% [markdown]
# ## 4. Run Optimization

# %% Configuration
MIN_MARGIN_PCT = 0.15      # 15% minimum margin
MAX_PRICE_CHANGE = 0.10    # ±10% price change limit

# Create constraints and bounds
constraints = create_constraints(P0, C, MIN_MARGIN_PCT, MAX_PRICE_CHANGE)
bounds = create_bounds(P0, C, MAX_PRICE_CHANGE, MIN_MARGIN_PCT)

print("Constraints:")
print(f"  Min Margin: {MIN_MARGIN_PCT*100}%")
print(f"  Max Price Change: ±{MAX_PRICE_CHANGE*100}%")
print(f"\nBounds:")
for i, (p, (lb, ub)) in enumerate(zip(product_order, bounds)):
    print(f"  {p}: ${lb:.2f} - ${ub:.2f}")

# %% Run optimization
result = minimize(
    negative_profit,
    x0=P0,
    args=(Q0, C, E_matrix, P0),
    method='SLSQP',
    bounds=bounds,
    constraints=constraints,
    options={'maxiter': 1000, 'ftol': 1e-9}
)

print(f"\nOptimization Status: {'SUCCESS' if result.success else 'FAILED'}")
print(f"Iterations: {result.nit}")
print(f"Message: {result.message}")

# %% [markdown]
# ## 5. Analyze Results

# %% Extract results
P_optimal = result.x
Q_optimal = demand_function(P_optimal, Q0, E_matrix, P0)

# Current metrics
current_profit = profit_function(P0, Q0, C, E_matrix, P0)
current_revenue = np.sum(P0 * Q0)

# Optimal metrics
optimal_profit = profit_function(P_optimal, Q0, C, E_matrix, P0)
optimal_revenue = np.sum(P_optimal * Q_optimal)

# Build results dataframe
results_df = pd.DataFrame({
    'Product': product_order,
    'Current_Price': P0,
    'Optimal_Price': P_optimal,
    'Price_Change_$': P_optimal - P0,
    'Price_Change_%': (P_optimal - P0) / P0 * 100,
    'Current_Qty': Q0,
    'Predicted_Qty': Q_optimal,
    'Qty_Change_%': (Q_optimal - Q0) / Q0 * 100,
    'Variable_Cost': C,
    'Current_Margin': (P0 - C) * Q0,
    'Optimal_Margin': (P_optimal - C) * Q_optimal,
    'Margin_Change_%': ((P_optimal - C) * Q_optimal - (P0 - C) * Q0) / ((P0 - C) * Q0) * 100
})

print("\n" + "="*80)
print("OPTIMIZATION RESULTS")
print("="*80)
print(results_df.round(2).to_string(index=False))

print(f"\n{'='*80}")
print("PORTFOLIO SUMMARY")
print("="*80)
print(f"  Current Total Profit:  ${current_profit:,.0f}")
print(f"  Optimal Total Profit:  ${optimal_profit:,.0f}")
print(f"  Profit Improvement:    ${optimal_profit - current_profit:,.0f} ({(optimal_profit/current_profit - 1)*100:.1f}%)")
print(f"\n  Current Total Revenue: ${current_revenue:,.0f}")
print(f"  Optimal Total Revenue: ${optimal_revenue:,.0f}")
print(f"  Revenue Change:        ${optimal_revenue - current_revenue:,.0f} ({(optimal_revenue/current_revenue - 1)*100:.1f}%)")

# %% [markdown]
# ## 6. Identify Binding Constraints

# %% Check which constraints are binding
print("\n" + "="*80)
print("BINDING CONSTRAINTS ANALYSIS")
print("="*80)

for i, prod in enumerate(product_order):
    lb, ub = bounds[i]
    p = P_optimal[i]
    margin = (p - C[i]) / p
    
    binding = []
    if abs(p - lb) < 0.01:
        if lb == C[i] / (1 - MIN_MARGIN_PCT):
            binding.append("MIN_MARGIN")
        else:
            binding.append("PRICE_FLOOR")
    if abs(p - ub) < 0.01:
        binding.append("PRICE_CEILING")
    
    status = ", ".join(binding) if binding else "NOT_BINDING"
    print(f"  {prod:15s}: ${p:.2f} | Margin: {margin*100:.1f}% | Status: {status}")

# %% [markdown]
# ## 7. Visualization

# %% 7.1 Price Changes Bar Chart
fig = go.Figure()

fig.add_trace(go.Bar(
    x=results_df['Product'],
    y=results_df['Price_Change_%'],
    marker_color=['#22c55e' if x >= 0 else '#ef4444' for x in results_df['Price_Change_%']],
    text=[f"{x:+.1f}%" for x in results_df['Price_Change_%']],
    textposition='outside'
))

fig.update_layout(
    title='Optimal Price Changes by Product',
    xaxis_title='Product Family',
    yaxis_title='Price Change (%)',
    template='plotly_dark',
    height=400,
    yaxis=dict(range=[-15, 15])
)

fig.add_hline(y=MAX_PRICE_CHANGE*100, line_dash="dash", line_color="yellow", 
              annotation_text=f"Max +{MAX_PRICE_CHANGE*100}%")
fig.add_hline(y=-MAX_PRICE_CHANGE*100, line_dash="dash", line_color="yellow",
              annotation_text=f"Max -{MAX_PRICE_CHANGE*100}%")

fig.show()

# %% 7.2 Margin Impact Waterfall
fig = go.Figure(go.Waterfall(
    x=product_order + ['Total'],
    y=list(results_df['Optimal_Margin'] - results_df['Current_Margin']) + [optimal_profit - current_profit],
    measure=['relative'] * len(product_order) + ['total'],
    text=[f"${x:,.0f}" for x in list(results_df['Optimal_Margin'] - results_df['Current_Margin']) + [optimal_profit - current_profit]],
    textposition='outside',
    connector={"line": {"color": "rgba(255,255,255,0.3)"}},
    increasing={"marker": {"color": "#22c55e"}},
    decreasing={"marker": {"color": "#ef4444"}},
    totals={"marker": {"color": "#3b82f6"}}
))

fig.update_layout(
    title='Margin Impact by Product',
    xaxis_title='Product',
    yaxis_title='Margin Change ($)',
    template='plotly_dark',
    height=400
)

fig.show()

# %% 7.3 Current vs Optimal Comparison
fig = make_subplots(rows=1, cols=2, subplot_titles=['Price Comparison', 'Margin Comparison'])

fig.add_trace(go.Bar(name='Current', x=results_df['Product'], y=results_df['Current_Price'], 
                     marker_color='#6366f1'), row=1, col=1)
fig.add_trace(go.Bar(name='Optimal', x=results_df['Product'], y=results_df['Optimal_Price'],
                     marker_color='#22c55e'), row=1, col=1)

fig.add_trace(go.Bar(name='Current', x=results_df['Product'], y=results_df['Current_Margin'],
                     marker_color='#6366f1', showlegend=False), row=1, col=2)
fig.add_trace(go.Bar(name='Optimal', x=results_df['Product'], y=results_df['Optimal_Margin'],
                     marker_color='#22c55e', showlegend=False), row=1, col=2)

fig.update_layout(template='plotly_dark', height=400, barmode='group',
                  title='Current vs Optimal: Price and Margin')
fig.show()

# %% [markdown]
# ## 8. Sensitivity Analysis

# %% Vary constraints and see impact
sensitivity_results = []

for margin_pct in [0.10, 0.15, 0.20, 0.25]:
    for change_pct in [0.05, 0.10, 0.15, 0.20]:
        bounds_sens = create_bounds(P0, C, change_pct, margin_pct)
        constraints_sens = create_constraints(P0, C, margin_pct, change_pct)
        
        try:
            res = minimize(negative_profit, x0=P0, args=(Q0, C, E_matrix, P0),
                          method='SLSQP', bounds=bounds_sens, constraints=constraints_sens)
            if res.success:
                profit = -res.fun
                sensitivity_results.append({
                    'Min_Margin_%': margin_pct * 100,
                    'Max_Change_%': change_pct * 100,
                    'Optimal_Profit': profit,
                    'Profit_vs_Current': (profit / current_profit - 1) * 100
                })
        except:
            pass

sens_df = pd.DataFrame(sensitivity_results)
print("\nSensitivity Analysis:")
print(sens_df.round(2).to_string(index=False))

# %% Sensitivity heatmap
sens_pivot = sens_df.pivot(index='Min_Margin_%', columns='Max_Change_%', values='Profit_vs_Current')

fig = go.Figure(data=go.Heatmap(
    z=sens_pivot.values,
    x=sens_pivot.columns,
    y=sens_pivot.index,
    colorscale='Greens',
    text=[[f"{v:.1f}%" for v in row] for row in sens_pivot.values],
    texttemplate='%{text}',
    colorbar=dict(title='Profit Improvement %')
))

fig.update_layout(
    title='Profit Improvement (%) by Constraint Settings',
    xaxis_title='Max Price Change (%)',
    yaxis_title='Min Margin (%)',
    template='plotly_dark',
    height=400
)

fig.show()

# %% [markdown]
# ## 9. Save Results to Snowflake

# %% Save optimization results
scenario_id = f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Prepare results for saving
save_results = []
for i, prod in enumerate(product_order):
    save_results.append({
        'SCENARIO_ID': scenario_id,
        'RUN_DATE': datetime.now(),
        'PRODUCT_ID': prod,
        'PRODUCT_NAME': prod.replace('_', ' ').title(),
        'REGION': 'ALL',
        'CURRENT_PRICE': float(P0[i]),
        'OPTIMAL_PRICE': float(P_optimal[i]),
        'PRICE_CHANGE_DOLLARS': float(P_optimal[i] - P0[i]),
        'PRICE_CHANGE_PCT': float((P_optimal[i] - P0[i]) / P0[i] * 100),
        'CURRENT_VOLUME': float(Q0[i]),
        'PREDICTED_VOLUME': float(Q_optimal[i]),
        'CURRENT_MARGIN': float((P0[i] - C[i]) * Q0[i]),
        'PREDICTED_MARGIN': float((P_optimal[i] - C[i]) * Q_optimal[i]),
        'IS_AT_FLOOR': abs(P_optimal[i] - bounds[i][0]) < 0.01,
        'IS_AT_CEILING': abs(P_optimal[i] - bounds[i][1]) < 0.01,
        'IS_MARGIN_BINDING': abs((P_optimal[i] - C[i]) / P_optimal[i] - MIN_MARGIN_PCT) < 0.01
    })

results_save_df = pd.DataFrame(save_results)

sp_df = session.create_dataframe(results_save_df)
sp_df.write.mode("append").save_as_table("CHEMICALS_DB.CHEMICAL_OPS.OPTIMAL_PRICING")
print(f"Saved optimization results with scenario_id: {scenario_id}")

# %% [markdown]
# ## Summary
# 
# **Optimization Results:**
# - Identified optimal price vector that maximizes total contribution margin
# - Respects all constraints (margin floors, price change limits)
# - Quantified expected profit improvement
# 
# **Key Insights:**
# - Cross-elasticity effects mean raising one product's price can benefit substitutes
# - Binding constraints identify where flexibility would add value
# - Sensitivity analysis shows trade-off between constraints and profit
# 
# **Next Steps:**
# - Integrate into Streamlit dashboard for interactive optimization
# - Add scenario comparison functionality
# - Enable regional optimization
