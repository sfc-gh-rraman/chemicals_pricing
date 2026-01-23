# =============================================================================
# CHEMICALS PRICING & SUPPLY CHAIN: ML WORKBOOK
# =============================================================================
# Comprehensive End-to-End Machine Learning Pipeline
# 
# This notebook implements a complete econometric analysis for optimal
# pricing in the chemicals industry, including:
#
#   1. Data Preparation & Feature Engineering
#   2. Exploratory Data Analysis
#   3. Linear Regression (Baseline Elasticity)
#   4. Seemingly Unrelated Regressions (Cross-Elasticity Matrix)
#   5. Optimal Pricing Engine (Constrained Optimization)
#   6. Business Insights & Recommendations
#
# Author: Chemicals Pricing Team
# Platform: Snowflake Notebooks
# =============================================================================

# %% [markdown]
# # 🧪 Chemicals Pricing Intelligence
# ## Machine Learning Workbook for Price Elasticity & Optimization
# 
# ---
# 
# ### Executive Summary
# 
# This workbook develops a **data-driven pricing optimization system** for a 
# chemicals manufacturer. We estimate **price elasticities** using econometric
# models, then use these estimates to find **profit-maximizing prices** subject
# to business constraints.
# 
# **Key Deliverables:**
# 1. Own-price elasticity for each product (how demand responds to own price)
# 2. Cross-price elasticity matrix (substitution patterns between products)
# 3. Optimal price recommendations that maximize portfolio profit
# 
# ---
# 
# ### Business Context
# 
# The chemicals industry faces several pricing challenges:
# 
# | Challenge | Impact |
# |-----------|--------|
# | **Commodity-like products** | Price sensitivity varies by product family |
# | **Feedstock volatility** | Costs fluctuate with crude oil and natural gas |
# | **Product substitution** | Customers switch between polymers based on price |
# | **Margin pressure** | Need to optimize portfolio, not just individual products |
# 
# Traditional pricing approaches (cost-plus, competitive matching) leave money
# on the table. **Elasticity-based pricing** captures demand dynamics and enables
# true portfolio optimization.

# %% [markdown]
# ---
# # Part 1: Setup & Data Preparation
# ---

# %% Import Libraries
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS
from scipy.optimize import minimize
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Snowflake Session
from snowflake.snowpark.context import get_active_session
session = get_active_session()

print("✅ Libraries loaded successfully")
print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# %% [markdown]
# ## 1.1 Data Architecture
# 
# Our data flows through a **three-tier architecture**:
# 
# ```
# ┌─────────────────────────────────────────────────────────────────┐
# │                        DATA ARCHITECTURE                        │
# ├─────────────────────────────────────────────────────────────────┤
# │                                                                 │
# │  ┌─────────┐     ┌──────────┐     ┌─────────────────────────┐  │
# │  │   RAW   │ ──▶ │  ATOMIC  │ ──▶ │     CHEMICAL_OPS       │  │
# │  │ Schema  │     │  Schema  │     │     (Data Mart)        │  │
# │  └─────────┘     └──────────┘     └─────────────────────────┘  │
# │       │               │                      │                 │
# │  • Market data   • Product      • MARGIN_ANALYZER              │
# │  • ERP extracts  • Customer     • COST_TO_SERVE                │
# │  • Plant costs   • Sales Order  • ML_TRAINING_DATA             │
# │                  • Inventory    • ELASTICITY_MATRIX            │
# │                                                                 │
# │  ┌─────────────────────────────────────────────────────────┐   │
# │  │              MARKETPLACE INTEGRATION                     │   │
# │  │  • Fuel Oil CPI  • Core CPI  • Industrial Production    │   │
# │  └─────────────────────────────────────────────────────────┘   │
# └─────────────────────────────────────────────────────────────────┘
# ```

# %% Load Training Data
query = """
SELECT *
FROM CHEMICALS_DB.CHEMICAL_OPS.ML_TRAINING_DATA
ORDER BY order_date, product_id
"""

df = session.sql(query).to_pandas()

print(f"📊 Dataset Overview")
print(f"   • Rows: {len(df):,}")
print(f"   • Date Range: {df['ORDER_DATE'].min()} to {df['ORDER_DATE'].max()}")
print(f"   • Products: {df['PRODUCT_ID'].nunique()}")
print(f"   • Product Families: {df['PRODUCT_FAMILY'].nunique()}")
print(f"   • Regions: {df['REGION'].nunique()}")

# %% [markdown]
# ## 1.2 Feature Engineering
# 
# Our training data includes carefully engineered features:
# 
# | Feature Category | Variables | Purpose |
# |-----------------|-----------|---------|
# | **Price** | `ln_price`, `price_avg_7d`, `price_avg_30d` | Capture price levels and trends |
# | **Quantity** | `ln_quantity`, `total_quantity_mt` | Log-transform for elasticity |
# | **Costs** | `avg_variable_cost`, `margin_pct` | Profitability context |
# | **Market** | `fuel_oil_cpi`, `core_cpi`, `industrial_production` | Macro conditions |
# | **Temporal** | `month`, `quarter`, `is_q4`, `is_winter` | Seasonality |
# 
# ### Why Log-Transform?
# 
# The log-log specification gives us **constant elasticity**:
# 
# $$\ln(Q) = \alpha + \beta \cdot \ln(P) + \gamma \cdot X + \varepsilon$$
# 
# The coefficient $\beta$ directly interprets as elasticity:
# 
# $$\varepsilon = \frac{\partial \ln Q}{\partial \ln P} = \frac{\partial Q / Q}{\partial P / P} = \beta$$
# 
# This means: **A 1% increase in price leads to a β% change in quantity.**

# %% Data Quality Check
print("📋 Data Quality Report")
print("=" * 50)

# Missing values
missing = df.isnull().sum()
missing_pct = (missing / len(df) * 100)
quality_report = pd.DataFrame({
    'Missing': missing,
    'Missing %': missing_pct.round(2)
})
print("\nColumns with missing values:")
print(quality_report[quality_report['Missing'] > 0].to_string())

# Key statistics
print("\n📈 Key Variable Statistics:")
key_cols = ['LN_QUANTITY', 'LN_PRICE', 'MARGIN_PCT', 'FUEL_OIL_CPI']
print(df[key_cols].describe().round(3).to_string())

# %% [markdown]
# ---
# # Part 2: Exploratory Data Analysis
# ---
# 
# Before modeling, we explore the data to understand:
# 1. Price-quantity relationships (visual check for negative correlation)
# 2. Product family differences
# 3. Temporal patterns
# 4. Market indicator correlations

# %% 2.1 Price-Quantity Scatter by Product Family
fig = px.scatter(
    df.dropna(subset=['LN_PRICE', 'LN_QUANTITY']),
    x='LN_PRICE',
    y='LN_QUANTITY',
    color='PRODUCT_FAMILY',
    opacity=0.4,
    trendline='ols',
    title='<b>Price-Quantity Relationship by Product Family</b><br><sup>Log-log scale • Negative slope indicates elastic demand</sup>',
    labels={'LN_PRICE': 'Log(Price per MT)', 'LN_QUANTITY': 'Log(Quantity MT)'}
)

fig.update_layout(
    template='plotly_dark',
    height=500,
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)
fig.show()

# %% [markdown]
# ### Interpretation
# 
# The scatter plot shows **negative correlation** between price and quantity
# across all product families, as expected. The slope of each trendline
# approximates the own-price elasticity.
# 
# Key observations:
# - **Olefins** (steep slope): Most price-sensitive
# - **Polymers** (flatter slope): Less price-sensitive, more essential
# - **Aromatics/Intermediates**: Moderate sensitivity

# %% 2.2 Price Trends Over Time
price_ts = df.groupby(['ORDER_DATE', 'PRODUCT_FAMILY'])['AVG_PRICE_PER_MT'].mean().reset_index()

fig = px.line(
    price_ts,
    x='ORDER_DATE',
    y='AVG_PRICE_PER_MT',
    color='PRODUCT_FAMILY',
    title='<b>Price Trends by Product Family</b><br><sup>Daily average price per metric ton</sup>',
    labels={'ORDER_DATE': 'Date', 'AVG_PRICE_PER_MT': 'Price ($/MT)'}
)

fig.update_layout(template='plotly_dark', height=400)
fig.show()

# %% 2.3 Correlation Analysis
corr_cols = ['LN_QUANTITY', 'LN_PRICE', 'FUEL_OIL_CPI', 'CORE_CPI', 
             'INDUSTRIAL_PRODUCTION', 'MARGIN_PCT']
corr_labels = ['Quantity', 'Price', 'Fuel CPI', 'Core CPI', 'Ind. Prod.', 'Margin %']

corr_matrix = df[corr_cols].corr()

fig = go.Figure(data=go.Heatmap(
    z=corr_matrix.values,
    x=corr_labels,
    y=corr_labels,
    colorscale='RdBu',
    zmid=0,
    text=corr_matrix.round(2).values,
    texttemplate='%{text}',
    textfont={"size": 12},
    colorbar=dict(title='ρ')
))

fig.update_layout(
    title='<b>Correlation Matrix</b><br><sup>Key variables for elasticity estimation</sup>',
    template='plotly_dark',
    height=450,
    width=550
)
fig.show()

# %% [markdown]
# ### Key Correlations
# 
# | Pair | Correlation | Interpretation |
# |------|-------------|----------------|
# | Price ↔ Quantity | **Negative** | Law of demand holds |
# | Fuel CPI ↔ Price | Positive | Cost pass-through |
# | Ind. Prod. ↔ Quantity | Positive | Demand linked to manufacturing activity |
# | Margin ↔ Price | Positive | Higher prices improve margins |

# %% 2.4 Distribution by Product Family
fig = make_subplots(rows=1, cols=2, 
                    subplot_titles=['Price Distribution', 'Margin Distribution'])

for i, family in enumerate(df['PRODUCT_FAMILY'].unique()):
    family_data = df[df['PRODUCT_FAMILY'] == family]
    
    fig.add_trace(
        go.Box(y=family_data['AVG_PRICE_PER_MT'], name=family, 
               marker_color=px.colors.qualitative.Set2[i]),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Box(y=family_data['MARGIN_PCT'] * 100, name=family, showlegend=False,
               marker_color=px.colors.qualitative.Set2[i]),
        row=1, col=2
    )

fig.update_layout(template='plotly_dark', height=400, showlegend=True,
                  title='<b>Price & Margin Distribution by Product Family</b>')
fig.update_yaxes(title_text='$/MT', row=1, col=1)
fig.update_yaxes(title_text='Margin %', row=1, col=2)
fig.show()

# %% [markdown]
# ---
# # Part 3: Linear Regression - Baseline Elasticity
# ---
# 
# ## 3.1 Model Specification
# 
# We estimate **own-price elasticity** for each product using OLS:
# 
# $$\ln(Q_{it}) = \alpha + \beta_1 \ln(P_{it}) + \beta_2 \cdot CPI_t + \beta_3 \cdot IP_t + \sum_{m=2}^{12} \gamma_m \cdot Month_m + \varepsilon_{it}$$
# 
# Where:
# - $Q_{it}$ = Quantity sold for product $i$ at time $t$
# - $P_{it}$ = Price per metric ton
# - $CPI_t$ = Fuel Oil Consumer Price Index (energy cost proxy)
# - $IP_t$ = Industrial Production Index (demand driver)
# - $Month_m$ = Monthly dummy variables (seasonality)
# 
# **Interpretation of $\beta_1$:**
# - If $\beta_1 = -1.5$: A 1% price increase → 1.5% quantity decrease
# - If $|\beta_1| > 1$: **Elastic** demand (revenue decreases with price increase)
# - If $|\beta_1| < 1$: **Inelastic** demand (revenue increases with price increase)

# %% 3.2 Estimation Function
def estimate_product_elasticity(data, product_id):
    """
    Estimate own-price elasticity for a single product.
    
    Returns dict with coefficients and diagnostics, or None if insufficient data.
    """
    product_data = data[data['PRODUCT_ID'] == product_id].copy()
    
    if len(product_data) < 50:
        return None
    
    # Dependent variable
    y = product_data['LN_QUANTITY']
    
    # Build feature matrix
    X = pd.DataFrame({'const': 1, 'ln_price': product_data['LN_PRICE']})
    
    # Add CPI control (standardized)
    if product_data['FUEL_OIL_CPI'].notna().sum() > len(product_data) * 0.5:
        cpi = product_data['FUEL_OIL_CPI'].fillna(product_data['FUEL_OIL_CPI'].mean())
        X['fuel_cpi'] = (cpi - cpi.mean()) / cpi.std()
    
    # Add Industrial Production control
    if product_data['INDUSTRIAL_PRODUCTION'].notna().sum() > len(product_data) * 0.5:
        ip = product_data['INDUSTRIAL_PRODUCTION'].fillna(product_data['INDUSTRIAL_PRODUCTION'].mean())
        X['ind_prod'] = (ip - ip.mean()) / ip.std()
    
    # Month dummies for seasonality
    month_dummies = pd.get_dummies(product_data['MONTH'], prefix='m', drop_first=True)
    X = pd.concat([X.reset_index(drop=True), month_dummies.reset_index(drop=True)], axis=1)
    
    # Handle missing values
    mask = ~(X.isna().any(axis=1) | y.reset_index(drop=True).isna())
    X_clean = X[mask.values]
    y_clean = y.reset_index(drop=True)[mask.values]
    
    if len(y_clean) < 50:
        return None
    
    # Fit OLS
    try:
        model = OLS(y_clean, X_clean).fit()
        
        return {
            'product_id': product_id,
            'product_name': product_data['PRODUCT_NAME'].iloc[0],
            'product_family': product_data['PRODUCT_FAMILY'].iloc[0],
            'elasticity': model.params['ln_price'],
            'std_error': model.bse['ln_price'],
            't_stat': model.tvalues['ln_price'],
            'p_value': model.pvalues['ln_price'],
            'r_squared': model.rsquared,
            'adj_r_squared': model.rsquared_adj,
            'n_obs': int(model.nobs),
            'cpi_coef': model.params.get('fuel_cpi', np.nan),
            'ip_coef': model.params.get('ind_prod', np.nan)
        }
    except Exception as e:
        print(f"  ⚠️ Error for {product_id}: {e}")
        return None

# %% 3.3 Run Estimation
print("🔄 Estimating elasticities for all products...")
print("=" * 60)

products = df['PRODUCT_ID'].unique()
results = []

for product_id in products:
    result = estimate_product_elasticity(df, product_id)
    if result:
        results.append(result)
        sig = '***' if result['p_value'] < 0.01 else '**' if result['p_value'] < 0.05 else '*' if result['p_value'] < 0.1 else ''
        print(f"  {result['product_name']:30s} ε = {result['elasticity']:+.3f} {sig:3s} (R²={result['r_squared']:.3f})")

elasticity_df = pd.DataFrame(results)
print(f"\n✅ Successfully estimated {len(elasticity_df)} products")

# %% [markdown]
# ## 3.4 Results Visualization

# %% Elasticity Bar Chart
fig = px.bar(
    elasticity_df.sort_values('elasticity'),
    x='elasticity',
    y='product_name',
    color='product_family',
    orientation='h',
    title='<b>Own-Price Elasticity by Product</b><br><sup>More negative = more price-sensitive</sup>',
    labels={'elasticity': 'Price Elasticity (ε)', 'product_name': ''},
    color_discrete_sequence=px.colors.qualitative.Set2
)

fig.add_vline(x=-1, line_dash="dash", line_color="white", 
              annotation_text="Unit Elastic (ε=-1)")
fig.add_vline(x=0, line_color="red", line_width=2)

fig.update_layout(
    template='plotly_dark',
    height=600,
    legend=dict(orientation='h', yanchor='bottom', y=1.02)
)
fig.show()

# %% [markdown]
# ## 3.5 Model Diagnostics
# 
# ### Statistical Quality Check

# %% Diagnostics Summary
n_negative = (elasticity_df['elasticity'] < 0).sum()
n_significant = (elasticity_df['p_value'] < 0.05).sum()
avg_r2 = elasticity_df['r_squared'].mean()

print("📊 MODEL DIAGNOSTICS SUMMARY")
print("=" * 50)
print(f"  Products estimated:           {len(elasticity_df)}")
print(f"  Negative elasticity (correct): {n_negative}/{len(elasticity_df)} ({n_negative/len(elasticity_df)*100:.0f}%)")
print(f"  Statistically significant:     {n_significant}/{len(elasticity_df)} ({n_significant/len(elasticity_df)*100:.0f}%)")
print(f"  Average R²:                    {avg_r2:.3f}")
print(f"  Elasticity range:              [{elasticity_df['elasticity'].min():.2f}, {elasticity_df['elasticity'].max():.2f}]")

# %% R² vs Elasticity
fig = px.scatter(
    elasticity_df,
    x='r_squared',
    y='elasticity',
    color='product_family',
    size='n_obs',
    hover_data=['product_name'],
    title='<b>Model Fit vs Elasticity Estimate</b><br><sup>Size = sample size</sup>',
    labels={'r_squared': 'R-squared', 'elasticity': 'Price Elasticity'}
)

fig.add_hline(y=-1, line_dash="dash", line_color="yellow")
fig.update_layout(template='plotly_dark', height=450)
fig.show()

# %% [markdown]
# ---
# # Part 4: SUR Model - Cross-Price Elasticity Matrix
# ---
# 
# ## 4.1 Why SUR?
# 
# Linear regression estimates **own-price elasticity** only. But in reality:
# 
# > *"When Polyethylene prices rise, some customers switch to Polypropylene"*
# 
# This **substitution effect** is captured by **cross-price elasticity**.
# 
# ### Seemingly Unrelated Regressions (SUR)
# 
# SUR estimates a **system of equations** where error terms are correlated:
# 
# $$
# \begin{aligned}
# \ln(Q_1) &= \alpha_1 + \beta_{11} \ln(P_1) + \beta_{12} \ln(P_2) + \cdots + \varepsilon_1 \\
# \ln(Q_2) &= \alpha_2 + \beta_{21} \ln(P_1) + \beta_{22} \ln(P_2) + \cdots + \varepsilon_2 \\
# &\vdots \\
# \ln(Q_n) &= \alpha_n + \beta_{n1} \ln(P_1) + \beta_{n2} \ln(P_2) + \cdots + \varepsilon_n
# \end{aligned}
# $$
# 
# Where $\text{Cov}(\varepsilon_i, \varepsilon_j) \neq 0$ for $i \neq j$.
# 
# ### The Elasticity Matrix
# 
# The matrix $\mathbf{E} = [\beta_{ij}]$ captures all price effects:
# 
# $$
# \mathbf{E} = \begin{bmatrix}
# \varepsilon_{11} & \varepsilon_{12} & \cdots & \varepsilon_{1n} \\
# \varepsilon_{21} & \varepsilon_{22} & \cdots & \varepsilon_{2n} \\
# \vdots & \vdots & \ddots & \vdots \\
# \varepsilon_{n1} & \varepsilon_{n2} & \cdots & \varepsilon_{nn}
# \end{bmatrix}
# $$
# 
# - **Diagonal** ($\varepsilon_{ii}$): Own-price elasticity (negative)
# - **Off-diagonal positive** ($\varepsilon_{ij} > 0$): Products $i$ and $j$ are **substitutes**
# - **Off-diagonal negative** ($\varepsilon_{ij} < 0$): Products are **complements**

# %% 4.2 Prepare Data for SUR
# Aggregate to product family level for cleaner estimation

query_family = """
WITH family_agg AS (
    SELECT 
        order_date,
        region,
        product_family,
        SUM(total_quantity_mt) as quantity,
        AVG(avg_price_per_mt) as price,
        AVG(fuel_oil_cpi) as fuel_cpi,
        AVG(industrial_production) as ip
    FROM CHEMICALS_DB.CHEMICAL_OPS.ML_TRAINING_DATA
    WHERE ln_quantity IS NOT NULL
    GROUP BY order_date, region, product_family
)
SELECT * FROM family_agg
ORDER BY order_date, region, product_family
"""

family_df = session.sql(query_family).to_pandas()
print(f"📊 Family-level data: {len(family_df):,} observations")
print(f"   Families: {family_df['PRODUCT_FAMILY'].unique().tolist()}")

# %% Create wide format for SUR
# Pivot to get all prices and quantities in each row

qty_wide = family_df.pivot_table(
    index=['ORDER_DATE', 'REGION'],
    columns='PRODUCT_FAMILY',
    values='QUANTITY',
    aggfunc='sum'
).reset_index()

price_wide = family_df.pivot_table(
    index=['ORDER_DATE', 'REGION'],
    columns='PRODUCT_FAMILY',
    values='PRICE',
    aggfunc='mean'
).reset_index()

controls = family_df.groupby(['ORDER_DATE', 'REGION']).agg({
    'FUEL_CPI': 'mean',
    'IP': 'mean'
}).reset_index()

# Merge
sur_data = qty_wide.merge(price_wide, on=['ORDER_DATE', 'REGION'], suffixes=('_Q', '_P'))
sur_data = sur_data.merge(controls, on=['ORDER_DATE', 'REGION'])

# Create log transforms
families = family_df['PRODUCT_FAMILY'].unique().tolist()
family_names = [f.replace(' ', '_').upper() for f in families]

for i, (fam, clean) in enumerate(zip(families, family_names)):
    sur_data[f'LN_Q_{clean}'] = np.log(sur_data[f'{fam}_Q'].clip(lower=0.01))
    sur_data[f'LN_P_{clean}'] = np.log(sur_data[f'{fam}_P'].clip(lower=0.01))

# Standardize controls
sur_data['CPI_STD'] = (sur_data['FUEL_CPI'] - sur_data['FUEL_CPI'].mean()) / sur_data['FUEL_CPI'].std()
sur_data['IP_STD'] = (sur_data['IP'] - sur_data['IP'].mean()) / sur_data['IP'].std()

sur_data = sur_data.dropna()
print(f"✅ SUR data prepared: {sur_data.shape}")

# %% 4.3 Estimate SUR System
print("🔄 Estimating SUR system...")
print("=" * 60)

price_cols = [f'LN_P_{f}' for f in family_names]
sur_results = {}

for fam in family_names:
    y = sur_data[f'LN_Q_{fam}']
    X = sur_data[price_cols + ['CPI_STD', 'IP_STD']].copy()
    X = sm.add_constant(X)
    
    model = OLS(y, X).fit()
    sur_results[fam] = model
    
    print(f"\n📊 {fam}")
    print(f"   R² = {model.rsquared:.4f} | N = {int(model.nobs)}")
    
    for col in model.params.index:
        if 'LN_P_' in col:
            p_fam = col.replace('LN_P_', '')
            coef = model.params[col]
            pval = model.pvalues[col]
            sig = '***' if pval < 0.01 else '**' if pval < 0.05 else '*' if pval < 0.1 else ''
            own = "← OWN" if p_fam == fam else ""
            print(f"   {p_fam:15s}: {coef:+.4f} {sig:3s} {own}")

# %% 4.4 Build Elasticity Matrix
E_matrix = pd.DataFrame(index=family_names, columns=family_names, dtype=float)
P_matrix = pd.DataFrame(index=family_names, columns=family_names, dtype=float)

for demand_fam in family_names:
    model = sur_results[demand_fam]
    for price_fam in family_names:
        col = f'LN_P_{price_fam}'
        if col in model.params.index:
            E_matrix.loc[demand_fam, price_fam] = model.params[col]
            P_matrix.loc[demand_fam, price_fam] = model.pvalues[col]

print("\n📊 CROSS-PRICE ELASTICITY MATRIX")
print("=" * 50)
print(E_matrix.round(3).to_string())

# %% 4.5 Visualize Elasticity Matrix
fig = go.Figure(data=go.Heatmap(
    z=E_matrix.values.astype(float),
    x=[f.replace('_', ' ').title() for f in E_matrix.columns],
    y=[f.replace('_', ' ').title() for f in E_matrix.index],
    colorscale='RdBu',
    zmid=0,
    text=E_matrix.round(2).values,
    texttemplate='%{text}',
    textfont={"size": 16, "color": "white"},
    hoverongaps=False,
    colorbar=dict(title='ε', titleside='right')
))

fig.update_layout(
    title='<b>Cross-Price Elasticity Matrix</b><br><sup>Row: Demand Product | Column: Price Product | Diagonal: Own-Price</sup>',
    template='plotly_dark',
    height=500,
    width=600,
    xaxis_title='Price Change In...',
    yaxis_title='Demand For...'
)

# Add annotations
fig.add_annotation(
    text="🔴 Negative diagonal = Own-price (expected)<br>🔵 Positive off-diagonal = Substitutes",
    xref="paper", yref="paper",
    x=0.5, y=-0.18,
    showarrow=False,
    font=dict(size=11, color='#94a3b8')
)

fig.show()

# %% [markdown]
# ## 4.6 Interpreting the Matrix
# 
# ### Example Reading:
# 
# If `E[Polymers, Aromatics] = +0.32`:
# 
# > *"When Aromatics price increases by 1%, demand for Polymers increases by 0.32%"*
# 
# This indicates **substitution** - customers switch from Aromatics to Polymers.
# 
# ### Key Insights from Our Matrix:

# %% Substitution Analysis
print("🔄 SUBSTITUTION PATTERN ANALYSIS")
print("=" * 60)

print("\n📈 Strongest Substitutes:")
for i, demand_fam in enumerate(family_names):
    for j, price_fam in enumerate(family_names):
        if i != j:
            e = E_matrix.iloc[i, j]
            p = P_matrix.iloc[i, j]
            if e > 0.1 and p < 0.05:
                print(f"   {demand_fam} ← {price_fam}: +{e:.3f} ***")
                print(f"      → If {price_fam} price ↑10%, {demand_fam} demand ↑{e*10:.1f}%")

print("\n📉 Own-Price Elasticities:")
for fam in family_names:
    e = E_matrix.loc[fam, fam]
    if e < -1:
        status = "ELASTIC (price ↑ → revenue ↓)"
    else:
        status = "INELASTIC (price ↑ → revenue ↑)"
    print(f"   {fam:15s}: {e:+.3f} → {status}")

# %% [markdown]
# ---
# # Part 5: Optimal Pricing Engine
# ---
# 
# ## 5.1 The Optimization Problem
# 
# Now we use the elasticity matrix to find **profit-maximizing prices**.
# 
# ### Mathematical Formulation
# 
# **Objective:** Maximize total contribution margin
# 
# $$\max_{\mathbf{P}} \quad \Pi(\mathbf{P}) = \sum_{i=1}^{n} (P_i - C_i) \cdot Q_i(\mathbf{P})$$
# 
# **Demand Function** (log-linear with cross-effects):
# 
# $$Q_i(\mathbf{P}) = Q_i^0 \cdot \exp\left( \sum_{j=1}^{n} \varepsilon_{ij} \cdot \ln\frac{P_j}{P_j^0} \right)$$
# 
# **Subject to:**
# 
# | Constraint | Mathematical Form | Business Meaning |
# |------------|-------------------|------------------|
# | Margin floor | $(P_i - C_i)/P_i \geq m^{min}$ | Minimum 15% margin |
# | Price ceiling | $P_i \leq P_i^0 (1 + \delta^+)$ | Max +10% increase |
# | Price floor | $P_i \geq P_i^0 (1 - \delta^-)$ | Max -10% decrease |
# 
# ### Why This Matters
# 
# Without cross-elasticities, optimizing each product independently misses
# the **portfolio effect**. Raising one price might hurt that product but
# boost substitutes, improving overall profit.

# %% 5.2 Define Optimization Functions
def demand_function(P, Q0, E, P0):
    """
    Compute demand given prices, using elasticity matrix.
    
    Q_i = Q0_i * exp(Σ_j E_ij * ln(P_j / P0_j))
    """
    log_price_ratio = np.log(P / P0)
    log_Q_change = E @ log_price_ratio
    return Q0 * np.exp(log_Q_change)

def profit_function(P, Q0, C, E, P0):
    """Total contribution margin: Σ (P_i - C_i) * Q_i"""
    Q = demand_function(P, Q0, E, P0)
    return np.sum((P - C) * Q)

def negative_profit(P, Q0, C, E, P0):
    """For scipy.minimize (minimizes, so we negate)"""
    return -profit_function(P, Q0, C, E, P0)

# %% 5.3 Load Current State
query_current = """
SELECT 
    product_family,
    AVG(avg_price_per_mt) as current_price,
    AVG(avg_variable_cost) as variable_cost,
    SUM(total_quantity_mt) as total_quantity
FROM CHEMICALS_DB.CHEMICAL_OPS.ML_TRAINING_DATA
WHERE order_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY product_family
"""

current_df = session.sql(query_current).to_pandas()
current_df['FAMILY_ID'] = current_df['PRODUCT_FAMILY'].str.replace(' ', '_').str.upper()
current_df = current_df.set_index('FAMILY_ID')

# Align with elasticity matrix
P0 = np.array([current_df.loc[f, 'CURRENT_PRICE'] for f in family_names])
Q0 = np.array([current_df.loc[f, 'TOTAL_QUANTITY'] for f in family_names])
C = np.array([current_df.loc[f, 'VARIABLE_COST'] for f in family_names])
E_np = E_matrix.values.astype(float)

print("📊 CURRENT STATE")
print("=" * 60)
for i, fam in enumerate(family_names):
    margin_pct = (P0[i] - C[i]) / P0[i] * 100
    print(f"   {fam:15s}: Price=${P0[i]:,.0f} | Cost=${C[i]:,.0f} | Margin={margin_pct:.1f}% | Vol={Q0[i]:,.0f}MT")

current_profit = profit_function(P0, Q0, C, E_np, P0)
print(f"\n💰 Current Total Profit: ${current_profit:,.0f}")

# %% 5.4 Configure Constraints
MIN_MARGIN = 0.15       # 15% minimum margin
MAX_INCREASE = 0.10     # +10% max price increase
MAX_DECREASE = 0.10     # -10% max price decrease

# Build bounds
bounds = []
for i in range(len(P0)):
    floor_from_change = P0[i] * (1 - MAX_DECREASE)
    floor_from_margin = C[i] / (1 - MIN_MARGIN)
    lower = max(floor_from_change, floor_from_margin)
    upper = P0[i] * (1 + MAX_INCREASE)
    bounds.append((lower, upper))

print("\n⚙️ OPTIMIZATION CONSTRAINTS")
print("=" * 60)
print(f"   Minimum Margin: {MIN_MARGIN*100:.0f}%")
print(f"   Max Price Change: ±{MAX_INCREASE*100:.0f}%")
print(f"\n   Price Bounds:")
for i, fam in enumerate(family_names):
    print(f"      {fam:15s}: ${bounds[i][0]:,.0f} - ${bounds[i][1]:,.0f}")

# %% 5.5 Run Optimization
print("\n🚀 Running optimization...")

result = minimize(
    negative_profit,
    x0=P0,
    args=(Q0, C, E_np, P0),
    method='SLSQP',
    bounds=bounds,
    options={'maxiter': 1000, 'ftol': 1e-10}
)

P_opt = result.x
Q_opt = demand_function(P_opt, Q0, E_np, P0)
optimal_profit = profit_function(P_opt, Q0, C, E_np, P0)

print(f"   Status: {'✅ SUCCESS' if result.success else '❌ FAILED'}")
print(f"   Iterations: {result.nit}")

# %% [markdown]
# ## 5.6 Optimization Results

# %% Results Table
results_table = pd.DataFrame({
    'Product': [f.replace('_', ' ').title() for f in family_names],
    'Current Price': [f"${p:,.0f}" for p in P0],
    'Optimal Price': [f"${p:,.0f}" for p in P_opt],
    'Change %': [f"{(P_opt[i]-P0[i])/P0[i]*100:+.1f}%" for i in range(len(P0))],
    'Volume Change': [f"{(Q_opt[i]-Q0[i])/Q0[i]*100:+.1f}%" for i in range(len(Q0))],
    'Margin Impact': [f"${(P_opt[i]-C[i])*Q_opt[i] - (P0[i]-C[i])*Q0[i]:+,.0f}" for i in range(len(P0))]
})

print("\n📊 OPTIMIZATION RESULTS")
print("=" * 80)
print(results_table.to_string(index=False))

profit_improvement = (optimal_profit - current_profit) / current_profit * 100
print(f"\n{'='*80}")
print(f"💰 PORTFOLIO IMPACT")
print(f"   Current Profit:  ${current_profit:>12,.0f}")
print(f"   Optimal Profit:  ${optimal_profit:>12,.0f}")
print(f"   Improvement:     ${optimal_profit-current_profit:>+12,.0f} ({profit_improvement:+.1f}%)")

# %% Results Visualization
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=['Price Changes', 'Volume Impact', 'Margin Waterfall', 'Current vs Optimal'],
    specs=[[{}, {}], [{"colspan": 2}, None]]
)

# Price changes
colors = ['#22c55e' if (P_opt[i]-P0[i]) >= 0 else '#ef4444' for i in range(len(P0))]
fig.add_trace(go.Bar(
    x=[f.replace('_', ' ').title() for f in family_names],
    y=[(P_opt[i]-P0[i])/P0[i]*100 for i in range(len(P0))],
    marker_color=colors,
    text=[f"{(P_opt[i]-P0[i])/P0[i]*100:+.1f}%" for i in range(len(P0))],
    textposition='outside',
    showlegend=False
), row=1, col=1)

# Volume impact
fig.add_trace(go.Bar(
    x=[f.replace('_', ' ').title() for f in family_names],
    y=[(Q_opt[i]-Q0[i])/Q0[i]*100 for i in range(len(Q0))],
    marker_color=['#3b82f6' if (Q_opt[i]-Q0[i]) >= 0 else '#f59e0b' for i in range(len(Q0))],
    text=[f"{(Q_opt[i]-Q0[i])/Q0[i]*100:+.1f}%" for i in range(len(Q0))],
    textposition='outside',
    showlegend=False
), row=1, col=2)

# Margin waterfall
margin_changes = [(P_opt[i]-C[i])*Q_opt[i] - (P0[i]-C[i])*Q0[i] for i in range(len(P0))]
fig.add_trace(go.Waterfall(
    x=[f.replace('_', ' ').title() for f in family_names] + ['TOTAL'],
    y=margin_changes + [sum(margin_changes)],
    measure=['relative']*len(margin_changes) + ['total'],
    text=[f"${x:+,.0f}" for x in margin_changes + [sum(margin_changes)]],
    textposition='outside',
    connector={"line": {"color": "rgba(255,255,255,0.3)"}},
    increasing={"marker": {"color": "#22c55e"}},
    decreasing={"marker": {"color": "#ef4444"}},
    totals={"marker": {"color": "#3b82f6"}}
), row=2, col=1)

fig.update_layout(
    template='plotly_dark',
    height=700,
    title='<b>Optimization Results Dashboard</b>',
    showlegend=False
)

fig.update_yaxes(title_text='Change %', row=1, col=1)
fig.update_yaxes(title_text='Change %', row=1, col=2)
fig.update_yaxes(title_text='Margin Change ($)', row=2, col=1)

fig.show()

# %% [markdown]
# ## 5.7 Binding Constraints Analysis
# 
# Understanding which constraints are "binding" (active at the optimum) helps
# identify where additional flexibility would add value.

# %% Binding Constraints
print("\n🔒 BINDING CONSTRAINTS ANALYSIS")
print("=" * 60)

for i, fam in enumerate(family_names):
    lb, ub = bounds[i]
    p = P_opt[i]
    margin = (p - C[i]) / p
    
    binding = []
    if abs(p - lb) < 1:
        if lb > P0[i] * (1 - MAX_DECREASE):
            binding.append("MIN_MARGIN")
        else:
            binding.append("PRICE_FLOOR")
    if abs(p - ub) < 1:
        binding.append("PRICE_CEILING")
    
    status = ", ".join(binding) if binding else "✓ Not binding"
    print(f"   {fam:15s}: ${p:,.0f} | Margin {margin*100:.1f}% | {status}")

# %% [markdown]
# ---
# # Part 6: Sensitivity Analysis
# ---
# 
# How do results change with different constraint settings?

# %% Sensitivity Grid
print("\n📊 Running sensitivity analysis...")

sensitivity_results = []

for min_margin in [0.10, 0.15, 0.20, 0.25]:
    for max_change in [0.05, 0.10, 0.15, 0.20]:
        # Build bounds
        bounds_sens = []
        for i in range(len(P0)):
            floor = max(P0[i] * (1 - max_change), C[i] / (1 - min_margin))
            ceiling = P0[i] * (1 + max_change)
            bounds_sens.append((floor, ceiling))
        
        try:
            res = minimize(negative_profit, x0=P0, args=(Q0, C, E_np, P0),
                          method='SLSQP', bounds=bounds_sens, options={'maxiter': 500})
            if res.success:
                profit = -res.fun
                sensitivity_results.append({
                    'Min Margin %': min_margin * 100,
                    'Max Change %': max_change * 100,
                    'Optimal Profit': profit,
                    'Improvement %': (profit / current_profit - 1) * 100
                })
        except:
            pass

sens_df = pd.DataFrame(sensitivity_results)
sens_pivot = sens_df.pivot(index='Min Margin %', columns='Max Change %', values='Improvement %')

fig = go.Figure(data=go.Heatmap(
    z=sens_pivot.values,
    x=[f"{x:.0f}%" for x in sens_pivot.columns],
    y=[f"{y:.0f}%" for y in sens_pivot.index],
    colorscale='Greens',
    text=[[f"{v:.1f}%" for v in row] for row in sens_pivot.values],
    texttemplate='%{text}',
    textfont={"size": 14},
    colorbar=dict(title='Profit Δ%')
))

fig.update_layout(
    title='<b>Sensitivity Analysis: Profit Improvement vs Constraint Settings</b>',
    xaxis_title='Max Price Change Allowed',
    yaxis_title='Minimum Margin Required',
    template='plotly_dark',
    height=400
)

fig.show()

# %% [markdown]
# ### Sensitivity Insights
# 
# - **Looser constraints** (lower margin floor, higher change limit) → More optimization potential
# - **Diminishing returns**: Beyond ±15% price flexibility, gains level off
# - **Trade-off zone**: 15% margin + 10% change is a good balance

# %% [markdown]
# ---
# # Part 7: Business Recommendations
# ---
# 
# ## 7.1 Key Findings

# %% Summary Insights
print("=" * 70)
print("📋 EXECUTIVE SUMMARY: CHEMICALS PRICING OPTIMIZATION")
print("=" * 70)

print("\n1️⃣ OWN-PRICE ELASTICITIES")
print("-" * 50)
for fam in family_names:
    e = E_matrix.loc[fam, fam]
    if e < -1.5:
        rec = "→ Price-sensitive: compete on value, not price"
    elif e > -1.0:
        rec = "→ Price power: room to increase selectively"
    else:
        rec = "→ Moderate sensitivity: balance pricing strategy"
    print(f"   {fam:15s}: ε = {e:+.2f} {rec}")

print("\n2️⃣ SUBSTITUTION PATTERNS")
print("-" * 50)
print("   Key substitutes identified:")
for i, d_fam in enumerate(family_names):
    for j, p_fam in enumerate(family_names):
        if i != j:
            e = E_matrix.iloc[i, j]
            if e > 0.15:
                print(f"   • {d_fam} ← {p_fam}: +{e:.2f} (strong substitute)")

print("\n3️⃣ OPTIMIZATION IMPACT")
print("-" * 50)
print(f"   💰 Profit Improvement: +${optimal_profit - current_profit:,.0f} ({profit_improvement:+.1f}%)")
print(f"   📊 This is achievable within ±{MAX_INCREASE*100:.0f}% price changes")
print(f"   🎯 Respecting {MIN_MARGIN*100:.0f}% minimum margins")

print("\n4️⃣ RECOMMENDED ACTIONS")
print("-" * 50)
for i, fam in enumerate(family_names):
    change = (P_opt[i] - P0[i]) / P0[i] * 100
    if change > 2:
        action = f"↑ INCREASE by {change:.1f}%"
    elif change < -2:
        action = f"↓ DECREASE by {abs(change):.1f}%"
    else:
        action = "→ HOLD current price"
    print(f"   {fam:15s}: {action}")

# %% [markdown]
# ## 7.2 Caveats & Limitations
# 
# | Limitation | Mitigation |
# |------------|------------|
# | **Endogeneity** | Prices may respond to demand shocks; consider IV methods |
# | **Constant elasticity** | Log-log assumes constant ε; reality may vary by price level |
# | **Competitive response** | Model ignores competitor reactions; layer in game theory |
# | **Short-term focus** | Long-term customer relationships not captured |
# 
# ## 7.3 Next Steps
# 
# 1. **Validate** recommendations with regional sales teams
# 2. **A/B test** price changes in select markets
# 3. **Monitor** actual elasticity vs predicted
# 4. **Refine** model with more granular data (customer-level, contract vs spot)

# %% [markdown]
# ---
# # Appendix: Save Results to Snowflake
# ---

# %% Save Elasticity Matrix
print("💾 Saving results to Snowflake...")

# Save elasticity matrix
model_version = f"workbook_{datetime.now().strftime('%Y%m%d_%H%M')}"

matrix_records = []
for d_fam in family_names:
    for p_fam in family_names:
        e = E_matrix.loc[d_fam, p_fam]
        p = P_matrix.loc[d_fam, p_fam]
        matrix_records.append({
            'MODEL_VERSION': model_version,
            'ESTIMATION_DATE': datetime.now(),
            'DEMAND_PRODUCT_ID': d_fam,
            'DEMAND_PRODUCT_NAME': d_fam.replace('_', ' ').title(),
            'PRICE_PRODUCT_ID': p_fam,
            'PRICE_PRODUCT_NAME': p_fam.replace('_', ' ').title(),
            'ELASTICITY': float(e),
            'P_VALUE': float(p),
            'IS_OWN_PRICE': d_fam == p_fam,
            'IS_SIGNIFICANT': p < 0.05,
            'RELATIONSHIP_TYPE': 'own_price' if d_fam == p_fam else ('substitute' if e > 0.05 else 'independent')
        })

matrix_save_df = pd.DataFrame(matrix_records)
session.create_dataframe(matrix_save_df).write.mode("append").save_as_table(
    "CHEMICALS_DB.CHEMICAL_OPS.ELASTICITY_MATRIX"
)
print(f"   ✅ Saved {len(matrix_records)} elasticity pairs")

# Save optimization results
opt_records = []
for i, fam in enumerate(family_names):
    opt_records.append({
        'SCENARIO_ID': f"workbook_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'RUN_DATE': datetime.now(),
        'PRODUCT_ID': fam,
        'PRODUCT_NAME': fam.replace('_', ' ').title(),
        'REGION': 'ALL',
        'CURRENT_PRICE': float(P0[i]),
        'OPTIMAL_PRICE': float(P_opt[i]),
        'PRICE_CHANGE_PCT': float((P_opt[i] - P0[i]) / P0[i] * 100),
        'CURRENT_VOLUME': float(Q0[i]),
        'PREDICTED_VOLUME': float(Q_opt[i]),
        'CURRENT_MARGIN': float((P0[i] - C[i]) * Q0[i]),
        'PREDICTED_MARGIN': float((P_opt[i] - C[i]) * Q_opt[i])
    })

opt_save_df = pd.DataFrame(opt_records)
session.create_dataframe(opt_save_df).write.mode("append").save_as_table(
    "CHEMICALS_DB.CHEMICAL_OPS.OPTIMAL_PRICING"
)
print(f"   ✅ Saved {len(opt_records)} optimization results")

print("\n🎉 WORKBOOK COMPLETE!")
print("=" * 70)
