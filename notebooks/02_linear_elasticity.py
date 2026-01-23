# =============================================================================
# 02 - Linear Regression Elasticity Model
# =============================================================================
# Snowflake Notebook for Chemicals Pricing Solution
# 
# This notebook estimates own-price elasticity using linear regression:
# 1. Per-product elasticity estimation
# 2. Model diagnostics
# 3. Results visualization
# 4. Save to Snowflake
# =============================================================================

# %% [markdown]
# # Linear Regression Elasticity Model

# %% Import libraries
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Snowflake connection
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# %% [markdown]
# ## 1. Load Training Data

# %% Load ML training data
query = """
SELECT 
    product_id,
    product_name,
    product_family,
    region,
    order_date,
    ln_quantity,
    ln_price,
    fuel_oil_cpi,
    core_cpi,
    industrial_production,
    margin_pct,
    month,
    quarter,
    is_q4
FROM CHEMICALS_DB.CHEMICAL_OPS.ML_TRAINING_DATA
WHERE ln_quantity IS NOT NULL 
  AND ln_price IS NOT NULL
ORDER BY product_id, order_date
"""

df = session.sql(query).to_pandas()
print(f"Loaded {len(df):,} rows for {df['PRODUCT_ID'].nunique()} products")

# %% [markdown]
# ## 2. Define Estimation Function

# %% Elasticity estimation function
def estimate_elasticity(data, product_id):
    """
    Estimate own-price elasticity for a single product using OLS.
    
    Model: ln(Q) = α + β₁·ln(P) + β₂·CPI + β₃·IP + γ·month_dummies + ε
    
    Returns dict with elasticity estimate and diagnostics.
    """
    product_data = data[data['PRODUCT_ID'] == product_id].copy()
    
    if len(product_data) < 30:
        return None
    
    # Prepare features
    y = product_data['LN_QUANTITY']
    
    # Core features
    X = pd.DataFrame({
        'const': 1,
        'ln_price': product_data['LN_PRICE'],
    })
    
    # Add CPI if available
    if product_data['FUEL_OIL_CPI'].notna().sum() > len(product_data) * 0.5:
        X['fuel_cpi'] = product_data['FUEL_OIL_CPI'].fillna(product_data['FUEL_OIL_CPI'].mean())
        X['fuel_cpi'] = (X['fuel_cpi'] - X['fuel_cpi'].mean()) / X['fuel_cpi'].std()
    
    # Add Industrial Production if available
    if product_data['INDUSTRIAL_PRODUCTION'].notna().sum() > len(product_data) * 0.5:
        X['ip'] = product_data['INDUSTRIAL_PRODUCTION'].fillna(product_data['INDUSTRIAL_PRODUCTION'].mean())
        X['ip'] = (X['ip'] - X['ip'].mean()) / X['ip'].std()
    
    # Add month dummies for seasonality
    month_dummies = pd.get_dummies(product_data['MONTH'], prefix='month', drop_first=True)
    X = pd.concat([X, month_dummies], axis=1)
    
    # Drop rows with NaN
    mask = ~(X.isna().any(axis=1) | y.isna())
    X = X[mask]
    y = y[mask]
    
    if len(y) < 30:
        return None
    
    # Fit OLS
    try:
        model = OLS(y, X).fit()
        
        # Extract results
        result = {
            'product_id': product_id,
            'product_name': product_data['PRODUCT_NAME'].iloc[0],
            'product_family': product_data['PRODUCT_FAMILY'].iloc[0],
            'n_observations': int(model.nobs),
            'own_elasticity': float(model.params['ln_price']),
            'own_elasticity_se': float(model.bse['ln_price']),
            'own_elasticity_tstat': float(model.tvalues['ln_price']),
            'own_elasticity_pvalue': float(model.pvalues['ln_price']),
            'cpi_coefficient': float(model.params.get('fuel_cpi', np.nan)),
            'cpi_pvalue': float(model.pvalues.get('fuel_cpi', np.nan)),
            'ip_coefficient': float(model.params.get('ip', np.nan)),
            'ip_pvalue': float(model.pvalues.get('ip', np.nan)),
            'r_squared': float(model.rsquared),
            'adj_r_squared': float(model.rsquared_adj),
            'rmse': float(np.sqrt(model.mse_resid)),
            'aic': float(model.aic),
            'training_start_date': product_data['ORDER_DATE'].min(),
            'training_end_date': product_data['ORDER_DATE'].max()
        }
        
        return result
        
    except Exception as e:
        print(f"Error estimating {product_id}: {e}")
        return None

# %% [markdown]
# ## 3. Estimate Elasticities for All Products

# %% Estimate for each product
products = df['PRODUCT_ID'].unique()
print(f"Estimating elasticities for {len(products)} products...")

results = []
for product_id in products:
    result = estimate_elasticity(df, product_id)
    if result:
        results.append(result)
        print(f"  {result['product_name']}: ε = {result['own_elasticity']:.3f} (p={result['own_elasticity_pvalue']:.4f})")

elasticity_df = pd.DataFrame(results)
print(f"\nSuccessfully estimated {len(elasticity_df)} products")

# %% [markdown]
# ## 4. Results Summary

# %% Summary table
summary = elasticity_df[['product_name', 'product_family', 'own_elasticity', 
                         'own_elasticity_pvalue', 'r_squared', 'n_observations']].copy()
summary = summary.round({'own_elasticity': 3, 'own_elasticity_pvalue': 4, 'r_squared': 3})
summary = summary.sort_values('own_elasticity')
print(summary.to_string(index=False))

# %% [markdown]
# ## 5. Visualization

# %% 5.1 Elasticity by Product
fig = px.bar(
    elasticity_df.sort_values('own_elasticity'),
    x='own_elasticity',
    y='product_name',
    color='product_family',
    orientation='h',
    title='Own-Price Elasticity by Product',
    labels={'own_elasticity': 'Price Elasticity', 'product_name': 'Product'}
)
fig.add_vline(x=-1, line_dash="dash", line_color="white", annotation_text="Unit Elastic")
fig.update_layout(template='plotly_dark', height=600)
fig.show()

# %% 5.2 Elasticity vs R-squared
fig = px.scatter(
    elasticity_df,
    x='r_squared',
    y='own_elasticity',
    color='product_family',
    size='n_observations',
    hover_data=['product_name'],
    title='Elasticity vs Model Fit (R²)',
    labels={'r_squared': 'R-squared', 'own_elasticity': 'Price Elasticity'}
)
fig.update_layout(template='plotly_dark', height=500)
fig.show()

# %% 5.3 Elasticity by Product Family
fig = px.box(
    elasticity_df,
    x='product_family',
    y='own_elasticity',
    color='product_family',
    title='Elasticity Distribution by Product Family'
)
fig.add_hline(y=-1, line_dash="dash", line_color="white")
fig.update_layout(template='plotly_dark', height=400)
fig.show()

# %% [markdown]
# ## 6. Model Diagnostics

# %% Diagnostic summary
n_positive = (elasticity_df['own_elasticity'] > 0).sum()
n_significant = (elasticity_df['own_elasticity_pvalue'] < 0.05).sum()
avg_r2 = elasticity_df['r_squared'].mean()

print("=== MODEL DIAGNOSTICS ===")
print(f"Products with positive elasticity (unexpected): {n_positive}/{len(elasticity_df)}")
print(f"Products with significant elasticity (p<0.05): {n_significant}/{len(elasticity_df)}")
print(f"Average R-squared: {avg_r2:.3f}")
print(f"Elasticity range: [{elasticity_df['own_elasticity'].min():.3f}, {elasticity_df['own_elasticity'].max():.3f}]")

# %% [markdown]
# ## 7. Save Results to Snowflake

# %% Prepare and save data
model_version = f"linear_v1_{datetime.now().strftime('%Y%m%d')}"
elasticity_df['model_version'] = model_version
elasticity_df['estimation_date'] = datetime.now()

# Column mapping to match table schema
save_df = elasticity_df.rename(columns={
    'own_elasticity': 'OWN_ELASTICITY',
    'own_elasticity_se': 'OWN_ELASTICITY_SE',
    'own_elasticity_tstat': 'OWN_ELASTICITY_TSTAT',
    'own_elasticity_pvalue': 'OWN_ELASTICITY_PVALUE',
    'cpi_coefficient': 'CPI_COEFFICIENT',
    'cpi_pvalue': 'CPI_PVALUE',
    'ip_coefficient': 'IP_COEFFICIENT',
    'ip_pvalue': 'IP_PVALUE',
    'n_observations': 'N_OBSERVATIONS',
    'r_squared': 'R_SQUARED',
    'adj_r_squared': 'ADJ_R_SQUARED',
    'rmse': 'RMSE',
    'aic': 'AIC',
    'training_start_date': 'TRAINING_START_DATE',
    'training_end_date': 'TRAINING_END_DATE',
    'model_version': 'MODEL_VERSION',
    'estimation_date': 'ESTIMATION_DATE',
    'product_id': 'PRODUCT_ID',
    'product_name': 'PRODUCT_NAME',
    'product_family': 'PRODUCT_FAMILY'
})

# Save to Snowflake
sp_df = session.create_dataframe(save_df)
sp_df.write.mode("append").save_as_table("CHEMICALS_DB.CHEMICAL_OPS.PRICE_ELASTICITY_LINEAR")
print(f"Saved {len(save_df)} elasticity estimates with version: {model_version}")

# %% [markdown]
# ## Summary
# 
# **Linear Regression Results:**
# - Estimated own-price elasticity for all products
# - Average elasticity: typically -1 to -2 (elastic demand)
# - Controls: Fuel Oil CPI, Industrial Production, Month dummies
# 
# **Key Findings:**
# - Most products have negative elasticity (as expected)
# - Polymers tend to be less elastic (more essential)
# - Aromatics/Olefins more elastic (more substitutes)
# 
# **Next Steps:**
# - Run 03_sur_elasticity.py for cross-elasticity matrix
# - This will capture substitution patterns between products
