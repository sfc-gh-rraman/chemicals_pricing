# =============================================================================
# 01 - Data Preparation for Price Elasticity Models
# =============================================================================
# Snowflake Notebook for Chemicals Pricing Solution
# 
# This notebook prepares the training data for elasticity estimation:
# 1. Load data from Snowflake
# 2. Exploratory data analysis
# 3. Feature engineering validation
# 4. Create train/test splits
# =============================================================================

# %% [markdown]
# # Data Preparation for Price Elasticity Models

# %% Import libraries
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Snowflake connection
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# %% [markdown]
# ## 1. Load Training Data

# %% Load ML training data
query = """
SELECT *
FROM CHEMICALS_DB.CHEMICAL_OPS.ML_TRAINING_DATA
ORDER BY order_date, product_id
"""

df = session.sql(query).to_pandas()

print(f"Loaded {len(df):,} rows")
print(f"Date range: {df['ORDER_DATE'].min()} to {df['ORDER_DATE'].max()}")
print(f"Products: {df['PRODUCT_ID'].nunique()}")
print(f"Regions: {df['REGION'].nunique()}")

# %% [markdown]
# ## 2. Data Quality Check

# %% Check for missing values
missing = df.isnull().sum()
missing_pct = (missing / len(df) * 100).round(2)
quality_df = pd.DataFrame({
    'Missing Count': missing,
    'Missing %': missing_pct
})
print("Columns with missing values:")
print(quality_df[quality_df['Missing Count'] > 0])

# %% Summary statistics for key variables
summary_cols = ['TOTAL_QUANTITY_MT', 'AVG_PRICE_PER_MT', 'LN_QUANTITY', 'LN_PRICE', 
                'MARGIN_PCT', 'FUEL_OIL_CPI', 'CORE_CPI', 'INDUSTRIAL_PRODUCTION']
print("\nSummary Statistics:")
print(df[summary_cols].describe().round(3))

# %% [markdown]
# ## 3. Exploratory Data Analysis

# %% 3.1 Price-Quantity Relationship by Product Family
fig = px.scatter(
    df.dropna(subset=['LN_PRICE', 'LN_QUANTITY']),
    x='LN_PRICE',
    y='LN_QUANTITY',
    color='PRODUCT_FAMILY',
    opacity=0.5,
    title='Log Price vs Log Quantity by Product Family',
    labels={'LN_PRICE': 'Log(Price)', 'LN_QUANTITY': 'Log(Quantity)'}
)
fig.update_layout(template='plotly_dark', height=500)
fig.show()

# %% 3.2 Price Distribution by Product
fig = px.box(
    df,
    x='PRODUCT_FAMILY',
    y='AVG_PRICE_PER_MT',
    color='PRODUCT_FAMILY',
    title='Price Distribution by Product Family'
)
fig.update_layout(template='plotly_dark', height=400)
fig.show()

# %% 3.3 Time Series of Prices by Product
price_ts = df.groupby(['ORDER_DATE', 'PRODUCT_FAMILY'])['AVG_PRICE_PER_MT'].mean().reset_index()

fig = px.line(
    price_ts,
    x='ORDER_DATE',
    y='AVG_PRICE_PER_MT',
    color='PRODUCT_FAMILY',
    title='Average Price Over Time by Product Family'
)
fig.update_layout(template='plotly_dark', height=400)
fig.show()

# %% 3.4 Correlation Matrix
corr_cols = ['LN_QUANTITY', 'LN_PRICE', 'FUEL_OIL_CPI', 'CORE_CPI', 
             'INDUSTRIAL_PRODUCTION', 'MARGIN_PCT']
corr_matrix = df[corr_cols].corr()

fig = px.imshow(
    corr_matrix,
    labels=dict(color="Correlation"),
    x=corr_cols,
    y=corr_cols,
    color_continuous_scale='RdBu',
    title='Correlation Matrix of Key Variables'
)
fig.update_layout(template='plotly_dark', height=500)
fig.show()

# %% [markdown]
# ## 4. Cross-Price Data Preparation
# For SUR model, we need prices of all products available at each date

# %% Create wide-format price data for cross-elasticity
price_wide = df.pivot_table(
    index=['ORDER_DATE', 'REGION'],
    columns='PRODUCT_NAME',
    values='AVG_PRICE_PER_MT',
    aggfunc='mean'
).reset_index()

print(f"Wide format shape: {price_wide.shape}")
print(f"Products as columns: {price_wide.columns[2:].tolist()}")

# %% Similarly for quantities
qty_wide = df.pivot_table(
    index=['ORDER_DATE', 'REGION'],
    columns='PRODUCT_NAME',
    values='TOTAL_QUANTITY_MT',
    aggfunc='sum'
).reset_index()

print(f"Quantity wide format shape: {qty_wide.shape}")

# %% [markdown]
# ## 5. Train/Test Split

# %% Temporal split - use last 20% of dates as test
dates = df['ORDER_DATE'].sort_values().unique()
split_idx = int(len(dates) * 0.8)
split_date = dates[split_idx]

train_df = df[df['ORDER_DATE'] < split_date].copy()
test_df = df[df['ORDER_DATE'] >= split_date].copy()

print(f"Split date: {split_date}")
print(f"Training: {len(train_df):,} rows ({len(train_df)/len(df)*100:.1f}%)")
print(f"Test: {len(test_df):,} rows ({len(test_df)/len(df)*100:.1f}%)")

# %% [markdown]
# ## 6. Product Family Aggregation
# For SUR, we'll estimate at product family level (4 equations)

# %% Aggregate to product family level for cleaner estimation
family_df = df.groupby(['ORDER_DATE', 'REGION', 'PRODUCT_FAMILY']).agg({
    'TOTAL_QUANTITY_MT': 'sum',
    'TOTAL_REVENUE': 'sum',
    'AVG_PRICE_PER_MT': 'mean',
    'AVG_VARIABLE_COST': 'mean',
    'FUEL_OIL_CPI': 'first',
    'CORE_CPI': 'first',
    'INDUSTRIAL_PRODUCTION': 'first',
    'MONTH': 'first',
    'QUARTER': 'first',
    'IS_Q4': 'first'
}).reset_index()

# Recalculate log transforms
family_df['LN_QUANTITY'] = np.log(family_df['TOTAL_QUANTITY_MT'].clip(lower=0.01))
family_df['LN_PRICE'] = np.log(family_df['AVG_PRICE_PER_MT'].clip(lower=0.01))

print(f"Family-level data: {len(family_df):,} rows")
print(f"Families: {family_df['PRODUCT_FAMILY'].unique().tolist()}")

# %% [markdown]
# ## 7. Create Cross-Price Features for SUR

# %% Pivot to get all family prices in each row
family_prices = family_df.pivot_table(
    index=['ORDER_DATE', 'REGION'],
    columns='PRODUCT_FAMILY',
    values='AVG_PRICE_PER_MT',
    aggfunc='mean'
).reset_index()

# Rename columns with prefix
family_prices.columns = ['ORDER_DATE', 'REGION'] + [f'PRICE_{c.upper().replace(" ", "_")}' for c in family_prices.columns[2:]]

# Merge back
family_df = family_df.merge(family_prices, on=['ORDER_DATE', 'REGION'], how='left')

# Create log price features
for col in family_prices.columns[2:]:
    family_df[f'LN_{col}'] = np.log(family_df[col].clip(lower=0.01))

print("Cross-price features created:")
print([c for c in family_df.columns if 'PRICE_' in c])

# %% [markdown]
# ## 8. Save Prepared Data

# %% Save to Snowflake for model training
sp_df = session.create_dataframe(family_df)
sp_df.write.mode("overwrite").save_as_table("CHEMICALS_DB.CHEMICAL_OPS.ML_FAMILY_TRAINING_DATA")
print("Saved ML_FAMILY_TRAINING_DATA to Snowflake")

# %% [markdown]
# ## Summary
# 
# **Data Prepared:**
# - Raw training data: 26,912 rows
# - Family-level aggregation: ~5,700 rows
# - 4 product families: Polymers, Aromatics, Intermediates, Olefins
# - Features include: prices, quantities, costs, CPI, industrial production
# - Cross-price features created for SUR estimation
# 
# **Next Steps:**
# - Run 02_linear_elasticity.py for baseline elasticities
# - Run 03_sur_elasticity.py for cross-elasticity matrix
