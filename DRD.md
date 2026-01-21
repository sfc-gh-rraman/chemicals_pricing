# Demo Requirements Document (DRD): Chemicals Integrated Pricing & Supply Chain Optimization

## 1. Strategic Overview

*   **Problem Statement:** Chemical companies are squeezed between volatile feedstock costs (Oil/Gas prices) and fixed customer contracts. Pricing teams lack real-time visibility into "Cost-to-Serve" and production constraints, often selling products at a loss during commodity spikes. Supply chains are disconnected from market signals, leading to inventory bloat.
*   **Target Business Goals (KPIs):**
    *   **Margin:** Increase EBITDA by 5-10% via dynamic pricing.
    *   **Inventory:** Reduce Working Capital by 15% through demand sensing.
    *   **Sustainability:** Trace 100% of carbon footprint per batch.
*   **The "Wow" Moment:** A Pricing Manager asks, "What should we charge for Polypropylene in the Northeast given the Propylene spike?" Cortex calculates the real-time "Marginal Cost" (including the new feedstock price + logistics), simulates the demand elasticity, and recommends a price of $1,450/MT to maximize margin, supported by a recent market report found via Search.
*   **Hidden Discovery:**
    *   **Discovery Statement:** "Demand for 'Polypropylene Grade B' in Southeast Asia is 90% correlated with 'Crude Oil Brent' prices with a 3-week lag, allowing for predictive inventory positioning before the spike."
    *   **Surface Appearance:** Demand looks random or seasonal.
    *   **Revealed Reality:** A clear lagged correlation with upstream crude indices drives downstream demand.
    *   **Business Impact:** Prevents stock-outs during price spikes, capturing an additional $2M in quarterly margin.

## 2. User Personas & Stories

| Persona Level | Role | User Story | Key Visuals |
| :--- | :--- | :--- | :--- |
| **Strategic** | **VP Commercial** | "As a VP, I want to see the 'Price vs. Volume' trade-off curve to set regional strategies." | Global Margin Map, Price Elasticity Curve, Competitor Price Index. |
| **Operational** | **Supply Chain Mgr** | "As a Manager, I want to balance inventory levels with predicted demand to minimize storage costs." | Inventory Health (Days of Supply), Feedstock Coverage, logistics Cost Trend. |
| **Technical** | **Pricing Analyst** | "As an Analyst, I want to simulate the impact of a 5% feedstock hike on our profitability." | "What-If" Simulator, Cost Component Waterfall, Margin-at-Risk Distribution. |

## 3. Data Architecture

### 3.1 Schema Structure
All data resides in `CHEMICALS_DB` with the following schemas:

*   **`RAW`**: Landing zone for Market Feeds (ICIS/Platts), ERP Sales, and Plant Data.
*   **`ATOMIC`**: Normalized Enterprise Data Model.
*   **`CHEMICAL_OPS`**: Data Mart for consumption.

### 3.2 RAW Layer
*   `RAW.MARKET_FEEDSTOCK`: External price indices (API feed).
*   `RAW.ERP_SALES_ORDERS`: Transaction history.
*   `RAW.PLANT_COSTS`: Allocated fixed/variable costs.

### 3.3 ATOMIC Layer (Core & Extensions)
*   **Core Entities** (Mapped from Data Dictionary):
    *   `ATOMIC.PRODUCT`:
        *   *Columns*: `PRODUCT_ID`, `CAS_NUMBER`, `PRODUCT_FAMILY`.
    *   `ATOMIC.PRODUCTION_RUN` (Batch):
        *   *Columns*: `RUN_ID`, `PRODUCT_ID`, `START_TIME`, `ACTUAL_YIELD`.
    *   `ATOMIC.INVENTORY_BALANCE`:
        *   *Columns*: `INVENTORY_BALANCE_ID`, `PRODUCT_ID`, `SITE_ID`, `QUANTITY`.
*   **Extension Entities** (Project Specific):
    *   `ATOMIC.MARKET_INDEX` (Extension):
        *   *Columns*: `INDEX_ID`, `NAME`, `DATE`, `PRICE`, `CURRENCY`.
    *   `ATOMIC.CONTRACT_TERM` (Extension):
        *   *Columns*: `TERM_ID`, `CUSTOMER_ID`, `PRODUCT_ID`, `PRICE_FORMULA` (e.g., Index + $50).

### 3.4 Data Mart Layer (`CHEMICAL_OPS`)
*   `CHEMICAL_OPS.MARGIN_ANALYZER` (Table): Pre-computed profitability per transaction.
*   `CHEMICAL_OPS.DEMAND_FORECAST` (Table): ML prediction of sales volume.
*   `CHEMICAL_OPS.OPTIMAL_PRICING` (Table): Recommended price points.

## 4. Cortex Intelligence Specifications

### 4.1 Cortex Analyst (Structured)
*   **Semantic Model Scope**:
    *   **Measures**: `Gross_Margin`, `Sales_Volume_MT`, `Avg_Selling_Price`, `Feedstock_Cost`.
    *   **Dimensions**: `Product_Family`, `Region`, `Customer_Segment`, `Sales_Channel`.
*   **Golden Query**: "Show me the average margin for 'Ethylene' products in 'Europe' vs 'North America' for Q3."

### 4.2 Cortex Search (Unstructured)
*   **Service Name**: `MARKET_INTEL_SEARCH`
*   **Source Data**: Market Analyst Reports (PDF), Competitor Earnings Transcripts, Trade Journals.
*   **Indexing Strategy**: Index by `Chemical_Name` and `Region`.
*   **Sample Prompt**: "What are the supply constraints for Propylene in Asia reported in the last month?"

### 4.3 Snowpark Notebooks (Data Science)
*   **Notebook Name**: `INTEGRATED_DEMAND_PRICING_ENGINE`
*   **Persona**: Pricing Analyst / Data Scientist.
*   **Objective**: Predict sales volume based on market indicators and recommend the optimal price to maximize margin.
*   **Workflow**:
    1.  **Demand Sensing**: Ingest macro-economic indicators (Oil prices, GDP) and train an XGBoost regressor to predict `Sales_Volume`.
    2.  **Price Elasticity**: Calculate the coefficient of elasticity for each product/region pair using historical sales data (Linear Regression).
    3.  **Optimization**: Use a solver (e.g., `scipy.optimize`) to find the price point that maximizes `(Price - Cost) * Predicted_Volume`.
*   **Libraries**: `snowflake.ml` (Modeling), `scikit-learn` (Regression), `scipy` (Optimization).

## 5. Streamlit Application UX/UI

### Page 1: Margin Command (Strategic)
*   **Situation**: "Feedstock costs spiked 10%. Are we passing it on?"
*   **Visuals**:
    *   **Spread Chart**: Selling Price vs. Raw Material Cost trend.
    *   **Leakage Waterfall**: Where margin is lost (Freight, Discounts, Rebates).

### Page 2: Pricing Desk (Operational)
*   **Task**: "Quote a price for Customer X."
*   **Visuals**:
    *   **Price Guidance**: "Target: $1.50/lb (Floor: $1.42)".
    *   **Drivers**: "Why? Feedstock +3%, Competitor Index +2%".
    *   **Action**: "Generate Quote".

### Page 3: Data Science Workbench (Technical)
*   **Action**: "Validate the Demand Model."
*   **Visuals**:
    *   **Forecast Accuracy**: Actual vs. Predicted Sales Volume.
    *   **Feature Importance**: Impact of "Crude Price" and "Competitor Index" on Demand.
    *   **Hidden Discovery**: The "Lagged Correlation" chart.

## 6. Success Criteria

*   **Technical**:
    *   Ingest daily market feeds and update cost models in < 15 mins.
    *   Demand forecast accuracy > 85% (MAPE).
*   **Business**:
    *   Improve EBITDA margin by 5%.
    *   Reduce "Underpriced" deals by 50%.
