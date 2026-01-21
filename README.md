# Chemicals Integrated Pricing & Supply Chain Optimization

**Version:** 1.0  
**Platform:** Snowflake  
**Database:** CHEMICALS_DB  
**Status:** Ready for Deployment ✅

---

## Overview

This demo showcases an **Intelligent Chemicals Pricing & Supply Chain** solution built on Snowflake. It combines real-time market data with ML-powered predictions to enable dynamic pricing, demand sensing, and margin optimization for chemical manufacturers.

### The Problem

Chemical companies are squeezed between **volatile feedstock costs** (Oil/Gas prices) and **fixed customer contracts**. Pricing teams lack real-time visibility into "Cost-to-Serve" and production constraints, often selling products at a loss during commodity spikes.

### The Solution

An integrated platform that:
- Calculates **real-time marginal costs** including feedstock prices and logistics
- Predicts **demand** using macro-economic indicators
- Recommends **optimal prices** that maximize margin
- Provides **natural language analytics** via Cortex Analyst
- Enables **semantic search** of market reports via Cortex Search

---

## The "Wow" Moment

> A Pricing Manager asks, **"What should we charge for Polypropylene in the Northeast given the Propylene spike?"**
>
> Cortex calculates the real-time "Marginal Cost" (including the new feedstock price + logistics), simulates the demand elasticity, and recommends a price of **$1,450/MT** to maximize margin, supported by a recent market report found via Search.

### Hidden Discovery

**Surface Appearance:** Demand looks random or seasonal.

**Revealed Reality:** Demand for 'Polypropylene Grade B' in Southeast Asia is **90% correlated** with 'Crude Oil Brent' prices with a **3-week lag**, allowing for predictive inventory positioning before the spike.

**Business Impact:** Prevents stock-outs during price spikes, capturing an additional **$2M in quarterly margin**.

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Real-time Cost-to-Serve** | Dynamic cost calculation from feedstock to delivery |
| **Demand Forecasting** | XGBoost model predicting sales volume |
| **Price Optimization** | scipy.optimize finding profit-maximizing prices |
| **Cortex Analyst** | Natural language queries on structured data |
| **Cortex Search** | Semantic search across market reports |
| **5 Dashboard Pages** | Strategic, Operational, and Technical views |

---

## Business KPIs (DRD Targets)

| KPI | Target | Description |
|-----|--------|-------------|
| **Margin** | +5-10% EBITDA | Via dynamic pricing |
| **Inventory** | -15% Working Capital | Through demand sensing |
| **Sustainability** | 100% Traceability | Carbon footprint per batch |
| **Underpriced Deals** | -50% | Reduce margin leakage |

---

## Architecture

### Database Structure

```
CHEMICALS_DB (Database)
├── RAW (Schema)                    # Landing zone
│   ├── MARKET_FEEDSTOCK           # ICIS/Platts price indices
│   ├── ERP_SALES_ORDERS           # Transaction history
│   ├── PLANT_COSTS                # Manufacturing costs
│   └── MARKET_REPORTS             # Analyst reports (for Search)
│
├── ATOMIC (Schema)                 # Normalized entity model
│   ├── PRODUCT                    # Chemical catalog (CAS numbers)
│   ├── CUSTOMER                   # Customer master
│   ├── SITE                       # Manufacturing sites
│   ├── PRODUCTION_RUN             # Batch data
│   ├── INVENTORY_BALANCE          # Stock levels
│   ├── MARKET_INDEX               # Price indices (extension)
│   ├── CONTRACT_TERM              # Pricing formulas (extension)
│   └── DEMAND_HISTORY             # For ML training
│
└── CHEMICAL_OPS (Schema)           # Data mart
    ├── MARGIN_ANALYZER            # Profitability analysis
    ├── COST_TO_SERVE              # Real-time costs
    ├── PRICE_GUIDANCE             # Price recommendations
    ├── INVENTORY_HEALTH           # Stock analysis
    ├── DEMAND_FORECAST            # ML predictions
    ├── OPTIMAL_PRICING            # Price optimization
    ├── PRICE_ELASTICITY           # Elasticity coefficients
    └── LAGGED_CORRELATION         # Hidden discovery
```

---

## Streamlit Application (5 Pages)

### Strategic Views

| Page | Persona | Description |
|------|---------|-------------|
| **📊 Margin Command** | VP Commercial | Spread charts, leakage waterfall, regional margins |

### Operational Views

| Page | Persona | Description |
|------|---------|-------------|
| **💵 Pricing Desk** | Supply Chain Mgr | Price guidance, cost breakdown, quote generator |

### Technical Views

| Page | Persona | Description |
|------|---------|-------------|
| **🔬 Data Science Workbench** | Pricing Analyst | Model validation, feature importance, lagged correlation |

### Intelligence Views

| Page | Persona | Description |
|------|---------|-------------|
| **📄 Market Intelligence** | All | Cortex Search on market reports |
| **🤖 Ask Cortex** | All | Natural language analytics |

---

## Quick Start

### Prerequisites

1. Snowflake account with ACCOUNTADMIN privileges
2. Snowflake CLI (`snow`) installed and configured
3. Python 3.9+ with pandas, numpy

### Step 1: Generate Synthetic Data

```bash
cd chemicals_pricing_supply_chain
python scripts/generate_synthetic_data.py --output-dir data/synthetic
```

This generates:
- `products.csv` - 20 chemical products
- `customers.csv` - 200 customers
- `market_indices.csv` - 2 years of price data
- `sales_orders.csv` - 1 year of transactions
- `production_runs.csv` - Batch data
- `inventory_balances.csv` - Current stock
- `contract_terms.csv` - Pricing contracts
- `market_reports.csv` - For Cortex Search

### Step 2: Deploy DDL

```bash
snow sql -f ddl/core/001_DATABASE_SCHEMAS.sql
snow sql -f ddl/core/002_RAW_TABLES.sql
snow sql -f ddl/core/003_ATOMIC_CORE.sql
snow sql -f ddl/core/004_ATOMIC_EXTENSIONS.sql
snow sql -f ddl/core/005_CHEMICAL_OPS_MART.sql
snow sql -f ddl/core/006_ML_PREDICTIONS.sql
```

### Step 3: Load Data

```bash
# Upload CSV files to stage
snow stage put data/synthetic/*.csv @RAW.DATA_STAGE

# Run data loading script
snow sql -f data/load_all_data.sql
```

### Step 4: Deploy Cortex Services

```bash
# Deploy Cortex Analyst semantic model
snow sql -f cortex/deploy_cortex.sql

# Deploy Cortex Search service
snow sql -f cortex/deploy_search.sql
```

### Step 5: Deploy Streamlit

```bash
cd streamlit
snow streamlit deploy --database CHEMICALS_DB --schema CHEMICAL_OPS
```

### Or Use the Deployment Script

```bash
chmod +x deploy.sh
./deploy.sh
```

---

## User Personas

### Strategic: VP Commercial

**Goal**: "See if we're passing through feedstock cost increases to customers"

**Key Views**:
- Price vs Cost Spread Chart
- Margin Leakage Waterfall
- Regional Margin Comparison
- Underpriced Deal Count

### Operational: Supply Chain Manager

**Goal**: "Quote the right price for Customer X on Product Y"

**Key Views**:
- Price Guidance (Floor, Target, Ceiling)
- Cost Breakdown Waterfall
- Market Drivers
- Contract Terms

### Technical: Pricing Analyst

**Goal**: "Validate the demand model and find hidden patterns"

**Key Views**:
- Actual vs Predicted Volume
- Feature Importance
- Lagged Correlation (Hidden Discovery)
- Price Elasticity Analysis
- What-If Simulator

---

## Cortex Analyst Golden Queries

These verified queries are embedded in the semantic model:

1. **"Show me the average margin for Ethylene products in Europe vs North America for Q3"**
2. **"How many deals were underpriced this month?"**
3. **"What is the trend in Propylene prices this week?"**
4. **"Which products have critical inventory levels?"**
5. **"What should we charge for Polypropylene in North America?"**

---

## Repository Structure

```
chemicals_pricing_supply_chain/
├── README.md                        # This file
├── DRD.md                          # Demo Requirements Document
├── deploy.sh                       # Main deployment script
├── clean.sh                        # Cleanup script
│
├── ddl/
│   └── core/
│       ├── 001_DATABASE_SCHEMAS.sql
│       ├── 002_RAW_TABLES.sql
│       ├── 003_ATOMIC_CORE.sql
│       ├── 004_ATOMIC_EXTENSIONS.sql
│       ├── 005_CHEMICAL_OPS_MART.sql
│       └── 006_ML_PREDICTIONS.sql
│
├── data/
│   ├── synthetic/                  # Generated data
│   └── load_all_data.sql           # Loading script
│
├── scripts/
│   └── generate_synthetic_data.py  # Data generator
│
├── cortex/
│   ├── chemicals_semantic_model.yaml
│   ├── deploy_cortex.sql
│   └── deploy_search.sql
│
└── streamlit/
    ├── streamlit_app.py            # Margin Command
    ├── pages/
    │   ├── 1_Pricing_Desk.py
    │   ├── 2_Data_Science_Workbench.py
    │   ├── 3_Market_Intelligence.py
    │   └── 4_Ask_Cortex.py
    ├── utils/
    │   └── theme.py
    ├── environment.yml
    └── snowflake.yml
```

---

## Technical Specifications

| Component | Requirement |
|-----------|-------------|
| Snowflake Edition | Enterprise or higher |
| Warehouse Size | X-Small sufficient |
| Cortex Features | Analyst, Search |
| Streamlit | Streamlit in Snowflake |

### Data Volumes

| Table | Records |
|-------|---------|
| PRODUCT | 20 |
| CUSTOMER | 200 |
| MARKET_INDEX | ~7,000 |
| SALES_ORDER | ~12,000 |
| PRODUCTION_RUN | ~8,000 |
| INVENTORY_BALANCE | ~120 |
| CONTRACT_TERM | ~150 |
| MARKET_REPORTS | ~500 |

---

## Related Resources

- [Demo Requirements Document (DRD)](./DRD.md)
- [Snowflake Cortex Analyst Docs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Snowflake Cortex Search Docs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search)

---

**Demo Version:** 1.0  
**Last Updated:** January 2026  
**Status:** Ready for Deployment ✅
# chemicals_pricing
# chemicals_pricing
