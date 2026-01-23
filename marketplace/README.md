# Marketplace Data Integration

This folder contains scripts and SQL to integrate real-time market data from Snowflake Marketplace into the Chemicals Pricing solution.

## Quick Start

### Step 1: Install Free Marketplace Data (Manual - Recommended)

1. Go to [Snowsight](https://app.snowflake.com)
2. Navigate to **Data** → **Marketplace**
3. Search for **"Cybersyn Financial & Economic Essentials"** (FREE)
4. Click **"Get"** and accept terms
5. Name the database: `CYBERSYN` (or remember the name)

### Step 2: Create Integration Views

After installing marketplace data, run:

```bash
snow sql -f marketplace/create_marketplace_views.sql --connection demo
```

### Step 3: Update Environment Variable (Optional)

If you named your database something other than `CYBERSYN`, update line 20 in `create_marketplace_views.sql`.

## Recommended Free Listings

| Provider | Listing Name | Data Types | Use Case |
|----------|-------------|------------|----------|
| **Cybersyn** | Financial & Economic Essentials | FRED data, commodities, PPI | Crude oil, natural gas, economic indicators |
| **Knoema** | World Data Atlas | Global economic data | International demand drivers |
| **Weathersource** | OnPoint Weather | Weather data | Supply chain disruption signals |

## What Gets Integrated

Once marketplace data is installed:

### 1. Real Crude Oil Prices
- WTI Crude (DCOILWTICO)
- Brent Crude (DCOILBRENTEU)
- Used for: Feedstock cost tracking, margin calculation

### 2. Natural Gas Prices  
- Henry Hub Spot Price (DHHNGSP)
- Used for: Energy cost component

### 3. Producer Price Index - Chemicals
- PPI: Chemical Manufacturing (PCU325325)
- Used for: Industry inflation adjustment

### 4. Economic Indicators
- Industrial Production Index (INDPRO)
- Durable Goods Orders (DGORDER)
- Housing Starts (HOUST)
- Consumer Sentiment (UMCSENT)
- Used for: Demand forecasting

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CHEMICALS_DB                              │
├─────────────────────────────────────────────────────────────┤
│  CHEMICAL_OPS Schema                                         │
│  ┌─────────────────────┐    ┌──────────────────────────┐   │
│  │ MARKETPLACE_*       │    │ MARGIN_ANALYZER          │   │
│  │ Views               │───▶│ (enhanced with real      │   │
│  │ (crude oil, gas,    │    │  market data)            │   │
│  │  PPI, indicators)   │    └──────────────────────────┘   │
│  └─────────────────────┘                                    │
│           ▲                                                  │
│           │                                                  │
├───────────┼─────────────────────────────────────────────────┤
│           │           EXTERNAL (Shared Database)             │
│  ┌────────┴────────┐                                        │
│  │    CYBERSYN     │  ◄── Snowflake Marketplace             │
│  │ (or KNOEMA etc) │      (Free data share)                 │
│  └─────────────────┘                                        │
└─────────────────────────────────────────────────────────────┘
```

## Programmatic Installation (Advanced)

For automated deployment:

```bash
python3 marketplace/install_cybersyn.py \
    --pat-token "$SNOWFLAKE_PAT_TOKEN" \
    --install cybersyn_economic \
    --database-name CYBERSYN
```

Note: The Listing API may require specific privileges and terms acceptance.

## Files

| File | Purpose |
|------|---------|
| `get_marketplace_listings.sql` | Discovery and manual installation guide |
| `create_marketplace_views.sql` | Integration views (run after install) |
| `install_cybersyn.py` | Programmatic installation script |

## Verification

After setup, verify with:

```sql
-- Check if marketplace database exists
SHOW DATABASES LIKE '%CYBERSYN%';

-- Check available data
SELECT COUNT(*) FROM CYBERSYN.ECONOMY.FRED_TIMESERIES 
WHERE series_id IN ('DCOILWTICO', 'DCOILBRENTEU', 'DHHNGSP');

-- Test crude oil prices
SELECT date, series_id, value 
FROM CYBERSYN.ECONOMY.FRED_TIMESERIES 
WHERE series_id = 'DCOILWTICO'
ORDER BY date DESC 
LIMIT 10;
```
