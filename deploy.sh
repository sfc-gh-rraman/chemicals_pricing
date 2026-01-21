#!/bin/bash
# =============================================================================
# Deployment Script: Chemicals Integrated Pricing & Supply Chain
# =============================================================================
# This script deploys the complete solution to Snowflake
# 
# Prerequisites:
# - Snowflake CLI (snow) installed and configured
# - Python 3.9+ with pandas, numpy
# - ACCOUNTADMIN or equivalent privileges
# =============================================================================

set -e

echo "=============================================="
echo "Chemicals Pricing & Supply Chain Deployment"
echo "=============================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTION="${SNOW_CONNECTION:-default}"

echo -e "${YELLOW}Using Snowflake connection: ${CONNECTION}${NC}"
echo ""

# =============================================================================
# Step 1: Deploy DDL
# =============================================================================
echo -e "${GREEN}Step 1: Deploying DDL scripts...${NC}"

snow sql -f "$SCRIPT_DIR/ddl/core/001_DATABASE_SCHEMAS.sql" --connection "$CONNECTION"
snow sql -f "$SCRIPT_DIR/ddl/core/002_RAW_TABLES.sql" --connection "$CONNECTION"
snow sql -f "$SCRIPT_DIR/ddl/core/003_ATOMIC_CORE.sql" --connection "$CONNECTION"
snow sql -f "$SCRIPT_DIR/ddl/core/004_ATOMIC_EXTENSIONS.sql" --connection "$CONNECTION"
snow sql -f "$SCRIPT_DIR/ddl/core/005_CHEMICAL_OPS_MART.sql" --connection "$CONNECTION"
snow sql -f "$SCRIPT_DIR/ddl/core/006_ML_PREDICTIONS.sql" --connection "$CONNECTION"

echo -e "${GREEN}✓ DDL scripts deployed${NC}"
echo ""

# =============================================================================
# Step 2: Generate synthetic data
# =============================================================================
echo -e "${GREEN}Step 2: Generating synthetic data...${NC}"

cd "$SCRIPT_DIR/scripts"
python generate_synthetic_data.py --output-dir ../data/synthetic --days 365

echo -e "${GREEN}✓ Synthetic data generated${NC}"
echo ""

# =============================================================================
# Step 3: Upload data to stage
# =============================================================================
echo -e "${GREEN}Step 3: Uploading data to Snowflake stage...${NC}"

snow sql -q "USE DATABASE CHEMICALS_DB; USE SCHEMA RAW;" --connection "$CONNECTION"

# Upload all CSV files
for csv_file in "$SCRIPT_DIR/data/synthetic"/*.csv; do
    filename=$(basename "$csv_file")
    echo "  Uploading $filename..."
    snow stage put "$csv_file" "@RAW.DATA_STAGE" --connection "$CONNECTION" --overwrite
done

echo -e "${GREEN}✓ Data uploaded to stage${NC}"
echo ""

# =============================================================================
# Step 4: Load data into tables
# =============================================================================
echo -e "${GREEN}Step 4: Loading data into tables...${NC}"

snow sql -f "$SCRIPT_DIR/data/load_all_data.sql" --connection "$CONNECTION"

echo -e "${GREEN}✓ Data loaded into tables${NC}"
echo ""

# =============================================================================
# Step 5: Deploy Cortex services
# =============================================================================
echo -e "${GREEN}Step 5: Deploying Cortex services...${NC}"

# Deploy Cortex Analyst semantic model
snow sql -f "$SCRIPT_DIR/cortex/deploy_cortex.sql" --connection "$CONNECTION"

# Deploy Cortex Search service
snow sql -f "$SCRIPT_DIR/cortex/deploy_search.sql" --connection "$CONNECTION"

echo -e "${GREEN}✓ Cortex services deployed${NC}"
echo ""

# =============================================================================
# Step 6: Deploy Streamlit app
# =============================================================================
echo -e "${GREEN}Step 6: Deploying Streamlit application...${NC}"

cd "$SCRIPT_DIR/streamlit"
snow streamlit deploy --database CHEMICALS_DB --schema CHEMICAL_OPS --connection "$CONNECTION"

echo -e "${GREEN}✓ Streamlit app deployed${NC}"
echo ""

# =============================================================================
# Deployment complete
# =============================================================================
echo "=============================================="
echo -e "${GREEN}✓ DEPLOYMENT COMPLETE${NC}"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Open Snowsight: https://app.snowflake.com"
echo "  2. Navigate to: Projects > Streamlit > CHEMICALS_PRICING_APP"
echo "  3. For Cortex Analyst: AI & ML > Cortex Analyst > CHEMICALS_PRICING_MODEL"
echo ""
echo "Golden Query to test:"
echo '  "Show me the average margin for Ethylene products in Europe vs North America for Q3"'
echo ""
