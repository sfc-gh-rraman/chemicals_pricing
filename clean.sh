#!/bin/bash
# =============================================================================
# Cleanup Script: Chemicals Integrated Pricing & Supply Chain
# =============================================================================
# This script removes all demo resources from Snowflake
# WARNING: This will delete all data!
# =============================================================================

set -e

echo "=============================================="
echo "Chemicals Pricing & Supply Chain Cleanup"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CONNECTION="${SNOW_CONNECTION:-default}"

echo -e "${RED}WARNING: This will delete the CHEMICALS_DB database and all data!${NC}"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cleanup cancelled."
    exit 0
fi

echo ""
echo -e "${YELLOW}Cleaning up demo resources...${NC}"

# Drop Streamlit app
snow sql -q "DROP STREAMLIT IF EXISTS CHEMICALS_DB.CHEMICAL_OPS.CHEMICALS_PRICING_APP;" --connection "$CONNECTION" 2>/dev/null || true

# Drop Cortex Search service
snow sql -q "DROP CORTEX SEARCH SERVICE IF EXISTS CHEMICALS_DB.CHEMICAL_OPS.MARKET_INTEL_SEARCH;" --connection "$CONNECTION" 2>/dev/null || true

# Drop the database (this removes all schemas, tables, views)
snow sql -q "DROP DATABASE IF EXISTS CHEMICALS_DB;" --connection "$CONNECTION"

# Clean local synthetic data
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rm -rf "$SCRIPT_DIR/data/synthetic"/*.csv 2>/dev/null || true

echo ""
echo -e "${YELLOW}✓ Cleanup complete${NC}"
echo ""
