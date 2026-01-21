-- =============================================================================
-- CORTEX SEARCH DEPLOYMENT
-- Chemicals Integrated Pricing & Supply Chain Optimization Demo
-- =============================================================================
-- DRD Reference: Section 4.2 - Cortex Search (Unstructured)
-- Service Name: MARKET_INTEL_SEARCH
-- Source Data: Market Analyst Reports (PDF), Competitor Earnings Transcripts, Trade Journals
-- Indexing Strategy: Index by Chemical_Name and Region
-- =============================================================================

USE DATABASE CHEMICALS_DB;
USE SCHEMA CHEMICAL_OPS;

-- =============================================================================
-- 1. CREATE DOCUMENT CHUNKS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS MARKET_REPORT_CHUNKS (
    chunk_id NUMBER AUTOINCREMENT PRIMARY KEY,
    report_id INTEGER,
    report_date DATE,
    report_type VARCHAR(100),
    report_title VARCHAR(500),
    report_source VARCHAR(100),
    chemical_name VARCHAR(100),
    region VARCHAR(50),
    chunk VARCHAR(16000),
    chunk_index INTEGER,
    language VARCHAR(50) DEFAULT 'English',
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Enable change tracking for search service
ALTER TABLE MARKET_REPORT_CHUNKS SET CHANGE_TRACKING = TRUE;

-- =============================================================================
-- 2. POPULATE CHUNKS FROM RAW MARKET REPORTS
-- =============================================================================
INSERT INTO MARKET_REPORT_CHUNKS (
    report_id, report_date, report_type, report_title, report_source,
    chemical_name, region, chunk, chunk_index
)
SELECT 
    report_id,
    TO_DATE(report_date),
    report_type,
    report_title,
    report_source,
    chemical_name,
    region,
    -- Combine title, summary, and content into searchable chunk
    report_title || '\n\n' ||
    'Source: ' || report_source || ' | Date: ' || report_date || ' | Region: ' || region || '\n\n' ||
    'Chemical: ' || chemical_name || '\n\n' ||
    'Summary: ' || report_summary || '\n\n' ||
    'Full Report:\n' || report_content AS chunk,
    1 AS chunk_index
FROM RAW.MARKET_REPORTS;

-- =============================================================================
-- 3. CREATE CORTEX SEARCH SERVICE
-- DRD: Service Name: MARKET_INTEL_SEARCH
-- DRD: Sample Prompt: "What are the supply constraints for Propylene in Asia reported in the last month?"
-- =============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE MARKET_INTEL_SEARCH
    ON chunk
    ATTRIBUTES chemical_name, region, report_date, report_type, report_source
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 hour'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
AS (
    SELECT 
        chunk_id,
        chunk,
        chemical_name,
        region,
        report_date,
        report_type,
        report_source,
        report_title
    FROM MARKET_REPORT_CHUNKS
);

-- =============================================================================
-- 4. VERIFY DEPLOYMENT
-- =============================================================================
SELECT 'MARKET_REPORT_CHUNKS' as table_name, COUNT(*) as rows FROM MARKET_REPORT_CHUNKS;

SELECT chemical_name, region, COUNT(*) as report_count 
FROM MARKET_REPORT_CHUNKS 
GROUP BY chemical_name, region
ORDER BY report_count DESC
LIMIT 20;

-- =============================================================================
-- 5. TEST SEARCH (uncomment after service is ready ~5 mins)
-- =============================================================================
-- Sample query from DRD: "What are the supply constraints for Propylene in Asia reported in the last month?"
/*
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CHEMICALS_DB.CHEMICAL_OPS.MARKET_INTEL_SEARCH',
        '{
            "query": "supply constraints for Propylene in Asia", 
            "columns": ["chunk", "chemical_name", "region", "report_date", "report_source"], 
            "filter": {"@gte": {"report_date": "2024-12-01"}},
            "limit": 5
        }'
    )
)['results'] AS search_results;
*/

-- =============================================================================
-- 6. GRANTS
-- =============================================================================
GRANT SELECT ON TABLE MARKET_REPORT_CHUNKS TO ROLE PUBLIC;
-- Note: Search service permissions are managed separately

SHOW CORTEX SEARCH SERVICES IN SCHEMA CHEMICAL_OPS;

SELECT 'Cortex Search service MARKET_INTEL_SEARCH deployed successfully!' AS status;
