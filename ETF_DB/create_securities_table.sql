-- ============================================================
-- SECURITIES master table — ETF_DB.LOCAL_COPY
-- One row per unique constituent ticker.
-- Source: CONSTITUENTS (most-recent snapshot per ticker via QUALIFY).
--
-- Part A: Pre-load validation (read-only profiling)
-- Part B: Create + load with cleaning
--
-- Run: snow sql -f ETF_DB/create_securities_table.sql --connection snowconn
-- ============================================================

-- ── Part A: Validation ──────────────────────────────────────────────────────

-- 1. Nulls and blank tickers
SELECT
    COUNT(*)                                              AS total_rows,
    COUNT_IF(CONSTITUENT_TICKER IS NULL)                  AS null_ticker,
    COUNT_IF(TRIM(CONSTITUENT_TICKER) = '')               AS blank_ticker,
    COUNT_IF(CONSTITUENT_NAME IS NULL)                    AS null_name,
    COUNT_IF(ASSET_CLASS IS NULL)                         AS null_asset_class,
    COUNT_IF(SECURITY_TYPE IS NULL)                       AS null_security_type,
    COUNT_IF(EXCHANGE IS NULL)                            AS null_exchange,
    COUNT_IF(CURRENCY_TRADED IS NULL)                     AS null_currency,
    COUNT_IF(CUSIP IS NULL)                               AS null_cusip,
    COUNT_IF(ISIN IS NULL)                                AS null_isin
FROM etf_db.local_copy.CONSTITUENTS;

-- 2. Distinct values — catch garbage / mixed case / extra whitespace
SELECT TRIM(ASSET_CLASS)     AS val, COUNT(*) AS n FROM etf_db.local_copy.CONSTITUENTS GROUP BY 1 ORDER BY 2 DESC;
SELECT TRIM(SECURITY_TYPE)   AS val, COUNT(*) AS n FROM etf_db.local_copy.CONSTITUENTS GROUP BY 1 ORDER BY 2 DESC;
SELECT TRIM(CURRENCY_TRADED) AS val, COUNT(*) AS n FROM etf_db.local_copy.CONSTITUENTS GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
SELECT TRIM(COUNTRY_OF_EXCHANGE) AS val, COUNT(*) AS n FROM etf_db.local_copy.CONSTITUENTS GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
SELECT TRIM(EXCHANGE)        AS val, COUNT(*) AS n FROM etf_db.local_copy.CONSTITUENTS GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- 3. Ticker format check — flag non-standard characters
SELECT CONSTITUENT_TICKER, COUNT(*) AS n
FROM etf_db.local_copy.CONSTITUENTS
WHERE CONSTITUENT_TICKER != UPPER(TRIM(CONSTITUENT_TICKER))
   OR CONSTITUENT_TICKER RLIKE '.*[^A-Z0-9\\.\\-].*'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- 4. Duplicate ticker check post-deduplication (should return 0 rows)
WITH deduped AS (
    SELECT UPPER(TRIM(CONSTITUENT_TICKER)) AS ticker
    FROM etf_db.local_copy.CONSTITUENTS
    WHERE TRIM(CONSTITUENT_TICKER) != '' AND CONSTITUENT_TICKER IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY UPPER(TRIM(CONSTITUENT_TICKER))
        ORDER BY AS_OF_DATE DESC
    ) = 1
)
SELECT ticker, COUNT(*) AS n FROM deduped GROUP BY 1 HAVING n > 1;

-- ── Part B: Create + load ───────────────────────────────────────────────────
-- Cleaning applied:
--   UPPER(TRIM(...))     on CONSTITUENT_TICKER — normalize case and whitespace
--   TRIM(...)            on all other text fields — strip whitespace
--   NULLIF(TRIM(...), '') on identifiers — convert empty strings to NULL

CREATE OR REPLACE TABLE etf_db.local_copy.SECURITIES AS
SELECT
    UPPER(TRIM(CONSTITUENT_TICKER))     AS CONSTITUENT_TICKER,
    TRIM(CONSTITUENT_NAME)              AS CONSTITUENT_NAME,
    TRIM(ASSET_CLASS)                   AS ASSET_CLASS,
    TRIM(SECURITY_TYPE)                 AS SECURITY_TYPE,
    TRIM(COUNTRY_OF_EXCHANGE)           AS COUNTRY_OF_EXCHANGE,
    TRIM(EXCHANGE)                      AS EXCHANGE,
    TRIM(CURRENCY_TRADED)               AS CURRENCY_TRADED,
    NULLIF(TRIM(CUSIP), '')             AS CUSIP,
    NULLIF(TRIM(ISIN),  '')             AS ISIN
FROM etf_db.local_copy.CONSTITUENTS
WHERE CONSTITUENT_TICKER IS NOT NULL
  AND TRIM(CONSTITUENT_TICKER) != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY UPPER(TRIM(CONSTITUENT_TICKER))
    ORDER BY AS_OF_DATE DESC
) = 1;

-- Declare primary key (informational; Snowflake does not enforce uniqueness)
ALTER TABLE etf_db.local_copy.SECURITIES
    ADD PRIMARY KEY (CONSTITUENT_TICKER);

-- ── Post-load verification ──────────────────────────────────────────────────
SELECT
    COUNT(*)                            AS total_securities,
    COUNT(DISTINCT CONSTITUENT_TICKER)  AS unique_tickers,     -- must equal total_securities
    COUNT_IF(ISIN IS NOT NULL)          AS with_isin,
    COUNT_IF(CUSIP IS NOT NULL)         AS with_cusip,
    COUNT_IF(EXCHANGE IS NULL)          AS missing_exchange,
    COUNT_IF(ASSET_CLASS IS NULL)       AS missing_asset_class
FROM etf_db.local_copy.SECURITIES;
