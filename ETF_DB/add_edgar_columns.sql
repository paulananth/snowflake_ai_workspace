-- ============================================================
-- Add EDGAR enrichment columns to SECURITIES table
-- Run: snow sql -f ETF_DB/add_edgar_columns.sql --connection myfirstsnow
-- ============================================================

ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS EDGAR_CIK          TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS EDGAR_NAME         TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS SIC_CODE           TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS SIC_DESCRIPTION    TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS STATE_OF_INC       TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS EIN                TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS ENTITY_TYPE        TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS LISTED_EXCHANGES   TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS FILER_CATEGORY     TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS ACTIVE_FLAG        BOOLEAN;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS INACTIVE_REASON    TEXT;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS SHARES_OUTSTANDING NUMBER;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS SHARES_AS_OF_DATE  DATE;
ALTER TABLE etf_db.local_copy.SECURITIES ADD COLUMN IF NOT EXISTS EDGAR_ENRICHED_AT  TIMESTAMP_NTZ;

-- Verify
SELECT column_name, data_type
FROM etf_db.information_schema.columns
WHERE table_schema = 'LOCAL_COPY' AND table_name = 'SECURITIES'
ORDER BY ordinal_position;
