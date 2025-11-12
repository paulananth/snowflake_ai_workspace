create database etf_db;

create schema etf_db.local_copy;


CREATE OR REPLACE TABLE etf_db.local_copy.CONSTITUENTS AS
SELECT *
FROM ETF_CONSTITUENT_DATA.PUBLIC.CONSTITUENTS;


CREATE OR REPLACE TABLE etf_db.local_copy.INDUSTRY AS
SELECT *
FROM ETF_INDUSTRY_DATA.PUBLIC.INDUSTRY;

CREATE OR REPLACE STAGE etf_db.local_copy.cortex_stage
  COMMENT = 'Stage to store Cortex Analyst schema files'
  DIRECTORY = (ENABLE = TRUE);

LIST @etf_db.local_copy.cortex_stage;
