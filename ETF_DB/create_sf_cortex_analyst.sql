--ETF_DB.LOCAL_COPY.ANALYST2
USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET ENABLE_CORTEX_ANALYST = TRUE;

-- Grants to cortex_user_role omitted (role not needed for this workspace)

-- The semantic model is defined in analyst2_semantic_model.yaml, uploaded to:
--   @ETF_DB.LOCAL_COPY.cortex_stage/analyst2_semantic_model.yaml
-- Cortex Analyst reads it directly via the REST API (no CREATE SEMANTIC VIEW needed).

-- Verify the YAML is in place:
LIST @ETF_DB.LOCAL_COPY.cortex_stage;

-- Describe the semantic model (run only after ANALYST2 has been created)
-- DESC SEMANTIC VIEW ETF_DB.LOCAL_COPY.ANALYST2;

-- Check your Snowflake version and region
SELECT CURRENT_VERSION(), CURRENT_REGION();

