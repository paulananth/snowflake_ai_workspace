# Track A README: Streamlit Dashboards (Snowflake-backed)

This track is the "SQL-first" portion of the workspace: a set of Streamlit apps that query Snowflake tables in `ETF_DB.LOCAL_COPY`.

What you get in this track:

- ETF EDA dashboard: `ETF_DB/etf_eda_app.py`
- Securities master dashboard: `ETF_DB/securities_dashboard.py`
- Equity-only securities dashboard: `ETF_DB/equity_dashboard.py`

Important scope note:

- Track A does NOT run the EDGAR enrichment loader. You should only set up the schema/tables required for the dashboards to run.

---

## High-level architecture

```mermaid
flowchart TD
  User[User (browser)] -->|requests UI| Streamlit[Streamlit apps]
  Streamlit -->|queries| Snowflake[Snowflake]
  Snowflake -->|tables| Tables[ETF_DB.LOCAL_COPY]
  Streamlit -->|renders charts| UI[Charts and tables]
```

Streamlit apps connect to Snowflake using:

- Connection name: `myfirstsnow` from `~/.snowflake/config.toml`
- Warehouse: `cortex_analyst_wh` (hardcoded in the apps)

---

## Prerequisites

### 1) Snowflake access

You need an account where you can:

- Run SQL scripts from this repository
- Read the objects in `ETF_DB.LOCAL_COPY`

The apps expect these tables to exist in Snowflake:

- `ETF_DB.LOCAL_COPY.CONSTITUENTS`
- `ETF_DB.LOCAL_COPY.INDUSTRY`
- `ETF_DB.LOCAL_COPY.SECURITIES`

And they expect the upstream source datasets referenced by `create_etf_data.sql` to exist:

- `ETF_CONSTITUENT_DATA.PUBLIC.CONSTITUENTS`
- `ETF_INDUSTRY_DATA.PUBLIC.INDUSTRY`

### 2) Local Snowflake connection config

The Python apps read:

- `~/.snowflake/config.toml`

and look for a connection named `myfirstsnow`.

If you do not already have this connection, create/update it in your local Snowflake config.

### 3) Python tooling

- Python 3.11+ installed
- `uv` installed (the repo run scripts use `uv run`)

---

## One-time Snowflake setup (setup these objects first)

Run the SQL scripts in this order. Replace execution method with whatever you use in your environment (SnowSQL CLI, UI console, CI job).

1. Create warehouse:
   - `ETF_DB/user_setup.sql`
   - Creates/ensures the warehouse `cortex_analyst_wh`

2. Create and load the base tables + stage:
   - `ETF_DB/create_etf_data.sql`
   - Creates `ETF_DB.LOCAL_COPY` and populates:
     - `CONSTITUENTS` from `ETF_CONSTITUENT_DATA.PUBLIC.CONSTITUENTS`
     - `INDUSTRY` from `ETF_INDUSTRY_DATA.PUBLIC.INDUSTRY`
   - Also creates a stage `ETF_DB.LOCAL_COPY.cortex_stage` (used by Cortex in other tracks)

3. Create the deduplicated security master:
   - `ETF_DB/create_securities_table.sql`
   - Builds `ETF_DB.LOCAL_COPY.SECURITIES` as "one row per security ticker" derived from `CONSTITUENTS`

4. Add EDGAR-related columns needed by the dashboards:
   - `ETF_DB/add_edgar_columns.sql`
   - Adds columns like `EDGAR_CIK`, `EDGAR_NAME`, `SIC_CODE`, `ACTIVE_FLAG`, etc.
   - Note: this Track A README does not populate those fields; it only prepares the columns so the dashboards can run.

5. Optional note (not required for Track A dashboards):
   - `ETF_DB/create_sf_cortex_analyst.sql` and `ETF_DB/analyst2_semantic_model.yaml`
   - These are needed for Cortex-based AI features in later tracks, not for the Streamlit dashboards in Track A.

Security warning:

- `ETF_DB/create_user.sql` contains a hardcoded password. Do not use it verbatim for real environments.

---

## Run Track A Streamlit dashboards (repeatable)

These apps all run locally on your machine and query Snowflake.

### Recommended: use the provided PowerShell launchers

Open three terminals and run:

- `run_etf_eda.ps1`
  - runs: `ETF_DB/etf_eda_app.py`
- `run_securities.ps1`
  - runs: `ETF_DB/securities_dashboard.py`
- `run_equity.ps1`
  - runs: `ETF_DB/equity_dashboard.py`

### Non-Windows / raw commands (same behavior)

You can also run them directly (use PowerShell/CMD equivalents as needed):

- ETF EDA:
  - `uv run --with streamlit --with pandas --with plotly --with snowflake-connector-python python -m streamlit run "ETF_DB/etf_eda_app.py"`
- Securities master:
  - `uv run --with streamlit --with pandas --with plotly --with snowflake-connector-python python -m streamlit run "ETF_DB/securities_dashboard.py"`
- Equity-only:
  - `uv run --with streamlit --with pandas --with plotly --with snowflake-connector-python python -m streamlit run "ETF_DB/equity_dashboard.py"`

---

## What to expect in each dashboard

### ETF EDA (`etf_eda_app.py`)

Sidebar sections include:

- Universe overview (ETF counts, missing values in `INDUSTRY`)
- AUM and issuers
- Fee analysis
- Geographic and sector exposure
- Holdings analysis (aggregations from `CONSTITUENTS`)
- Combined analysis (`INDUSTRY` joined with `CONSTITUENTS`)
- Time-series snapshot summaries
- Ticker deep dive (latest snapshot derived in-code)
- Security lookup (given a security ticker, show ETFs that hold it)

### Securities master (`securities_dashboard.py`)

Sidebar sections include:

- Overview (identifier coverage; EDGAR fields may be present but can be empty)
- Active vs inactive
- Industry and SIC
- Geography and exchange
- Shares outstanding
- Security detail lookup (given a ticker, show EDGAR identity fields and ETF holdings)

### Equity dashboard (`equity_dashboard.py`)

Only includes rows where `asset_class = 'Equity'` and adds geography views (global map, US, non-US).

---

## Agent mode (strict checklist)

Use this section if you want an agent to “spin up Track A” end-to-end without making decisions.

### Scope guardrails

- Do NOT run any EDGAR enrichment scripts (for example `ETF_DB/sec_edgar_enrich_load.py`).
- You may run `ETF_DB/add_edgar_columns.sql` to add empty EDGAR columns, because some dashboards reference those columns.

### Inputs the agent must have

- A Snowflake account where the upstream source tables exist:
  - `ETF_CONSTITUENT_DATA.PUBLIC.CONSTITUENTS`
  - `ETF_INDUSTRY_DATA.PUBLIC.INDUSTRY`
- A local Snowflake connection profile named `myfirstsnow` in `~/.snowflake/config.toml`
- Python 3.11+ and `uv`

### Step-by-step procedure

1. Snowflake one-time setup (run in order):
   - `ETF_DB/user_setup.sql`
   - `ETF_DB/create_etf_data.sql`
   - `ETF_DB/create_securities_table.sql`
   - `ETF_DB/add_edgar_columns.sql`

2. Start the dashboards (one terminal per app):
   - `run_etf_eda.ps1`
   - `run_securities.ps1`
   - `run_equity.ps1`

### Acceptance checks (agent should confirm)

- **Snowflake objects exist**:
  - `ETF_DB.LOCAL_COPY.CONSTITUENTS`
  - `ETF_DB.LOCAL_COPY.INDUSTRY`
  - `ETF_DB.LOCAL_COPY.SECURITIES`
- **All three apps start** and show a sidebar without crashing.
- **No missing-column errors** (if you see “invalid identifier” for EDGAR fields, ensure `ETF_DB/add_edgar_columns.sql` ran).

---

## Troubleshooting

### Connection failures

- Symptom: apps error immediately on startup (Snowflake auth/connection errors).
- Fix:
  - Confirm `~/.snowflake/config.toml` has a connection named `myfirstsnow`
  - Confirm your Snowflake user has access to warehouse `cortex_analyst_wh`

### Missing tables/objects

- Symptom: SQL errors like "table does not exist".
- Fix:
  - Run `ETF_DB/create_etf_data.sql` and `ETF_DB/create_securities_table.sql` again

### Missing EDGAR columns referenced by the dashboards

- Symptom: SQL errors like "invalid identifier" for EDGAR fields.
- Fix:
  - Run `ETF_DB/add_edgar_columns.sql` (even though Track A does not populate the values)

### Performance

- Symptom: dashboards feel slow.
- Notes:
  - The apps load some whole-table datasets into cached memory (with TTL). First runs can be slower depending on table size and Snowflake performance.

