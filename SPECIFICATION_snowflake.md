# ETF Intelligence Portal — Implementation Specification

## How to Use This Document (Read First)

This spec is designed for **parallel implementation by multiple AI coding agents**.
Each agent is assigned a bounded scope with explicit file ownership, clear inputs/outputs,
and integration contracts. Agents MUST NOT modify files outside their assigned scope.

### Implementation Order

```
Step 1 — Foundation Agent (MUST complete first or define contracts before others start)
    |
    +-- Step 2 (all parallel, depend on Foundation contracts only)
    |       Agent A: ETF Screener
    |       Agent B: ETF Profile Page
    |       Agent C: Security Profile Page
    |
    +-- Step 3 (parallel, depend on Step 2 API routes being defined)
    |       Agent D: Holdings Overlap Tool
    |       Agent E: Issuer Page + Historical Holdings
    |
    +-- Step 4 (independent, can run fully in parallel with Steps 2 & 3)
            Agent F: AI Research Assistant
            Agent G: Inactive Securities Monitor + Alerts
```

---

## Project Overview

**Product:** ETF Intelligence Portal
**Stack:** Next.js 14 (App Router) + FastAPI (Python) + Snowflake
**Database:** Snowflake — `ETF_DB.LOCAL_COPY` schema
**Deployment:** Vercel (frontend) + Railway or Render (API)
**Repo layout:**

```
/
├── SPECIFICATION.md          (this file — read-only for all agents)
├── web/                      (Next.js frontend — Agent ownership varies by page)
│   ├── app/
│   │   ├── layout.tsx        (Foundation)
│   │   ├── page.tsx          (Foundation — home)
│   │   ├── etfs/             (Agent A)
│   │   ├── etf/[ticker]/     (Agent B)
│   │   ├── security/[ticker]/(Agent C)
│   │   ├── compare/          (Agent D)
│   │   ├── issuer/[name]/    (Agent E)
│   │   ├── research/         (Agent F)
│   │   └── monitor/          (Agent G)
│   ├── components/
│   │   ├── shared/           (Foundation — read-only after creation)
│   │   ├── etf-screener/     (Agent A)
│   │   ├── etf-profile/      (Agent B)
│   │   ├── security-profile/ (Agent C)
│   │   ├── overlap/          (Agent D)
│   │   ├── issuer/           (Agent E)
│   │   ├── research/         (Agent F)
│   │   └── monitor/          (Agent G)
│   ├── lib/
│   │   ├── types.ts          (Foundation — read-only after creation)
│   │   ├── api-client.ts     (Foundation — read-only after creation)
│   │   └── utils.ts          (Foundation — read-only after creation)
│   └── package.json          (Foundation)
├── api/                      (FastAPI backend)
│   ├── main.py               (Foundation)
│   ├── db.py                 (Foundation — read-only after creation)
│   ├── models.py             (Foundation — read-only after creation)
│   ├── routers/
│   │   ├── etfs.py           (Agent A)
│   │   ├── etf_detail.py     (Agent B)
│   │   ├── securities.py     (Agent C)
│   │   ├── overlap.py        (Agent D)
│   │   ├── issuers.py        (Agent E)
│   │   ├── research.py       (Agent F)
│   │   └── monitor.py        (Agent G)
│   ├── requirements.txt      (Foundation)
│   └── .env.example          (Foundation)
└── ETF_DB/                   (existing Snowflake scripts — DO NOT MODIFY)
```

---

## Snowflake Data Reference (Read-Only — All Agents)

**Connection:** Uses `SNOWFLAKE_*` environment variables (see `.env.example`)
**Warehouse:** `cortex_analyst_wh`
**Database.Schema:** `ETF_DB.LOCAL_COPY`

### Table: INDUSTRY (~4,152 rows)
Primary key: `(AS_OF_DATE, COMPOSITE_TICKER)`

| Column | Type | Description |
|--------|------|-------------|
| AS_OF_DATE | DATE | Snapshot date |
| COMPOSITE_TICKER | TEXT | ETF ticker |
| DESCRIPTION | TEXT | ETF full name |
| ISSUER | TEXT | Fund company (BlackRock, Vanguard, etc.) |
| ASSET_CLASS | TEXT | Equity, Fixed Income, Alternatives, Multi-Asset |
| CATEGORY | TEXT | Morningstar category |
| FOCUS | TEXT | Investment theme |
| REGION | TEXT | Geographic region |
| DEVELOPMENT_CLASS | TEXT | Developed, Emerging, Frontier, Blend |
| SECTOR_EXPOSURE | TEXT | GICS sector |
| INDUSTRY_EXPOSURE | TEXT | GICS industry |
| LISTING_EXCHANGE | TEXT | NYSE Arca, NASDAQ, BATS |
| IS_LEVERAGED | BOOLEAN | Leveraged fund flag |
| IS_ACTIVE | BOOLEAN | Currently trading |
| IS_ETN | BOOLEAN | ETN vs ETF flag |
| DISTRIBUTION_FREQUENCY | TEXT | Monthly, Quarterly, Annual, etc. |
| TAX_CLASSIFICATION | TEXT | RIC, Grantor Trust |
| PRIMARY_BENCHMARK | TEXT | Tracked index |
| AUM | FLOAT | Assets under management (USD) |
| NUM_HOLDINGS | INTEGER | Number of constituent securities |
| MANAGEMENT_FEE | FLOAT | Management fee (basis points) |
| NET_EXPENSES | FLOAT | Net expense ratio (basis points) |
| TOTAL_EXPENSES | FLOAT | Total expense ratio (basis points) |
| BID_ASK_SPREAD | FLOAT | Bid-ask spread |
| SHORT_INTEREST | FLOAT | Short interest |
| DISCOUNT_PREMIUM | FLOAT | Discount/premium to NAV |
| INCEPTION_DATE | DATE | Fund launch date |

### Table: CONSTITUENTS (~1.17M rows)
Primary key: `(AS_OF_DATE, COMPOSITE_TICKER, CONSTITUENT_TICKER)`

| Column | Type | Description |
|--------|------|-------------|
| AS_OF_DATE | DATE | Holdings snapshot date |
| COMPOSITE_TICKER | TEXT | ETF ticker |
| CONSTITUENT_TICKER | TEXT | Held security ticker |
| CONSTITUENT_NAME | TEXT | Security full name |
| ASSET_CLASS | TEXT | Security asset class |
| SECURITY_TYPE | TEXT | Common Stock, Bond, ETF, ADR, etc. |
| COUNTRY_OF_EXCHANGE | TEXT | ISO country code (US, GB, JP, …) |
| EXCHANGE | TEXT | Trading venue |
| CURRENCY_TRADED | TEXT | ISO currency (USD, EUR, GBP, …) |
| CUSIP | TEXT | CUSIP identifier (nullable) |
| ISIN | TEXT | ISIN identifier (nullable) |
| WEIGHT | FLOAT | Portfolio weight (0.0–1.0) |
| MARKET_VALUE | FLOAT | Holding value in USD |
| SHARES_HELD | FLOAT | Number of shares held |

### Table: SECURITIES (~113,855 rows)
Primary key: `CONSTITUENT_TICKER`

| Column | Type | Description |
|--------|------|-------------|
| CONSTITUENT_TICKER | TEXT | Unique ticker (PK) |
| CONSTITUENT_NAME | TEXT | Security name |
| ASSET_CLASS | TEXT | Security asset class |
| SECURITY_TYPE | TEXT | Common Stock, Bond, etc. |
| COUNTRY_OF_EXCHANGE | TEXT | Country |
| EXCHANGE | TEXT | Exchange |
| CURRENCY_TRADED | TEXT | Currency |
| CUSIP | TEXT | CUSIP (nullable) |
| ISIN | TEXT | ISIN (nullable) |
| EDGAR_CIK | TEXT | SEC CIK number (nullable) |
| EDGAR_NAME | TEXT | Official SEC company name (nullable) |
| SIC_CODE | TEXT | SIC code (nullable) |
| SIC_DESCRIPTION | TEXT | SIC description (nullable) |
| STATE_OF_INC | TEXT | State of incorporation (nullable) |
| EIN | TEXT | Employer Identification Number (nullable) |
| ENTITY_TYPE | TEXT | Corporate structure (nullable) |
| LISTED_EXCHANGES | TEXT | Pipe-separated exchange list (nullable) |
| FILER_CATEGORY | TEXT | SEC filer category (nullable) |
| ACTIVE_FLAG | BOOLEAN | True = currently listed/trading (nullable) |
| INACTIVE_REASON | TEXT | Reason code if inactive (nullable) |
| SHARES_OUTSTANDING | NUMBER | Latest reported shares (nullable) |
| SHARES_AS_OF_DATE | DATE | Date of shares data (nullable) |
| EDGAR_ENRICHED_AT | TIMESTAMP_NTZ | When EDGAR data was fetched (nullable) |

### Useful SQL Patterns

```sql
-- Latest snapshot date (use this everywhere, do not hardcode dates)
SELECT MAX(AS_OF_DATE) AS latest_date FROM ETF_DB.LOCAL_COPY.INDUSTRY;

-- Latest holdings for a specific ETF
SELECT * FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
WHERE COMPOSITE_TICKER = :ticker
  AND AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
                    WHERE COMPOSITE_TICKER = :ticker)
ORDER BY WEIGHT DESC;

-- Latest INDUSTRY row per ETF
SELECT * FROM ETF_DB.LOCAL_COPY.INDUSTRY
QUALIFY ROW_NUMBER() OVER (PARTITION BY COMPOSITE_TICKER ORDER BY AS_OF_DATE DESC) = 1;
```

---

## Shared Contracts (Foundation Agent Defines — All Agents Consume)

### TypeScript Types (`web/lib/types.ts`)

```typescript
// ETF summary (used in screener results, issuer pages)
export interface ETFSummary {
  ticker: string;
  name: string;
  issuer: string;
  assetClass: string;
  category: string | null;
  region: string | null;
  sector: string | null;
  aum: number | null;           // USD
  expenseRatio: number | null;  // basis points
  numHoldings: number | null;
  isLeveraged: boolean;
  isActive: boolean;
  isEtn: boolean;
  inceptionDate: string | null; // ISO date string
  listingExchange: string | null;
}

// Full ETF detail (used on profile page)
export interface ETFDetail extends ETFSummary {
  focus: string | null;
  developmentClass: string | null;
  industryExposure: string | null;
  taxClassification: string | null;
  distributionFrequency: string | null;
  primaryBenchmark: string | null;
  managementFee: number | null;
  netExpenses: number | null;
  totalExpenses: number | null;
  bidAskSpread: number | null;
  shortInterest: number | null;
  discountPremium: number | null;
  asOfDate: string;
}

// Single holding row
export interface Holding {
  ticker: string;
  name: string;
  assetClass: string;
  securityType: string;
  weight: number;          // 0.0–1.0
  marketValue: number | null;
  sharesHeld: number | null;
  country: string | null;
  exchange: string | null;
}

// Security master record
export interface Security {
  ticker: string;
  name: string;
  assetClass: string;
  securityType: string;
  country: string | null;
  exchange: string | null;
  currency: string | null;
  cusip: string | null;
  isin: string | null;
  edgarCik: string | null;
  edgarName: string | null;
  sicCode: string | null;
  sicDescription: string | null;
  stateOfInc: string | null;
  ein: string | null;
  entityType: string | null;
  listedExchanges: string | null;
  filerCategory: string | null;
  activeFlag: boolean | null;
  inactiveReason: string | null;
  sharesOutstanding: number | null;
  sharesAsOfDate: string | null;
}

// ETF membership (used on security profile)
export interface ETFMembership {
  etfTicker: string;
  etfName: string;
  weight: number;
  marketValue: number | null;
  sharesHeld: number | null;
  asOfDate: string;
}

// Overlap result (used by overlap tool)
export interface OverlapResult {
  etfA: string;
  etfB: string;
  overlapByWeight: number;    // % of combined portfolio that overlaps
  overlapByCount: number;     // number of shared securities
  sharedHoldings: {
    ticker: string;
    name: string;
    weightInA: number;
    weightInB: number;
  }[];
  uniqueToA: Holding[];
  uniqueToB: Holding[];
}

// Screener filter params
export interface ScreenerFilters {
  assetClass?: string[];
  issuer?: string[];
  region?: string[];
  sector?: string[];
  minExpenseRatio?: number;   // bps
  maxExpenseRatio?: number;   // bps
  minAum?: number;            // USD
  maxAum?: number;            // USD
  developmentClass?: string[];
  isLeveraged?: boolean;
  isEtn?: boolean;
  isActive?: boolean;
}

// Paginated API response wrapper
export interface PagedResponse<T> {
  data: T[];
  total: number;
  page: number;
  pageSize: number;
}

// AI research response
export interface ResearchResponse {
  question: string;
  answer: string;
  sql: string | null;
  data: Record<string, unknown>[] | null;
  suggestions: string[];
}

// Alert (Phase 4)
export interface Alert {
  id: string;
  type: 'inactive_security' | 'aum_change' | 'new_holding' | 'dropped_holding';
  severity: 'info' | 'warning' | 'critical';
  ticker: string;
  message: string;
  detectedAt: string;   // ISO datetime
  etfsAffected: string[];
}
```

### API Base URL
All API calls go through `web/lib/api-client.ts`:
```typescript
const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';
```

### Python Models (`api/models.py`)
Pydantic models that mirror the TypeScript types above. Foundation Agent creates these.
All routers import from `api/models.py` — never define response models inline.

### Environment Variables (`.env.example`)
```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=cortex_analyst_wh
SNOWFLAKE_DATABASE=ETF_DB
SNOWFLAKE_SCHEMA=LOCAL_COPY
CORTEX_ANALYST_MODEL_URL=https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message
CORTEX_SEMANTIC_MODEL=@ETF_DB.LOCAL_COPY.cortex_stage/analyst2_semantic_model.yaml
NEXT_PUBLIC_API_URL=http://localhost:8000
```

---

## Agent Assignments

---

### Foundation Agent

**Phase:** Pre-requisite (complete before other agents start)
**Owns:**
- `web/` — Next.js project scaffold, `package.json`, `tailwind.config.ts`, root `layout.tsx`, home `page.tsx`
- `web/lib/types.ts` — all TypeScript types (paste the block above verbatim)
- `web/lib/api-client.ts` — typed fetch wrapper
- `web/lib/utils.ts` — formatters (formatAUM, formatBps, formatDate, formatPct)
- `web/components/shared/` — NavBar, Footer, SearchBar, StatCard, DataTable, LoadingSpinner, ErrorBoundary, Badge
- `api/main.py` — FastAPI app init, CORS, router registration stubs
- `api/db.py` — Snowflake connection pool using `snowflake-connector-python`
- `api/models.py` — Pydantic models mirroring TypeScript types
- `api/requirements.txt`
- `api/.env.example`

**Deliverables:**
1. Working Next.js app that loads at `localhost:3000` with nav and home page
2. FastAPI at `localhost:8000` with `/health` endpoint returning `{"status": "ok"}`
3. `GET /api/meta` endpoint returning: `{ latestDate, totalEtfs, totalSecurities, totalAum }`
4. All shared types and Pydantic models matching the contracts above exactly
5. `api-client.ts` with `apiFetch<T>(path, params?)` typed helper

**DO NOT implement** any feature-specific pages or routers — leave those to feature agents.

---

### Agent A — ETF Screener

**Phase:** 1
**Owns:** `web/app/etfs/`, `web/components/etf-screener/`, `api/routers/etfs.py`

**API Endpoints to implement:**

```
GET /api/etfs
  Query params:
    page (int, default 1)
    pageSize (int, default 50, max 200)
    sortBy (string: aum|expenseRatio|numHoldings|ticker, default aum)
    sortDir (string: asc|desc, default desc)
    assetClass (string, comma-separated)
    issuer (string, comma-separated)
    region (string, comma-separated)
    sector (string, comma-separated)
    minExpenseRatio (float, bps)
    maxExpenseRatio (float, bps)
    minAum (float, USD)
    maxAum (float, USD)
    developmentClass (string, comma-separated)
    isLeveraged (bool)
    isEtn (bool)
    isActive (bool)
    q (string, free-text search on ticker + name)
  Returns: PagedResponse<ETFSummary>

GET /api/etfs/filter-options
  Returns:
    {
      assetClasses: string[],
      issuers: string[],
      regions: string[],
      sectors: string[],
      developmentClasses: string[]
    }
  Note: Only return non-null values with count >= 3
```

**UI Page (`/etfs`):**
- Left sidebar: filter panel using filter-options data
  - Multi-select dropdowns for categorical filters
  - Range sliders for AUM and expense ratio
  - Toggle switches for boolean filters
  - "Clear All" button
- Main area: results table using `DataTable` shared component
  - Columns: Ticker (linked to `/etf/{ticker}`), Name, Issuer, AUM, Expense Ratio, Holdings, Region, Asset Class
  - Client-side sort on all columns
  - Pagination controls
  - Row count display ("Showing X of Y ETFs")
- Top bar: search input (filters table as user types)
- Export CSV button (downloads current filtered set)

**SQL pattern to use:**
```sql
SELECT
    i.COMPOSITE_TICKER, i.DESCRIPTION, i.ISSUER, i.ASSET_CLASS,
    i.CATEGORY, i.REGION, i.DEVELOPMENT_CLASS, i.SECTOR_EXPOSURE,
    i.AUM, i.NET_EXPENSES, i.NUM_HOLDINGS, i.IS_LEVERAGED, i.IS_ACTIVE,
    i.IS_ETN, i.LISTING_EXCHANGE, i.INCEPTION_DATE
FROM ETF_DB.LOCAL_COPY.INDUSTRY i
QUALIFY ROW_NUMBER() OVER (PARTITION BY i.COMPOSITE_TICKER ORDER BY i.AS_OF_DATE DESC) = 1
WHERE 1=1
  -- append filter clauses dynamically
ORDER BY {sortBy} {sortDir}
LIMIT :pageSize OFFSET :offset
```

---

### Agent B — ETF Profile Page

**Phase:** 1
**Owns:** `web/app/etf/[ticker]/`, `web/components/etf-profile/`, `api/routers/etf_detail.py`

**API Endpoints to implement:**

```
GET /api/etf/{ticker}
  Returns: ETFDetail

GET /api/etf/{ticker}/holdings
  Query params:
    limit (int, default 25, max 500)
    assetClass (string filter, optional)
  Returns:
    {
      asOfDate: string,
      totalHoldings: number,
      top10Weight: number,    // sum of top 10 weights
      top25Weight: number,
      holdings: Holding[]
    }

GET /api/etf/{ticker}/holdings/history
  Returns:
    {
      dates: string[],
      holdingCounts: number[],
      top10Weights: number[]  // concentration over time
    }

GET /api/etf/{ticker}/exposure
  Returns:
    {
      bySector: { label: string, weight: number }[],
      byCountry: { label: string, weight: number }[],
      byCurrency: { label: string, weight: number }[],
      byAssetClass: { label: string, weight: number }[]
    }
  Note: Aggregate from CONSTITUENTS, join SECURITIES for metadata
```

**UI Page (`/etf/{ticker}`):**

**Header section:**
- Ticker (large), full name, issuer, listing exchange
- Active/Inactive badge (from `IS_ACTIVE`)
- Inception date

**KPI cards row (use shared `StatCard`):**
- AUM (formatted: "$12.4B")
- Expense Ratio ("15 bps")
- Holdings count
- Primary Benchmark
- Distribution Frequency

**Tab navigation:**

**Tab 1: Holdings**
- Top holdings table: Rank | Ticker (linked to `/security/{ticker}`) | Name | Weight | Market Value | Shares Held
- Bar chart: Top 10 weights
- Concentration summary: "Top 10 = X%, Top 25 = Y%, Top 50 = Z%"
- "Show all holdings" toggle (expands to full list)

**Tab 2: Exposure**
- Horizontal bar: sector breakdown (sorted descending)
- Horizontal bar: top 15 country breakdown
- Pie: currency distribution
- Pie: asset class mix (Equity/Fixed Income/Cash/Other)

**Tab 3: History**
- Line chart: holdings count over time
- Line chart: top-10 weight concentration over time
- Note: only show if >1 AS_OF_DATE exists for this ETF

**Tab 4: Fund Info**
- Full metadata table: all INDUSTRY columns formatted neatly
- Fee breakdown: Management / Net / Total expense ratio
- Trading stats: bid-ask spread, short interest, discount/premium

---

### Agent C — Security Profile Page

**Phase:** 1
**Owns:** `web/app/security/[ticker]/`, `web/components/security-profile/`, `api/routers/securities.py`

**API Endpoints to implement:**

```
GET /api/security/{ticker}
  Returns: Security

GET /api/security/{ticker}/etfs
  Query params:
    limit (int, default 50)
    sortBy (string: weight|marketValue|etfAum, default weight)
  Returns:
    {
      asOfDate: string,
      etfCount: number,
      totalExposure: number,   // sum of (weight * ETF AUM) across all ETFs
      memberships: ETFMembership[]
    }

GET /api/security/search
  Query params:
    q (string, required, min 2 chars)
    limit (int, default 10)
  Returns: { ticker: string, name: string, assetClass: string }[]
  Note: Search on CONSTITUENT_TICKER ILIKE and CONSTITUENT_NAME ILIKE
```

**UI Page (`/security/{ticker}`):**

**Header:**
- Ticker, full name
- Active badge (green "Active" or red "Inactive: {reason}")
- Asset class + security type tags

**Identity card (2-column grid):**
- CUSIP, ISIN
- Country of Exchange, Currency
- SEC CIK (hyperlinked to `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}`)
- SIC Code + Description
- State of Incorporation
- Entity Type
- Filer Category
- EIN
- Shares Outstanding + as-of date
- EDGAR enriched at timestamp

**ETF Memberships section:**
- Summary: "Held by X ETFs | Avg weight Y% | Max weight Z% (in {ETF})"
- Sortable table: ETF Ticker (linked) | ETF Name | Weight % | Market Value | Shares Held | As Of
- Bar chart: top 20 ETFs by weight

---

### Agent D — Holdings Overlap Tool

**Phase:** 2 (can start once Foundation Agent contracts exist)
**Owns:** `web/app/compare/`, `web/components/overlap/`, `api/routers/overlap.py`

**API Endpoints to implement:**

```
GET /api/overlap
  Query params:
    tickers (string, comma-separated, 2–4 ETF tickers, required)
  Returns: OverlapResult[]
  Note: Return one OverlapResult per pair of tickers

POST /api/overlap
  Body: { tickers: string[] }
  Returns: same as GET
```

**Overlap calculation SQL (per pair):**
```sql
WITH a AS (
    SELECT CONSTITUENT_TICKER, WEIGHT FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
    WHERE COMPOSITE_TICKER = :etfA
      AND AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS WHERE COMPOSITE_TICKER = :etfA)
),
b AS (
    SELECT CONSTITUENT_TICKER, WEIGHT FROM ETF_DB.LOCAL_COPY.CONSTITUENTS
    WHERE COMPOSITE_TICKER = :etfB
      AND AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM ETF_DB.LOCAL_COPY.CONSTITUENTS WHERE COMPOSITE_TICKER = :etfB)
),
shared AS (
    SELECT a.CONSTITUENT_TICKER, a.WEIGHT AS weight_a, b.WEIGHT AS weight_b
    FROM a JOIN b ON a.CONSTITUENT_TICKER = b.CONSTITUENT_TICKER
)
SELECT
    COUNT(*) AS overlap_count,
    SUM(weight_a) AS overlap_weight_in_a,
    SUM(weight_b) AS overlap_weight_in_b
FROM shared;
```

**UI Page (`/compare`):**
- Ticker input bar: add 2–4 ETF tickers (autocomplete from `/api/etfs?q=...`)
- "Compare" button triggers API call

**Results layout:**

If 2 ETFs selected:
- Overlap score card: "X% portfolio overlap by weight, Y shared securities"
- Two-column table: shared holdings with side-by-side weights
- Section: "Only in {ETF A}" — holdings table
- Section: "Only in {ETF B}" — holdings table

If 3–4 ETFs selected:
- Heatmap matrix of pairwise overlap scores
- Shared holdings table (securities in ALL selected ETFs)

---

### Agent E — Issuer Page + Historical Holdings

**Phase:** 2
**Owns:** `web/app/issuer/[name]/`, `web/components/issuer/`, `api/routers/issuers.py`
**Also adds:** Historical tab to ETF Profile (coordinate with Agent B on tab interface only)

**API Endpoints to implement:**

```
GET /api/issuers
  Returns:
    {
      issuers: {
        name: string,
        etfCount: number,
        totalAum: number,
        assetClasses: string[]
      }[]
    }
  Sorted by totalAum DESC

GET /api/issuer/{name}
  Returns:
    {
      name: string,
      totalAum: number,
      etfCount: number,
      etfs: ETFSummary[]
    }

GET /api/etf/{ticker}/holdings/trends
  Returns:
    {
      snapshots: {
        date: string,
        holdingCount: number,
        top10Weight: number,
        aum: number | null
      }[]
    }
  Note: This endpoint is added to etf_detail.py router by Agent E
  Coordinate with Agent B — do NOT touch Agent B's other endpoints
```

**UI Page (`/issuer/{name}`):**
- Header: Issuer name, total AUM, ETF count
- KPI cards: Total AUM | ETF Count | Avg Expense Ratio | Largest Fund
- Breakdown charts: Asset class distribution (pie), AUM by asset class (bar)
- ETF list table (reuse shared DataTable component)
- Link to screener pre-filtered to this issuer

---

### Agent F — AI Research Assistant

**Phase:** 3 (independent — can run in parallel with Phases 1 & 2)
**Owns:** `web/app/research/`, `web/components/research/`, `api/routers/research.py`

**API Endpoints to implement:**

```
POST /api/research/ask
  Body: { question: string, conversationId?: string }
  Returns: ResearchResponse
  Note: Proxies to Snowflake Cortex Analyst REST API

GET /api/research/suggestions
  Returns: { suggestions: string[] }
  Note: Return 6 pre-written starter questions (hardcoded is fine)
```

**Cortex Analyst integration (`api/routers/research.py`):**
```python
# Snowflake Cortex Analyst REST endpoint
CORTEX_URL = f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/api/v2/cortex/analyst/message"
SEMANTIC_MODEL = "@ETF_DB.LOCAL_COPY.cortex_stage/analyst2_semantic_model.yaml"

# POST body structure
{
  "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
  "semantic_model_file": SEMANTIC_MODEL
}
# Auth: Bearer token from snowflake.connector session
```

**Starter questions (hardcoded):**
1. "Which ETFs have the highest exposure to the Technology sector?"
2. "What are the top 10 most widely held securities across all ETFs?"
3. "Which issuers have the lowest average expense ratios?"
4. "Show me ETFs with more than 500 holdings and AUM over $1 billion"
5. "What is the average weight of AAPL across all ETFs that hold it?"
6. "Which ETFs have more than 30% overlap with SPY?"

**UI Page (`/research`):**
- Full-width chat interface
- Message input at bottom with send button (Enter to submit)
- Conversation history displayed as chat bubbles
- Each AI response shows:
  - Answer text (markdown rendered)
  - Collapsible "View SQL" section
  - Data table (if data returned)
  - Suggested follow-up questions as clickable chips
- Starter questions shown when conversation is empty
- "New conversation" button resets chat

---

### Agent G — Inactive Securities Monitor + Alerts

**Phase:** 4 (independent — can run in parallel with all phases)
**Owns:** `web/app/monitor/`, `web/components/monitor/`, `api/routers/monitor.py`

**API Endpoints to implement:**

```
GET /api/monitor/inactive
  Query params:
    reason (string, optional: not_in_edgar_tickers|deregistration|no_recent_10k_10q)
    assetClass (string, optional)
    page (int, default 1)
    pageSize (int, default 50)
  Returns:
    {
      total: number,
      byReason: { reason: string, count: number }[],
      securities: {
        ticker: string,
        name: string,
        inactiveReason: string,
        assetClass: string,
        securityType: string,
        edgarCik: string | null,
        etfsHolding: string[],   // tickers of ETFs that hold this
        etfCount: number
      }[]
    }

GET /api/monitor/summary
  Returns:
    {
      totalInactive: number,
      totalActive: number,
      totalUnknown: number,    // ACTIVE_FLAG IS NULL
      lastEnrichedAt: string,
      byReason: { reason: string, count: number }[]
    }

GET /api/monitor/etf/{ticker}/inactive
  Returns inactive securities within a specific ETF
    {
      etfTicker: string,
      inactiveCount: number,
      inactiveWeight: number,   // sum of weights of inactive holdings
      inactiveHoldings: {
        ticker: string,
        name: string,
        weight: number,
        inactiveReason: string
      }[]
    }
```

**UI Page (`/monitor`):**

**Summary row:**
- KPI cards: Active Securities | Inactive Securities | Unknown Status | Last Enriched

**Inactive breakdown section:**
- Stacked bar or donut: breakdown by `INACTIVE_REASON`
  - `not_in_edgar_tickers` — label as "Not in SEC EDGAR"
  - `deregistration_*` — label as "Formally Deregistered"
  - `no_recent_10k_10q` — label as "No Recent Filings (Stale)"

**Inactive securities table:**
- Filter by reason (tab or dropdown)
- Columns: Ticker | Name | Asset Class | Reason | ETFs Holding (count, linked) | CIK
- Click row → navigate to `/security/{ticker}`

**ETF-level risk view:**
- Input: search for an ETF ticker
- Show: how many of its holdings are inactive, what % of portfolio weight they represent
- Table of inactive holdings with weight

---

## Coding Standards (All Agents Must Follow)

### General
- TypeScript strict mode (`"strict": true` in tsconfig)
- No `any` types — use `unknown` and narrow, or define proper types
- All API errors return `{ error: string, detail?: string }` with appropriate HTTP status
- Use `loading` / `error` / `data` state pattern in all React components

### Python API
- All endpoints use Pydantic response models imported from `api/models.py`
- Use parameterized queries — NEVER format SQL with f-strings for user input
- Wrap all Snowflake calls in try/except; return HTTP 503 on DB errors
- Add `@router.get(...)` docstrings for auto-generated OpenAPI docs
- Connection pooling: reuse the single connection from `api/db.py`

### Next.js / React
- Use Next.js App Router `fetch` with `cache: 'no-store'` for real-time data
- Use `loading.tsx` files for Suspense boundaries on all route segments
- All data fetching goes through `web/lib/api-client.ts` — no raw `fetch` in components
- Chart components must be dynamically imported (`next/dynamic`, `ssr: false`)
- All tables must handle empty state with a clear "No data" message

### Formatting
- Numbers: AUM → "$12.4B" / "$345M" / "$12K" (auto-scale)
- Expense ratios: "15 bps" or "0.15%"
- Weights: "12.34%" (multiply by 100, 2 decimal places)
- Dates: "Mar 15, 2024" for display, ISO strings in API
- All formatters live in `web/lib/utils.ts` (Foundation creates, all agents import)

### Testing
- Each agent writes at minimum:
  - 1 API integration test (uses test Snowflake connection or mock)
  - 1 component render test (React Testing Library)
  - 1 edge case test (empty data, null fields, single-item lists)
- Test files: co-located with source (`*.test.ts`, `*.test.tsx`)

---

## Integration Checklist (Per Agent, Before Declaring Done)

- [ ] All API endpoints return correct TypeScript-matching shapes
- [ ] All internal links use Next.js `<Link>` component
- [ ] Pages handle `loading` state (skeleton or spinner)
- [ ] Pages handle `error` state (error boundary or inline message)
- [ ] Pages handle empty data state
- [ ] Null/nullable fields never cause runtime errors
- [ ] Mobile responsive (min-width 375px usable)
- [ ] No hardcoded dates or tickers
- [ ] All API calls go through `api-client.ts`
- [ ] No types defined inline — all imported from `lib/types.ts`
- [ ] Router registered in `api/main.py` (coordinate with Foundation Agent)

---

## Parallel Execution Tips for AI Agents

1. **Start with types**: Read `web/lib/types.ts` and `api/models.py` before writing any code.
2. **Mock the API first**: If the API isn't ready, use local mock data matching the exact type shapes.
3. **Never edit shared files** (`types.ts`, `api-client.ts`, `db.py`, `models.py`) without flagging it — propose changes as additive-only.
4. **Prefix your components**: Agent A components live in `components/etf-screener/`, never in `components/shared/`.
5. **Register your router**: Each agent's final step is adding their router to `api/main.py` with `app.include_router(...)`.
6. **Claim your route first**: Add a stub `page.tsx` with a `<h1>` placeholder immediately so other agents know the route exists.
