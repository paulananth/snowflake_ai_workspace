# Single Agent — Session Guide

Complete sessions in order. Commit after every session before starting the next.
Sessions 1–3 can be done in any order. Sessions 6–7 can slot in anywhere after Session 0.

---

## Build Order

```
Session 0 — Foundation             (always first — all others depend on it)
    |
    +-- Session 1 — ETF Screener          (Phase 1, parallel-safe with 2 & 3)
    +-- Session 2 — ETF Profile Page      (Phase 1, parallel-safe with 1 & 3)
    +-- Session 3 — Security Profile      (Phase 1, parallel-safe with 1 & 2)
    |
    +-- Session 4 — Overlap Tool          (Phase 2, needs Sessions 1 & 2 done)
    +-- Session 5 — Issuer + History      (Phase 2, needs Sessions 1 & 2 done)
    |
    +-- Session 6 — AI Assistant          (Phase 3, can run after Session 0)
    +-- Session 7 — Monitor + Alerts      (Phase 4, can run after Session 0)
```

---

## Session 0 — Foundation

**Agent:** Foundation
**Estimated time:** 45–60 min
**SPECIFICATION.md section:** Foundation Agent

**Prompt addition:**
```
Do not implement any feature-specific pages or API routers.
Only build the scaffold, shared contracts, and the two base endpoints.
Leave router stubs (commented-out include_router lines) in api/main.py
so other sessions know where to register their routers.
```

**Delivers:**
- Next.js 14 project at `web/` (Tailwind, TypeScript strict mode)
- FastAPI project at `api/` with Snowflake connection
- `web/lib/types.ts` — all shared TypeScript interfaces
- `web/lib/api-client.ts` — typed fetch wrapper
- `web/lib/utils.ts` — formatAUM, formatBps, formatDate, formatPct
- `web/components/shared/` — NavBar, Footer, StatCard, DataTable, Badge, LoadingSpinner, ErrorBoundary
- `GET /health` → `{ "status": "ok" }`
- `GET /api/meta` → `{ latestDate, totalEtfs, totalSecurities, totalAum }`
- Home page at `localhost:3000` with nav and meta stats

**Verify before committing:**
```bash
cd web && npm run build          # zero TypeScript errors
cd api && uvicorn main:app --reload
curl localhost:8000/health       # {"status":"ok"}
curl localhost:8000/api/meta     # returns 4 fields
```

---

## Session 1 — ETF Screener

**Agent:** A
**Estimated time:** 60 min
**SPECIFICATION.md section:** Agent A — ETF Screener
**Depends on:** Session 0 complete and committed

**Prompt addition:**
```
Foundation is already built. Read web/ and api/ to understand existing structure.
Implement Agent A — ETF Screener only.
The shared types in web/lib/types.ts and api/models.py already exist — import them, do not redefine.
```

**Delivers:**
- `GET /api/etfs` with all filter params and pagination
- `GET /api/etfs/filter-options`
- `/etfs` page with sidebar filters, sortable table, search bar, CSV export

**Verify before committing:**
```bash
curl "localhost:8000/api/etfs?pageSize=5"
curl "localhost:8000/api/etfs/filter-options"
# Open localhost:3000/etfs — filters and table should work
```

---

## Session 2 — ETF Profile Page

**Agent:** B
**Estimated time:** 60 min
**SPECIFICATION.md section:** Agent B — ETF Profile Page
**Depends on:** Session 0 complete and committed

**Prompt addition:**
```
Foundation is already built. Read web/ and api/ to understand existing structure.
Implement Agent B — ETF Profile Page only.
Do not modify Agent A's router (api/routers/etfs.py).
```

**Delivers:**
- `GET /api/etf/{ticker}` → ETFDetail
- `GET /api/etf/{ticker}/holdings`
- `GET /api/etf/{ticker}/holdings/history`
- `GET /api/etf/{ticker}/exposure`
- `/etf/[ticker]` page with 4 tabs: Holdings, Exposure, History, Fund Info

**Verify before committing:**
```bash
curl localhost:8000/api/etf/SPY
curl localhost:8000/api/etf/SPY/holdings
curl localhost:8000/api/etf/SPY/exposure
# Open localhost:3000/etf/SPY — all 4 tabs should load
```

---

## Session 3 — Security Profile Page

**Agent:** C
**Estimated time:** 45 min
**SPECIFICATION.md section:** Agent C — Security Profile Page
**Depends on:** Session 0 complete and committed

**Prompt addition:**
```
Foundation is already built. Read web/ and api/ to understand existing structure.
Implement Agent C — Security Profile Page only.
Do not modify Agent A's or Agent B's routers.
```

**Delivers:**
- `GET /api/security/{ticker}` → Security (with EDGAR fields)
- `GET /api/security/{ticker}/etfs` → ETF memberships
- `GET /api/security/search?q=` → autocomplete
- `/security/[ticker]` page with identity card and ETF memberships table

**Verify before committing:**
```bash
curl localhost:8000/api/security/AAPL
curl "localhost:8000/api/security/search?q=app"
curl localhost:8000/api/security/AAPL/etfs
# Open localhost:3000/security/AAPL — identity card and memberships should show
```

---

## Session 4 — Holdings Overlap Tool

**Agent:** D
**Estimated time:** 45 min
**SPECIFICATION.md section:** Agent D — Holdings Overlap Tool
**Depends on:** Sessions 1 & 2 complete and committed

**Prompt addition:**
```
Sessions 0, 1, 2, and 3 are already built. Read web/ and api/ to understand existing structure.
Implement Agent D — Holdings Overlap Tool only.
The /api/etfs endpoint (Agent A) already exists — use it for ticker autocomplete.
Do not modify any existing routers.
```

**Delivers:**
- `GET /api/overlap?tickers=SPY,QQQ`
- `POST /api/overlap` with body `{ tickers: [...] }`
- `/compare` page with ticker input, overlap score card, shared/unique holdings tables
- Heatmap matrix for 3–4 ETF comparison

**Verify before committing:**
```bash
curl "localhost:8000/api/overlap?tickers=SPY,QQQ"
# Open localhost:3000/compare — enter SPY and QQQ, click Compare
```

---

## Session 5 — Issuer Page + Historical Holdings

**Agent:** E
**Estimated time:** 45 min
**SPECIFICATION.md section:** Agent E — Issuer Page + Historical Holdings
**Depends on:** Sessions 1 & 2 complete and committed

**Prompt addition:**
```
Sessions 0, 1, 2, 3, and 4 are already built. Read web/ and api/ to understand existing structure.
Implement Agent E — Issuer Page and Historical Holdings only.
You will ADD one endpoint to api/routers/etf_detail.py (the /trends endpoint).
Do NOT change any of Agent B's existing endpoints in that file — append only.
```

**Delivers:**
- `GET /api/issuers`
- `GET /api/issuer/{name}`
- `GET /api/etf/{ticker}/holdings/trends` (appended to etf_detail.py)
- `/issuer/[name]` page with AUM breakdown and ETF list
- History tab on ETF Profile now populated (was stub from Session 2)

**Verify before committing:**
```bash
curl localhost:8000/api/issuers
curl "localhost:8000/api/issuer/BlackRock"
curl localhost:8000/api/etf/SPY/holdings/trends
# Open localhost:3000/issuer/BlackRock
# Open localhost:3000/etf/SPY — History tab should now show charts
```

---

## Session 6 — AI Research Assistant

**Agent:** F
**Estimated time:** 30 min
**SPECIFICATION.md section:** Agent F — AI Research Assistant
**Depends on:** Session 0 complete and committed (independent of Sessions 1–5)

**Prompt addition:**
```
Session 0 (Foundation) is built. Read web/ and api/ to understand existing structure.
Implement Agent F — AI Research Assistant only.
The Snowflake Cortex Analyst endpoint and semantic model details are in SPECIFICATION.md.
Do not modify any other routers.
```

**Delivers:**
- `POST /api/research/ask` → proxies to Cortex Analyst, returns ResearchResponse
- `GET /api/research/suggestions` → 6 starter questions
- `/research` page with full-width chat UI, markdown rendering, SQL collapsible, follow-up chips

**Verify before committing:**
```bash
curl -X POST localhost:8000/api/research/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the top 5 ETFs by AUM?"}'
# Open localhost:3000/research — type a question and check the response
```

---

## Session 7 — Inactive Securities Monitor + Alerts

**Agent:** G
**Estimated time:** 45 min
**SPECIFICATION.md section:** Agent G — Inactive Securities Monitor + Alerts
**Depends on:** Session 0 complete and committed (independent of Sessions 1–6)

**Prompt addition:**
```
Session 0 (Foundation) is built. Read web/ and api/ to understand existing structure.
Implement Agent G — Inactive Securities Monitor only.
The SECURITIES table already has ACTIVE_FLAG and INACTIVE_REASON columns populated by the EDGAR enrichment pipeline.
Do not modify any other routers.
```

**Delivers:**
- `GET /api/monitor/inactive` with reason/assetClass filters
- `GET /api/monitor/summary`
- `GET /api/monitor/etf/{ticker}/inactive`
- `/monitor` page with KPI cards, reason breakdown chart, inactive securities table, ETF risk view

**Verify before committing:**
```bash
curl localhost:8000/api/monitor/summary
curl "localhost:8000/api/monitor/inactive?reason=not_in_edgar_tickers&pageSize=5"
curl localhost:8000/api/monitor/etf/SPY/inactive
# Open localhost:3000/monitor — KPI cards and table should load
```

---

## After All Sessions — Final Integration Check

Run through these manually before declaring the project done:

- [ ] Navigate from `/etfs` → click a ticker → lands on `/etf/{ticker}`
- [ ] On ETF Profile, click a holding ticker → lands on `/security/{ticker}`
- [ ] On Security Profile, click an ETF → lands on `/etf/{ticker}`
- [ ] Navigate to `/compare`, compare SPY vs QQQ — overlap shows correctly
- [ ] Navigate to `/issuer/BlackRock` — ETFs list with AUM
- [ ] Navigate to `/research` — ask a question — AI responds with data table
- [ ] Navigate to `/monitor` — inactive securities load with correct reasons
- [ ] All pages work on a narrow window (375px width)
- [ ] `npm run build` in `web/` completes with zero errors
