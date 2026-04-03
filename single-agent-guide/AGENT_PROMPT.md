# Single Agent Session Template

Use this prompt at the start of every new coding session.
Fill in the bracketed fields before sending.

---

## Session Start Prompt (copy & paste)

```
Read SPECIFICATION.md fully before writing any code.
Read all existing files in web/ and api/ to understand what is already built.

You are implementing: [AGENT LETTER] — [FEATURE NAME]

Your owned files are:
  [paste the "Owns:" list from SPECIFICATION.md for your agent]

Do NOT modify any files outside your owned scope.
If you notice a bug or improvement in another agent's files, log it as a TODO comment — do not fix it now.

When done:
  1. Run `npm run build` in web/ — fix any TypeScript errors before stopping
  2. Confirm the API endpoint returns data (test with curl or the browser)
  3. Confirm the UI page loads at localhost:3000/[route] with no console errors
  4. Stop and tell me what you built and what (if anything) is incomplete
  Do NOT start the next feature
```

---

## Current Session (fill in each time)

| Field | Value |
|-------|-------|
| Agent | (e.g. Foundation, Agent A, Agent B ...) |
| Feature | (e.g. ETF Screener, Security Profile ...) |
| Files I own | (from SPECIFICATION.md Owns section) |
| Files I must NOT touch | Everything not in my Owns list |
| Route to verify | (e.g. localhost:3000/etfs) |

---

## Done Checklist (agent confirms before stopping)

- [ ] API endpoints return correct data (test with curl or browser /docs)
- [ ] UI page loads without errors at the assigned route
- [ ] Loading state works (show spinner/skeleton while fetching)
- [ ] Error state works (show message if API fails)
- [ ] Empty data state works (show "No data" if result is empty)
- [ ] No TypeScript errors (`npm run build` passes)
- [ ] No types defined inline — all imported from `web/lib/types.ts`
- [ ] All API calls go through `web/lib/api-client.ts`
- [ ] All internal links use Next.js `<Link>` component
- [ ] No hardcoded dates, tickers, or environment values
- [ ] Shared files (`types.ts`, `db.py`, `models.py`) were NOT modified
