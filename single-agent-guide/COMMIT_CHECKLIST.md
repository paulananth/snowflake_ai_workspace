# After-Session Commit Checklist

Run through this after every session, before starting the next one.
A clean commit after each session gives you a safe rollback point.

---

## Step 1 — Verify the Build

```bash
# Frontend — must pass with zero errors
cd web && npm run build

# Backend — must start without import errors
cd api && uvicorn main:app --reload
```

## Step 2 — Smoke Test the New Endpoints

Use the curl commands from SESSION_GUIDE.md for the session you just completed.
At minimum: confirm each new API endpoint returns a non-error response.

## Step 3 — Check for Shared File Modifications

```bash
git diff web/lib/types.ts
git diff web/lib/api-client.ts
git diff web/lib/utils.ts
git diff api/db.py
git diff api/models.py
```

If any of these files were modified, review the changes carefully:
- Additions only? → OK to commit
- Renames or removals? → Check all other sessions' files for breakage before committing

## Step 4 — Stage and Commit

```bash
# Stage only the files for this session (never use git add -A blindly)
git add web/app/[route]/
git add web/components/[agent-folder]/
git add api/routers/[router-file].py

# Commit with a clear message
git commit -m "feat([agent]): [feature name]

- [what API endpoints were added]
- [what UI page was built]
- [any notable decisions or known gaps]"
```

## Step 5 — Log Any TODOs

If the agent flagged anything incomplete or noticed issues in other files,
add them to `single-agent-guide/TODOS.md` before starting the next session.

---

## Commit Message Convention

```
feat(foundation): scaffold, shared types, DB client, home page
feat(screener): ETF screener with filters, pagination, CSV export
feat(etf-profile): ETF profile page with holdings, exposure, history, fund info tabs
feat(security): security profile page with EDGAR metadata and ETF memberships
feat(overlap): holdings overlap tool for up to 4 ETFs
feat(issuer): issuer page and historical holdings trends endpoint
feat(ai-chat): AI research assistant with Cortex Analyst integration
feat(monitor): inactive securities monitor and ETF-level risk view
fix([agent]): [description of fix]
chore: [cleanup, dependency update, etc.]
```

---

## Rollback if a Session Breaks Something

```bash
# See commit history
git log --oneline

# Undo last commit but keep the files (so you can review what went wrong)
git reset --soft HEAD~1

# Or discard everything from the last commit entirely
git reset --hard HEAD~1
```
