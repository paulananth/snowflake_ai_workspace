# Single Agent Implementation Guide

This directory contains everything needed to implement the ETF Intelligence Portal
using a single AI coding agent across sequential sessions.

## Files

| File | Purpose |
|------|---------|
| `README.md` | This file — start here |
| `SESSION_GUIDE.md` | Full session-by-session build order with prompts and verify commands |
| `AGENT_PROMPT.md` | Reusable session start template — copy/paste at the start of every session |
| `COMMIT_CHECKLIST.md` | What to do after each session before starting the next |
| `TODOS.md` | Running log of deferred items and known gaps |

## How to Use

1. Read `SESSION_GUIDE.md` to understand the full sequence
2. Start each session using the template in `AGENT_PROMPT.md`
3. Point the agent to `SPECIFICATION.md` in the project root as the source of truth
4. After the session ends, follow `COMMIT_CHECKLIST.md` before starting the next session
5. Log any deferred items in `TODOS.md`

## Session Order at a Glance

```
0 Foundation  →  1 Screener  →  4 Overlap Tool
                 2 ETF Profile →  5 Issuer + History
                 3 Security
                 (any time after 0) 6 AI Assistant
                 (any time after 0) 7 Monitor
```

## Reference

Full implementation contracts, database schema, API signatures, and UI specs
are in `SPECIFICATION.md` at the project root.
