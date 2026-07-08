# CATS Connector — MCP server for Claude (Vercel)

Deploys the CATS ATS connector as a serverless HTTP endpoint so it can be
added to Claude as a custom connector — works in Cowork **and** claude.ai
Projects.

## What's in here

- `api/index.py` — the MCP server (46 tools; core reads: list_jobs, get_job,
  list_pipeline_candidates, get_candidate, get_candidate_resume,
  list_recent_candidates). Vercel auto-detects this as the FastAPI
  entrypoint — no `vercel.json` or `pyproject.toml` needed.
- `requirements.txt` — fastapi + httpx

## Deploy steps (you do these — ~10 minutes)

### 1. Push to GitHub
```
cd cats-mcp-server
git init
git add .
git commit -m "CATS MCP connector"
git remote add origin https://github.com/Thecachegroup/cats-mcp-server.git
git push -u origin main
```
(Create the empty repo `cats-mcp-server` under the Thecachegroup GitHub
account first, same as CV Suite.)

### 2. Import into Vercel
- vercel.com → Add New → Project → Import the `cats-mcp-server` repo
- Framework preset: **Other** (it'll detect the Python function automatically)
- Deploy

### 3. Set environment variables
Vercel Project → Settings → Environment Variables, add:

| Key | Value |
|---|---|
| `CATS_API_KEY` | your CATS API key (Administration → API Keys in CATS) |
| `CONNECTOR_SHARED_KEY` | any long random string you make up — this is the password your team's Claude connector will send |

Redeploy after adding these (Vercel → Deployments → ⋯ → Redeploy).

### 4. Get your URL
Your Vercel URL is `https://cats-mcp-server-gta1p7pad-tcg-s-projects.vercel.app`.

**Note:** claude.ai's "Add custom connector" dialog only supports OAuth — there's
no field for a custom header. So the shared key goes in the URL path instead:

```
https://cats-mcp-server-gta1p7pad-tcg-s-projects.vercel.app/api/mcp/<CONNECTOR_SHARED_KEY>
```

Test it's alive: open that full URL (with your real key on the end) in a
browser — you should see `{"status":"ok","server":"cats-connector",...}`.
A wrong or missing key returns a 401.

### 5. Add it in Claude
Settings → Connectors → Add custom connector:
- Name: `CATS Connector`
- URL: `https://cats-mcp-server-gta1p7pad-tcg-s-projects.vercel.app/api/mcp/<CONNECTOR_SHARED_KEY>`
- Leave Advanced settings (OAuth) blank

Give the same full URL (key included) to your team — each person adds it
individually under their own Settings → Connectors. Treat this URL like a
password since it contains your key.

## Notes

- The CATS API key never leaves Vercel — Claude only ever sends the shared
  connector key (embedded in the URL), not your CATS credentials.
- `list_recent_candidates` and `list_pipeline_candidates` both accept a
  `since` ISO 8601 timestamp — this is what you'll use for the hourly
  pipeline check.
- Write actions (`create_job`, `change_job_status`, `add_candidate_to_pipeline`,
  `change_pipeline_status`, `create_candidate_list`, `add_candidates_to_list`,
  `publish_job_to_portal`, `update_pipeline_rating_status`,
  `bulk_update_pipelines`, `update_job_notes`, `update_candidate_notes`,
  `add_candidate_tag`) are preview-by-default — calling them without
  `confirm: true` shows what would happen without touching CATS. They only
  execute when called again with `confirm: true`.
- `add_candidate_tag` is additive — it adds a tag without removing existing
  ones. This matters because CATS also has a destructive "replace tags"
  mechanism; this connector deliberately avoids it so tagging a candidate
  can never silently wipe an existing flag like "Never Employ".
- `search_candidates_deep` is a slow tool (it downloads and parses CVs) —
  always bound it with `since` or rely on the `max_results` cap.
- Endpoints inferred from CATS's conventions rather than confirmed live:
  `change_pipeline_status`, `publish_job_to_portal`, `search_contacts`,
  `add_candidate_tag`. Each is flagged in its own tool description — report
  back if any errors on first real use.
- Free Vercel tier is plenty for this traffic level.


## v2 changes (July 2026)

- **Shaped responses everywhere** — `_embedded` flattened to a top-level array, `_links` stripped, and `total` / `per_page` / `pages` / `has_more` surfaced at top level. No more digging job arrays out of nested HAL structures.
- **`filter_jobs` / `filter_candidates`** — one-call filtered pulls. The connector pages through CATS internally (Vercel side) and returns only matches, so "jobs advertised since 2025-07-07" is a single tool call instead of a six-page pull. Job filters: date_created/date_modified (`>=`, `<=`), status_id, company_id, title (contains). Candidate scan is capped at the 1,000 most recently modified.
- **Activities entity** — `list_candidate_activities`, `create_candidate_activity`, plus contact equivalents. Interaction history (calls, emails, meetings) is now readable, and calls are logged as timestamped activities instead of overwriting the notes field. Endpoints follow the confirmed sub-resource pattern but are inferred — report back on first live use.
- **Lists read side** — `list_candidate_lists`, `get_list_items`, `remove_list_item`. Lists were previously write-only.
- **Candidate writes** — `create_candidate`, `update_candidate`, `upload_candidate_attachment` (base64; `is_resume` flag), `remove_candidate_tag`. All preview-by-default like existing writes. Tag removal is deliberately single-tag; there is still no bulk tag replace, so protective flags can never be wiped.
- **Small read gaps** — `get_contact`, `get_job_applications`.
- Version string bumped to 2.0.0.

### Redeploy
Push to the existing repo; Vercel redeploys automatically. No new env vars, no requirements changes. Same connector URL — nothing to change in Claude.

### First-live-use checklist (inferred endpoints)
Run each once and report errors: `list_candidate_activities`, `create_candidate_activity` (preview then confirm on a test candidate), `remove_list_item`, `remove_candidate_tag`, `get_job_applications`.

## v2.1.0 (July 2026) — candidate date-handling fixes

Fixes a cluster of bugs found in live testing, all rooted in two causes: CATS returns the `/candidates` list **oldest-first** and ignores a sort parameter, and the internal date filter crashed on date-only `since` values.

- **Newest-first candidate paging** — new internal helper reads candidates from the newest end of the list instead of trusting a (silently ignored) sort param. Fixes:
  - `filter_candidates` — was returning empty for any recent-date filter (it was scanning 2013 records).
  - `list_recent_candidates` — same bug; this powers the **hourly pipeline check**.
  - `search_candidates_deep` — was only ever scanning the oldest ~200 candidates, so recent CVs were unreachable.
- **Date parsing no longer crashes** — `since` now accepts date-only (`2026-07-08`), `Z`-suffixed, or full-offset timestamps, and naive values are treated as UTC so comparisons against CATS's tz-aware dates can't raise. Fixes the hard error on `search_candidates_deep`/`search_pipelines_by_status` when a `since` was supplied.
- **Real error messages** — the MCP dispatch now catches all exceptions and returns the actual error type, message, and a short traceback instead of an opaque "Error occurred during tool execution". This is how to diagnose the remaining inferred endpoints.

### Still to verify live after this deploy
- `search_pipelines_by_status` — errored on both paths before; with real error reporting now on, re-run it and the response will say what CATS actually rejects (likely the `/pipelines/filter` body shape).
- **Contacts** — `search_contacts` returns zero even for broad terms. Grab one real contact ID from the CATS UI (open a client contact; the ID is in the URL) and run `get_contact` with it — that single call will confirm whether contacts are barely populated or the endpoint path is wrong.
