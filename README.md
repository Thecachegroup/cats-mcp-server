# CATS Connector — MCP server for Claude (Vercel)

Deploys the CATS ATS connector as a serverless HTTP endpoint so it can be
added to Claude as a custom connector — works in Cowork **and** claude.ai
Projects.

## What's in here

- `api/index.py` — the MCP server (6 tools: list_jobs, get_job,
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
