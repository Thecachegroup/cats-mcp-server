"""
CATS ATS — MCP server for Claude connectors, deployed as a Vercel Python
serverless function using Streamable HTTP transport.

Endpoint: POST /api/mcp/<CONNECTOR_SHARED_KEY>
CATS:     Authorization: Token <CATS_API_KEY>  (set as env var, never sent by client)

Tools exposed:
  list_jobs                 - list jobs, with readable status name resolved
  get_job                   - get a single job, with readable status + company
  get_company                - get a company (client) by id
  list_job_statuses          - list all possible job statuses (id -> name)
  list_pipeline_candidates   - candidates in a job's pipeline, with readable
                                status name, rating, and date_modified
  get_workflow_statuses      - list statuses for a pipeline workflow (id -> name)
  get_candidate               - get a candidate profile by id
  get_candidate_resume        - list a candidate's resume/attachment history
  read_resume                - download + extract text from a specific attachment
  list_recent_candidates      - candidates created/updated since a given time
"""

import os
import io
import json
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

CATS_API_BASE = "https://api.catsone.com/v3"
CATS_API_KEY = os.environ.get("CATS_API_KEY", "")
CONNECTOR_SHARED_KEY = os.environ.get("CONNECTOR_SHARED_KEY", "")

app = FastAPI()


def cats_headers():
    if not CATS_API_KEY:
        raise HTTPException(status_code=500, detail="CATS_API_KEY not configured on server")
    return {
        "Authorization": f"Token {CATS_API_KEY}",
        "Content-Type": "application/json",
    }


async def cats_get(path: str, params: dict | None = None):
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(f"{CATS_API_BASE}{path}", headers=cats_headers(), params=params or {})
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


async def cats_get_binary(path: str):
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(f"{CATS_API_BASE}{path}", headers={"Authorization": f"Token {CATS_API_KEY}"})
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text[:500])
        return resp.content, resp.headers.get("content-type", "")


def _since_filter(items: list, since: str):
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    filtered = []
    for item in items:
        updated = item.get("date_modified") or item.get("date_created")
        if not updated:
            continue
        try:
            updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        except ValueError:
            continue
        if updated_dt >= since_dt:
            filtered.append(item)
    return filtered


def extract_text_from_bytes(content: bytes, filename: str) -> str:
    """Best-effort text extraction for common resume formats."""
    lower = (filename or "").lower()
    try:
        if lower.endswith(".pdf"):
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(content))
            return "\n".join((page.extract_text() or "") for page in reader.pages)
        if lower.endswith(".docx"):
            import docx
            doc = docx.Document(io.BytesIO(content))
            return "\n".join(p.text for p in doc.paragraphs)
        if lower.endswith(".txt") or lower.endswith(".rtf"):
            return content.decode("utf-8", errors="ignore")
    except Exception as e:
        return f"[Could not extract text from {filename}: {e}]"
    return f"[Unsupported format for text extraction: {filename}. Use the CATS UI to view this file directly.]"


# ---- Tool implementations ------------------------------------------------

async def tool_list_jobs(args: dict):
    per_page = args.get("per_page", 25)
    page = args.get("page", 1)
    data = await cats_get("/jobs", {"per_page": per_page, "page": page})

    statuses = await cats_get("/jobs/statuses", {"per_page": 100})
    status_map = {s["id"]: s.get("title") or s.get("name") for s in statuses.get("_embedded", {}).get("statuses", [])}
    for job in data.get("_embedded", {}).get("jobs", []):
        job["status_name"] = status_map.get(job.get("status_id"), f"Unknown ({job.get('status_id')})")

    return data


async def tool_get_job(args: dict):
    job_id = args["job_id"]
    data = await cats_get(f"/jobs/{job_id}")

    statuses = await cats_get("/jobs/statuses", {"per_page": 100})
    status_map = {s["id"]: s.get("title") or s.get("name") for s in statuses.get("_embedded", {}).get("statuses", [])}
    data["status_name"] = status_map.get(data.get("status_id"), f"Unknown ({data.get('status_id')})")

    if data.get("company_id"):
        try:
            company = await cats_get(f"/companies/{data['company_id']}")
            data["company_name"] = company.get("name")
        except HTTPException:
            pass

    return data


async def tool_get_company(args: dict):
    company_id = args["company_id"]
    return await cats_get(f"/companies/{company_id}")


async def tool_list_job_statuses(args: dict):
    return await cats_get("/jobs/statuses", {"per_page": 100})


async def tool_list_pipeline_candidates(args: dict):
    job_id = args["job_id"]
    per_page = args.get("per_page", 100)
    page = args.get("page", 1)
    data = await cats_get(f"/jobs/{job_id}/pipelines", {"per_page": per_page, "page": page})

    items = data.get("_embedded", {}).get("pipelines", [])

    workflow_ids = {item.get("workflow_id") for item in items if item.get("workflow_id")}
    status_map = {}
    for wf_id in workflow_ids:
        try:
            statuses = await cats_get(f"/pipelines/workflows/{wf_id}/statuses", {"per_page": 100})
            for s in statuses.get("_embedded", {}).get("statuses", []):
                status_map[s["id"]] = s.get("title") or s.get("name")
        except HTTPException:
            continue

    for item in items:
        item["status_name"] = status_map.get(item.get("status_id"), f"Unknown ({item.get('status_id')})")
        # rating and date_modified already come through as-is from CATS

    since = args.get("since")
    if since:
        items = _since_filter(items, since)
        data["filtered_since"] = since

    data["_embedded"]["pipelines"] = items
    return data


async def tool_get_workflow_statuses(args: dict):
    workflow_id = args["workflow_id"]
    return await cats_get(f"/pipelines/workflows/{workflow_id}/statuses", {"per_page": 100})


async def tool_get_candidate(args: dict):
    candidate_id = args["candidate_id"]
    return await cats_get(f"/candidates/{candidate_id}")


async def tool_get_candidate_resume(args: dict):
    candidate_id = args["candidate_id"]
    # Full attachment history — includes every resume version uploaded for
    # this candidate, each with an attachment id, filename, and is_resume flag.
    return await cats_get(f"/candidates/{candidate_id}/attachments", {"per_page": 100})


async def tool_read_resume(args: dict):
    attachment_id = args["attachment_id"]
    meta = await cats_get(f"/attachments/{attachment_id}")
    content, content_type = await cats_get_binary(f"/attachments/{attachment_id}/download")
    text = extract_text_from_bytes(content, meta.get("filename", ""))
    return {
        "attachment_id": attachment_id,
        "filename": meta.get("filename"),
        "is_resume": meta.get("is_resume"),
        "content_type": content_type,
        "text": text,
    }


async def tool_list_recent_candidates(args: dict):
    per_page = args.get("per_page", 50)
    page = args.get("page", 1)
    data = await cats_get("/candidates", {"per_page": per_page, "page": page, "sort": "-date_modified"})

    since = args.get("since")
    if since:
        items = _since_filter(data.get("_embedded", {}).get("candidates", []), since)
        data["_embedded"]["candidates"] = items
        data["filtered_since"] = since

    return data


TOOLS = {
    "list_jobs": {
        "description": "List jobs in CATS, including a readable status_name for each (e.g. 'Open', 'Closed', 'On Hold').",
        "inputSchema": {
            "type": "object",
            "properties": {
                "per_page": {"type": "integer", "default": 25},
                "page": {"type": "integer", "default": 1},
            },
        },
        "handler": tool_list_jobs,
    },
    "get_job": {
        "description": "Get a single job by its CATS job id, including readable status_name, full description, and the client company_name.",
        "inputSchema": {
            "type": "object",
            "properties": {"job_id": {"type": "integer"}},
            "required": ["job_id"],
        },
        "handler": tool_get_job,
    },
    "get_company": {
        "description": "Get a client company's details by CATS company id.",
        "inputSchema": {
            "type": "object",
            "properties": {"company_id": {"type": "integer"}},
            "required": ["company_id"],
        },
        "handler": tool_get_company,
    },
    "list_job_statuses": {
        "description": "List all possible job statuses in this CATS account (id to name mapping, e.g. Open/Closed/On Hold).",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": tool_list_job_statuses,
    },
    "list_pipeline_candidates": {
        "description": "List candidates in a job's pipeline, including readable status_name (e.g. 'Submitted', 'Interviewing', 'Rejected'), rating, and date_modified. Optionally filter to candidates updated since a given ISO 8601 timestamp.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "integer"},
                "since": {"type": "string", "description": "ISO 8601 timestamp, e.g. 2026-07-01T00:00:00Z"},
                "per_page": {"type": "integer", "default": 100},
                "page": {"type": "integer", "default": 1},
            },
            "required": ["job_id"],
        },
        "handler": tool_list_pipeline_candidates,
    },
    "get_workflow_statuses": {
        "description": "List all statuses for a given pipeline workflow id (id to name mapping).",
        "inputSchema": {
            "type": "object",
            "properties": {"workflow_id": {"type": "integer"}},
            "required": ["workflow_id"],
        },
        "handler": tool_get_workflow_statuses,
    },
    "get_candidate": {
        "description": "Get a candidate's full profile by CATS candidate id, including date_modified.",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}},
            "required": ["candidate_id"],
        },
        "handler": tool_get_candidate,
    },
    "get_candidate_resume": {
        "description": "List a candidate's full attachment/resume history by CATS candidate id — every version uploaded, each with an attachment_id. Use read_resume with an attachment_id from this list to get the actual CV text.",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}},
            "required": ["candidate_id"],
        },
        "handler": tool_get_candidate_resume,
    },
    "read_resume": {
        "description": "Download a specific attachment and extract its text content, so the actual CV can be read (not just the filename). Get the attachment_id from get_candidate_resume first. Supports PDF, DOCX, and TXT.",
        "inputSchema": {
            "type": "object",
            "properties": {"attachment_id": {"type": "integer"}},
            "required": ["attachment_id"],
        },
        "handler": tool_read_resume,
    },
    "list_recent_candidates": {
        "description": "List candidates created or updated since a given ISO 8601 timestamp. Use this for hourly pipeline checks.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "ISO 8601 timestamp, e.g. 2026-07-01T00:00:00Z"},
                "per_page": {"type": "integer", "default": 50},
                "page": {"type": "integer", "default": 1},
            },
        },
        "handler": tool_list_recent_candidates,
    },
}


# ---- MCP JSON-RPC over Streamable HTTP ------------------------------------

def rpc_result(id_, result):
    return {"jsonrpc": "2.0", "id": id_, "result": result}


def rpc_error(id_, code, message):
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}


@app.post("/api/mcp/{key}")
async def mcp_endpoint(key: str, request: Request):
    if CONNECTOR_SHARED_KEY and key != CONNECTOR_SHARED_KEY:
        raise HTTPException(status_code=401, detail="Invalid connector key")

    body = await request.json()
    method = body.get("method")
    id_ = body.get("id")
    params = body.get("params", {}) or {}

    if method == "initialize":
        return JSONResponse(rpc_result(id_, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "cats-connector", "version": "1.1.0"},
        }))

    if method == "tools/list":
        tools_out = [
            {"name": name, "description": t["description"], "inputSchema": t["inputSchema"]}
            for name, t in TOOLS.items()
        ]
        return JSONResponse(rpc_result(id_, {"tools": tools_out}))

    if method == "tools/call":
        tool_name = params.get("name")
        args = params.get("arguments", {}) or {}
        tool = TOOLS.get(tool_name)
        if not tool:
            return JSONResponse(rpc_error(id_, -32601, f"Unknown tool: {tool_name}"))
        try:
            result = await tool["handler"](args)
        except HTTPException as e:
            return JSONResponse(rpc_error(id_, -32000, f"CATS API error: {e.detail}"))
        except KeyError as e:
            return JSONResponse(rpc_error(id_, -32602, f"Missing required argument: {e}"))
        return JSONResponse(rpc_result(id_, {
            "content": [{"type": "text", "text": json.dumps(result)}]
        }))

    return JSONResponse(rpc_error(id_, -32601, f"Unknown method: {method}"))


@app.get("/api/mcp/{key}")
async def health(key: str):
    return {"status": "ok", "server": "cats-connector", "time": datetime.now(timezone.utc).isoformat()}
