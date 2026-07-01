"""
CATS ATS — MCP server for Claude connectors, deployed as a Vercel Python
serverless function using Streamable HTTP transport.

Endpoint: POST /api/mcp
Auth:     Shared secret in header  X-Connector-Key: <CONNECTOR_SHARED_KEY>
CATS:     Authorization: Token <CATS_API_KEY>  (set as env var, never sent by client)

Tools exposed:
  list_jobs              - list open jobs
  get_job                - get a single job by id
  list_pipeline_candidates - candidates in a job's pipeline, optional `since` filter
  get_candidate           - get a candidate profile by id
  get_candidate_resume    - get a candidate's resume text
  list_recent_candidates  - candidates created/updated since a given time
"""

import os
import json
import time
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


# ---- Tool implementations ------------------------------------------------

async def tool_list_jobs(args: dict):
    per_page = args.get("per_page", 25)
    page = args.get("page", 1)
    data = await cats_get("/jobs", {"per_page": per_page, "page": page})
    return data


async def tool_get_job(args: dict):
    job_id = args["job_id"]
    data = await cats_get(f"/jobs/{job_id}")
    return data


async def tool_list_pipeline_candidates(args: dict):
    job_id = args["job_id"]
    per_page = args.get("per_page", 100)
    page = args.get("page", 1)
    data = await cats_get(f"/jobs/{job_id}/pipelines", {"per_page": per_page, "page": page})

    since = args.get("since")
    if since:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        items = data.get("_embedded", {}).get("pipelines", [])
        filtered = []
        for item in items:
            updated = item.get("date_modified") or item.get("date_created")
            if not updated:
                filtered.append(item)
                continue
            try:
                updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            except ValueError:
                filtered.append(item)
                continue
            if updated_dt >= since_dt:
                filtered.append(item)
        data["_embedded"]["pipelines"] = filtered
        data["filtered_since"] = since

    return data


async def tool_get_candidate(args: dict):
    candidate_id = args["candidate_id"]
    data = await cats_get(f"/candidates/{candidate_id}")
    return data


async def tool_get_candidate_resume(args: dict):
    candidate_id = args["candidate_id"]
    # CATS exposes attachments per candidate; resume text extraction depends
    # on the attachment endpoint returning parsed text where available.
    data = await cats_get(f"/candidates/{candidate_id}/attachments")
    return data


async def tool_list_recent_candidates(args: dict):
    per_page = args.get("per_page", 50)
    page = args.get("page", 1)
    data = await cats_get("/candidates", {"per_page": per_page, "page": page, "sort": "-date_modified"})

    since = args.get("since")
    if since:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        items = data.get("_embedded", {}).get("candidates", [])
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
        data["_embedded"]["candidates"] = filtered
        data["filtered_since"] = since

    return data


TOOLS = {
    "list_jobs": {
        "description": "List open jobs in CATS.",
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
        "description": "Get a single job by its CATS job id.",
        "inputSchema": {
            "type": "object",
            "properties": {"job_id": {"type": "integer"}},
            "required": ["job_id"],
        },
        "handler": tool_get_job,
    },
    "list_pipeline_candidates": {
        "description": "List candidates in a job's pipeline. Optionally filter to candidates updated since a given ISO 8601 timestamp.",
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
    "get_candidate": {
        "description": "Get a candidate's full profile by CATS candidate id.",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}},
            "required": ["candidate_id"],
        },
        "handler": tool_get_candidate,
    },
    "get_candidate_resume": {
        "description": "Get a candidate's resume/attachment listing by CATS candidate id.",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}},
            "required": ["candidate_id"],
        },
        "handler": tool_get_candidate_resume,
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


@app.post("/api/mcp")
async def mcp_endpoint(request: Request):
    if CONNECTOR_SHARED_KEY:
        provided = request.headers.get("x-connector-key", "")
        if provided != CONNECTOR_SHARED_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing connector key")

    body = await request.json()
    method = body.get("method")
    id_ = body.get("id")
    params = body.get("params", {}) or {}

    if method == "initialize":
        return JSONResponse(rpc_result(id_, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "cats-connector", "version": "1.0.0"},
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


@app.get("/api/mcp")
async def health():
    return {"status": "ok", "server": "cats-connector", "time": datetime.now(timezone.utc).isoformat()}
