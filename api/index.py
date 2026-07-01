"""
CATS ATS — MCP server for Claude connectors, deployed as a Vercel Python
serverless function using Streamable HTTP transport.

Endpoint: POST /api/mcp/<CONNECTOR_SHARED_KEY>
CATS:     Authorization: Token <CATS_API_KEY>  (set as env var, never sent by client)

Read tools:
  list_jobs, get_job, get_company, list_job_statuses,
  list_pipeline_candidates, get_workflow_statuses,
  get_candidate, get_candidate_resume, read_resume, list_recent_candidates,
  get_candidate_tags, get_candidate_custom_fields, get_candidate_pipeline_history,
  list_portals

Write tools (preview-by-default, require confirm: true to execute):
  create_job, change_job_status, add_candidate_to_pipeline,
  change_pipeline_status, create_candidate_list, add_candidates_to_list,
  publish_job_to_portal

Two write endpoints (change_pipeline_status, publish_job_to_portal) are
inferred from CATS's consistent API patterns rather than fully confirmed
against live docs — flagged in their own descriptions/responses.
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


async def cats_post(path: str, body: dict | None = None):
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(f"{CATS_API_BASE}{path}", headers=cats_headers(), json=body or {})
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        location = resp.headers.get("location")
        try:
            data = resp.json()
        except Exception:
            data = {}
        if location:
            data["_created_location"] = location
        return data


async def cats_put(path: str, body: dict | None = None):
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.put(f"{CATS_API_BASE}{path}", headers=cats_headers(), json=body or {})
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        try:
            return resp.json()
        except Exception:
            return {}


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


# ---- New: candidate flags (tags, custom fields, cross-job history) -------

async def tool_get_candidate_tags(args: dict):
    candidate_id = args["candidate_id"]
    return await cats_get(f"/candidates/{candidate_id}/tags", {"per_page": 100})


async def tool_get_candidate_custom_fields(args: dict):
    candidate_id = args["candidate_id"]
    return await cats_get(f"/candidates/{candidate_id}/custom_fields", {"per_page": 100})


async def tool_get_candidate_pipeline_history(args: dict):
    candidate_id = args["candidate_id"]
    per_page = args.get("per_page", 100)
    page = args.get("page", 1)
    data = await cats_get(f"/candidates/{candidate_id}/pipelines", {"per_page": per_page, "page": page})

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

    job_ids = {item.get("job_id") for item in items if item.get("job_id")}
    job_titles = {}
    for jid in job_ids:
        try:
            job = await cats_get(f"/jobs/{jid}")
            job_titles[jid] = job.get("title")
        except HTTPException:
            continue

    for item in items:
        item["status_name"] = status_map.get(item.get("status_id"), f"Unknown ({item.get('status_id')})")
        item["job_title"] = job_titles.get(item.get("job_id"), f"Unknown job ({item.get('job_id')})")

    data["_embedded"]["pipelines"] = items
    return data


async def tool_update_pipeline_rating_status(args: dict):
    """Set rating and/or status on a single pipeline entry."""
    pipeline_id = args["pipeline_id"]
    rating = args.get("rating")
    status_id = args.get("status_id")

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "update_pipeline_rating_status",
            "pipeline_id": pipeline_id,
            "would_set_rating_to": rating,
            "would_set_status_id_to": status_id,
            "note": "Nothing has changed yet. Call again with confirm: true to actually apply this in CATS.",
        }

    results = {}
    if rating is not None:
        results["rating_update"] = await cats_put(f"/pipelines/{pipeline_id}", {"rating": rating})
    if status_id is not None:
        results["status_update"] = await cats_post(f"/pipelines/{pipeline_id}/status", {"status_id": status_id})
    return {"changed": True, "action": "update_pipeline_rating_status", "pipeline_id": pipeline_id, "results": results}


async def tool_bulk_update_pipelines(args: dict):
    """Set rating and/or status across multiple pipeline entries at once —
    e.g. 'give all those good candidates Qualifying status and 3 stars'."""
    pipeline_ids = args["pipeline_ids"]
    rating = args.get("rating")
    status_id = args.get("status_id")

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "bulk_update_pipelines",
            "pipeline_ids": pipeline_ids,
            "count": len(pipeline_ids),
            "would_set_rating_to": rating,
            "would_set_status_id_to": status_id,
            "note": f"Nothing has changed yet. This would update {len(pipeline_ids)} pipeline entries. "
                    "Call again with confirm: true to actually apply this in CATS.",
        }

    results = []
    for pid in pipeline_ids:
        entry = {"pipeline_id": pid}
        try:
            if rating is not None:
                await cats_put(f"/pipelines/{pid}", {"rating": rating})
            if status_id is not None:
                await cats_post(f"/pipelines/{pid}/status", {"status_id": status_id})
            entry["success"] = True
        except HTTPException as e:
            entry["success"] = False
            entry["error"] = str(e.detail)
        results.append(entry)

    return {"changed": True, "action": "bulk_update_pipelines", "results": results}


async def tool_update_job_notes(args: dict):
    job_id = args["job_id"]
    new_notes = args["notes"]

    current = await cats_get(f"/jobs/{job_id}")
    body = {
        "title": current["title"],
        "location": current.get("location", {}),
        "company_id": current["company_id"],
        "country_code": current.get("country_code"),
        "department_id": current.get("department_id"),
        "recruiter_id": current.get("recruiter_id"),
        "owner_id": current.get("owner_id"),
        "category_name": current.get("category_name"),
        "is_hot": current.get("is_hot"),
        "start_date": current.get("start_date"),
        "salary": current.get("salary"),
        "max_rate": current.get("max_rate"),
        "duration": current.get("duration"),
        "type": current.get("type"),
        "openings": current.get("openings"),
        "external_id": current.get("external_id"),
        "description": current.get("description"),
        "contact_id": current.get("contact_id"),
        "notes": new_notes,
    }
    body = {k: v for k, v in body.items() if v is not None}

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "update_job_notes",
            "job_id": job_id,
            "current_notes": current.get("notes"),
            "would_set_notes_to": new_notes,
            "note": "Only the notes field will change — everything else on the job (title, description, status, etc.) is preserved as-is. Nothing has changed yet. Call again with confirm: true to actually apply this in CATS.",
        }

    await cats_put(f"/jobs/{job_id}", body)
    return {"changed": True, "action": "update_job_notes", "job_id": job_id}


async def tool_update_candidate_notes(args: dict):
    candidate_id = args["candidate_id"]
    new_notes = args["notes"]

    current = await cats_get(f"/candidates/{candidate_id}")
    body = {
        "first_name": current["first_name"],
        "middle_name": current.get("middle_name"),
        "last_name": current["last_name"],
        "title": current.get("title"),
        "address": current.get("address", {}),
        "country_code": current.get("country_code"),
        "social_media_urls": current.get("social_media_urls", []),
        "website": current.get("website"),
        "best_time_to_call": current.get("best_time_to_call"),
        "current_employer": current.get("current_employer"),
        "date_available": current.get("date_available"),
        "current_pay": current.get("current_pay"),
        "desired_pay": current.get("desired_pay"),
        "is_willing_to_relocate": current.get("is_willing_to_relocate"),
        "key_skills": current.get("key_skills"),
        "source": current.get("source"),
        "owner_id": current.get("owner_id"),
        "is_active": current.get("is_active"),
        "is_hot": current.get("is_hot"),
        "notes": new_notes,
    }
    body = {k: v for k, v in body.items() if v is not None}

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "update_candidate_notes",
            "candidate_id": candidate_id,
            "current_notes": current.get("notes"),
            "would_set_notes_to": new_notes,
            "note": "Only the notes field will change — everything else on the candidate record is preserved as-is. Nothing has changed yet. Call again with confirm: true to actually apply this in CATS.",
        }

    await cats_put(f"/candidates/{candidate_id}", body)
    return {"changed": True, "action": "update_candidate_notes", "candidate_id": candidate_id}


# ---- New: write actions — preview unless confirm=true --------------------

async def tool_create_job(args: dict):
    body = {
        "title": args["title"],
        "location": args.get("location", {}),
        "company_id": args["company_id"],
        "description": args.get("description", ""),
        "notes": args.get("notes", ""),
        "country_code": args.get("country_code", "AU"),
        "salary": args.get("salary", ""),
        "max_rate": args.get("max_rate", ""),
        "duration": args.get("duration", ""),
        "openings": args.get("openings", 1),
        "recruiter_id": args.get("recruiter_id"),
        "owner_id": args.get("owner_id"),
        "contact_id": args.get("contact_id"),
    }
    body = {k: v for k, v in body.items() if v is not None}

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "create_job",
            "would_create": body,
            "note": "Nothing has been created yet. Call this again with confirm: true to actually create this job in CATS.",
        }

    result = await cats_post("/jobs", body)
    return {"created": True, "action": "create_job", "result": result}


async def tool_change_job_status(args: dict):
    job_id = args["job_id"]
    status_id = args["status_id"]

    if not args.get("confirm"):
        statuses = await cats_get("/jobs/statuses", {"per_page": 100})
        status_map = {s["id"]: s.get("title") or s.get("name") for s in statuses.get("_embedded", {}).get("statuses", [])}
        return {
            "preview": True,
            "action": "change_job_status",
            "job_id": job_id,
            "would_set_status_to": status_map.get(status_id, f"Unknown ({status_id})"),
            "note": "Nothing has changed yet. Call this again with confirm: true to actually change the job status in CATS.",
        }

    result = await cats_post(f"/jobs/{job_id}/status", {"status_id": status_id})
    return {"changed": True, "action": "change_job_status", "job_id": job_id, "result": result}


async def tool_add_candidate_to_pipeline(args: dict):
    candidate_id = args["candidate_id"]
    job_id = args["job_id"]

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "add_candidate_to_pipeline",
            "candidate_id": candidate_id,
            "job_id": job_id,
            "note": "Nothing has changed yet. Call this again with confirm: true to actually add this candidate to the job's pipeline in CATS.",
        }

    result = await cats_post("/pipelines", {"candidate_id": candidate_id, "job_id": job_id})
    return {"created": True, "action": "add_candidate_to_pipeline", "result": result}


async def tool_change_pipeline_status(args: dict):
    pipeline_id = args["pipeline_id"]
    status_id = args["status_id"]

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "change_pipeline_status",
            "pipeline_id": pipeline_id,
            "would_set_status_id_to": status_id,
            "note": "This endpoint is inferred from CATS's consistent status-change pattern and hasn't been "
                    "live-tested yet — if it fails, the exact endpoint may need a small fix. Nothing has changed "
                    "yet either way. Call this again with confirm: true to actually attempt the change in CATS.",
        }

    result = await cats_post(f"/pipelines/{pipeline_id}/status", {"status_id": status_id})
    return {"changed": True, "action": "change_pipeline_status", "pipeline_id": pipeline_id, "result": result}


async def tool_create_candidate_list(args: dict):
    name = args["name"]
    notes = args.get("notes", "")

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "create_candidate_list",
            "would_create": {"name": name, "notes": notes},
            "note": "Nothing has been created yet. Call this again with confirm: true to actually create this list in CATS.",
        }

    result = await cats_post("/candidates/lists", {"name": name, "notes": notes})
    return {"created": True, "action": "create_candidate_list", "result": result}


async def tool_add_candidates_to_list(args: dict):
    list_id = args["list_id"]
    candidate_ids = args["candidate_ids"]

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "add_candidates_to_list",
            "list_id": list_id,
            "would_add_candidate_ids": candidate_ids,
            "note": "Nothing has changed yet. Call this again with confirm: true to actually add these candidates to the list in CATS.",
        }

    items = [{"candidate_id": cid} for cid in candidate_ids]
    result = await cats_post(f"/candidates/lists/{list_id}/items", {"items": items})
    return {"added": True, "action": "add_candidates_to_list", "result": result}


async def tool_list_portals(args: dict):
    return await cats_get("/portals", {"per_page": 100})


async def tool_publish_job_to_portal(args: dict):
    portal_id = args["portal_id"]
    job_id = args["job_id"]

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "publish_job_to_portal",
            "portal_id": portal_id,
            "job_id": job_id,
            "note": "This endpoint is inferred from the CATS docs table of contents (exact request shape wasn't "
                    "fully retrievable) and hasn't been live-tested — if it fails, check the response error and "
                    "we'll adjust the path. Nothing has changed yet either way. Call this again with confirm: true "
                    "to actually attempt publishing in CATS.",
        }

    result = await cats_post(f"/portals/{portal_id}/jobs/{job_id}/publish", {})
    return {"published": True, "action": "publish_job_to_portal", "result": result}


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
    "get_candidate_tags": {
        "description": "Get all tags attached to a candidate. Use this to check for flags like 'Never Employ' that are set up as tags in this CATS instance.",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}},
            "required": ["candidate_id"],
        },
        "handler": tool_get_candidate_tags,
    },
    "get_candidate_custom_fields": {
        "description": "Get all custom field values on a candidate (e.g. a 'Recruitment Status' or 'Never Employ' dropdown field). Use this to check for flags set up as custom fields rather than tags.",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}},
            "required": ["candidate_id"],
        },
        "handler": tool_get_candidate_custom_fields,
    },
    "get_candidate_pipeline_history": {
        "description": "Get a candidate's FULL pipeline history across every job they have ever been attached to (not just the current job), with readable status_name and job_title for each. Always check this before recommending a candidate — a 'Never Employ' or rejection status from a past role, even years ago, will show up here even if the candidate looks fresh on a new job's pipeline.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer"},
                "per_page": {"type": "integer", "default": 100},
                "page": {"type": "integer", "default": 1},
            },
            "required": ["candidate_id"],
        },
        "handler": tool_get_candidate_pipeline_history,
    },
    "create_job": {
        "description": "Create a new job order in CATS. PREVIEW BY DEFAULT: call without confirm to see what would be created without making any change. Call again with confirm: true to actually create it.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "company_id": {"type": "integer"},
                "location": {"type": "object", "description": "{city, state, postal_code}"},
                "description": {"type": "string"},
                "notes": {"type": "string"},
                "country_code": {"type": "string", "default": "AU"},
                "salary": {"type": "string"},
                "max_rate": {"type": "string"},
                "duration": {"type": "string"},
                "openings": {"type": "integer", "default": 1},
                "recruiter_id": {"type": "integer"},
                "owner_id": {"type": "integer"},
                "contact_id": {"type": "integer"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["title", "company_id"],
        },
        "handler": tool_create_job,
    },
    "change_job_status": {
        "description": "Change a job's status in CATS (e.g. to make it Active/published, or Closed). PREVIEW BY DEFAULT: call without confirm to see the readable status name before it's set. Call again with confirm: true to actually change it. Use list_job_statuses first to find the right status_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "integer"},
                "status_id": {"type": "integer"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["job_id", "status_id"],
        },
        "handler": tool_change_job_status,
    },
    "add_candidate_to_pipeline": {
        "description": "Add a candidate to a job's pipeline in CATS. PREVIEW BY DEFAULT: call without confirm first. Call again with confirm: true to actually add them.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer"},
                "job_id": {"type": "integer"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["candidate_id", "job_id"],
        },
        "handler": tool_add_candidate_to_pipeline,
    },
    "change_pipeline_status": {
        "description": "Move a candidate to a different pipeline stage/status in CATS (e.g. from 'Qualifying' to 'TCG Interview'). PREVIEW BY DEFAULT: call without confirm first. Call again with confirm: true to actually change it. Use get_workflow_statuses first to find the right status_id. Endpoint is inferred from CATS's pattern and not yet live-tested — report back if it errors.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_id": {"type": "integer"},
                "status_id": {"type": "integer"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["pipeline_id", "status_id"],
        },
        "handler": tool_change_pipeline_status,
    },
    "create_candidate_list": {
        "description": "Create a new candidate list in CATS (e.g. a shortlist). PREVIEW BY DEFAULT: call without confirm first. Call again with confirm: true to actually create it.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "notes": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["name"],
        },
        "handler": tool_create_candidate_list,
    },
    "add_candidates_to_list": {
        "description": "Add candidates to an existing CATS candidate list. PREVIEW BY DEFAULT: call without confirm first. Call again with confirm: true to actually add them.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "list_id": {"type": "integer"},
                "candidate_ids": {"type": "array", "items": {"type": "integer"}},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["list_id", "candidate_ids"],
        },
        "handler": tool_add_candidates_to_list,
    },
    "list_portals": {
        "description": "List job board portals connected to this CATS account, with their portal_id. Use this to find the right portal_id before publishing a job.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": tool_list_portals,
    },
    "publish_job_to_portal": {
        "description": "Publish a job to a specific job board portal so it goes live for external applicants. PREVIEW BY DEFAULT: call without confirm first. Call again with confirm: true to actually publish. Use list_portals first to find the right portal_id. Endpoint is inferred from CATS's docs table of contents and not yet live-tested — report back if it errors.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "portal_id": {"type": "integer"},
                "job_id": {"type": "integer"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["portal_id", "job_id"],
        },
        "handler": tool_publish_job_to_portal,
    },
    "update_pipeline_rating_status": {
        "description": "Set the star rating and/or pipeline status on a single candidate's pipeline entry (e.g. set rating to 3 and status to 'Qualifying'). PREVIEW BY DEFAULT: call without confirm first. Call again with confirm: true to actually apply. Use get_workflow_statuses first to find the right status_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_id": {"type": "integer"},
                "rating": {"type": "integer", "description": "Star rating, typically 0-5"},
                "status_id": {"type": "integer"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["pipeline_id"],
        },
        "handler": tool_update_pipeline_rating_status,
    },
    "bulk_update_pipelines": {
        "description": "Set the star rating and/or pipeline status across MULTIPLE candidates' pipeline entries at once — this is the tool for 'give all those good candidates Qualifying status and 3 stars'. PREVIEW BY DEFAULT: call without confirm first to see how many entries would change. Call again with confirm: true to actually apply. Use get_workflow_statuses first to find the right status_id for the target stage.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_ids": {"type": "array", "items": {"type": "integer"}},
                "rating": {"type": "integer", "description": "Star rating, typically 0-5"},
                "status_id": {"type": "integer"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["pipeline_ids"],
        },
        "handler": tool_bulk_update_pipelines,
    },
    "update_job_notes": {
        "description": "Update the internal notes field on a job in CATS, preserving everything else on the job unchanged (title, description, status, etc.). PREVIEW BY DEFAULT: call without confirm first to see current vs new notes. Call again with confirm: true to actually apply.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "integer"},
                "notes": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["job_id", "notes"],
        },
        "handler": tool_update_job_notes,
    },
    "update_candidate_notes": {
        "description": "Update the internal notes field on a candidate in CATS, preserving everything else on the candidate record unchanged. PREVIEW BY DEFAULT: call without confirm first to see current vs new notes. Call again with confirm: true to actually apply.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer"},
                "notes": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["candidate_id", "notes"],
        },
        "handler": tool_update_candidate_notes,
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
