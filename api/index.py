"""
CATS ATS — MCP server for Claude connectors, deployed as a Vercel Python
serverless function using Streamable HTTP transport.

Endpoint: POST /api/mcp/<CONNECTOR_SHARED_KEY>
CATS:     Authorization: Token <CATS_API_KEY>  (set as env var, never sent by client)

v2 (July 2026) changes:
  - All responses auto-shaped: _embedded flattened to a top-level array,
    _links stripped, pagination (total/per_page/pages/has_more) at top level.
  - filter_jobs / filter_candidates: one-call filtered pulls (paging happens
    inside the connector, not in Claude).
  - Activities entity added (read + preview-gated write, candidates and contacts).
  - Lists read side added (list_candidate_lists, get_list_items, remove_list_item).
  - Candidate writes: create_candidate, update_candidate,
    upload_candidate_attachment, remove_candidate_tag (single-tag only —
    deliberately no bulk replace, so flags like 'Never Employ' can't be wiped).
  - get_contact, get_job_applications.

Read tools:
  list_jobs, get_job, get_company, list_job_statuses,
  list_pipeline_candidates, get_workflow_statuses,
  get_candidate, get_candidate_resume, read_resume, list_recent_candidates,
  get_candidate_tags, get_candidate_custom_fields, get_candidate_pipeline_history,
  list_portals, search_candidates, search_pipelines_by_status,
  search_candidates_deep, search_companies, search_contacts

Write tools (preview-by-default, require confirm: true to execute):
  create_job, change_job_status, add_candidate_to_pipeline,
  change_pipeline_status, create_candidate_list, add_candidates_to_list,
  publish_job_to_portal, update_pipeline_rating_status, bulk_update_pipelines,
  update_job_notes, update_candidate_notes, add_candidate_tag

Endpoints inferred from CATS's consistent API patterns rather than fully
confirmed against live docs — flagged in their own descriptions/responses:
  change_pipeline_status, publish_job_to_portal, search_contacts,
  add_candidate_tag (endpoint path inferred; built against the "Attach" /
  additive pattern rather than "Replace" to avoid wiping existing tags —
  see tool description).
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

# Reported by the MCP 'initialize' handshake. Derived from the Vercel build
# commit so the version string can never drift from the deployed code.
SERVER_VERSION = os.environ.get("VERCEL_GIT_COMMIT_SHA", "dev")[:7]

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



async def cats_delete(path: str, body: dict | None = None):
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.request("DELETE", f"{CATS_API_BASE}{path}", headers=cats_headers(), json=body)
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        try:
            return resp.json()
        except Exception:
            return {"deleted": True}


async def cats_post_multipart(path: str, filename: str, content: bytes, extra: dict | None = None):
    headers = {"Authorization": f"Token {CATS_API_KEY}"}
    files = {"file": (filename, content)}
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(f"{CATS_API_BASE}{path}", headers=headers, files=files, data=extra or {})
        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=resp.text[:500])
        try:
            return resp.json()
        except Exception:
            return {"uploaded": True}


def auto_shape(result):
    """v2: flatten HAL _embedded to a top-level array, strip _links, and
    surface pagination (total/count/page/pages/has_more) at top level so
    responses are directly usable without digging into nested structures.
    Applied globally in the MCP endpoint."""
    if not isinstance(result, dict):
        return result
    emb = result.get("_embedded")
    if isinstance(emb, dict):
        for key, val in emb.items():
            if key not in result:
                result[key] = val
        result.pop("_embedded", None)
    result.pop("_links", None)
    total = result.get("total")
    per_page = result.get("count") if isinstance(result.get("count"), int) else result.get("per_page")
    page = result.get("page")
    if isinstance(total, int) and isinstance(per_page, int) and per_page > 0:
        pages = (total + per_page - 1) // per_page
        result["per_page"] = per_page
        result["pages"] = pages
        if isinstance(page, int):
            result["has_more"] = page < pages
    return result


def _to_aware(value: str):
    """Parse an ISO date or datetime into a tz-aware UTC datetime.
    Accepts date-only ('2026-01-01'), 'Z' suffix, or full offset. Returns
    None if unparseable. Naive inputs are assumed UTC so comparisons against
    CATS's tz-aware timestamps never raise TypeError."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(str(value)[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _since_filter(items: list, since: str):
    since_dt = _to_aware(since)
    if since_dt is None:
        return items
    filtered = []
    for item in items:
        updated_dt = _to_aware(item.get("date_modified") or item.get("date_created"))
        if updated_dt is None:
            continue
        if updated_dt >= since_dt:
            filtered.append(item)
    return filtered


async def _fetch_candidates_newest_first(pages: int = 10, per_page: int = 100):
    """Pull candidates newest-first. CATS's /candidates list returns
    OLDEST-first and ignores a sort query param, so we read the LAST pages
    and reverse them. total/pages come from page 1; we then walk backwards
    from the final page. Returns a flat list, newest first."""
    first = await cats_get("/candidates", {"per_page": per_page, "page": 1})
    total = first.get("total", 0)
    if total == 0:
        return []
    last_page = (total + per_page - 1) // per_page
    collected = []
    p = last_page
    walked = 0
    while p >= 1 and walked < pages:
        data = await cats_get("/candidates", {"per_page": per_page, "page": p})
        items = data.get("_embedded", {}).get("candidates", [])
        collected.extend(reversed(items))  # reverse within page -> newest first
        walked += 1
        p -= 1
    return collected




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
    since = args.get("since")
    per_page = args.get("per_page", 50)
    page = args.get("page", 1)

    _all = await _fetch_candidates_newest_first(pages=10, per_page=100)
    if since:
        _all = _since_filter(_all, since)

    start = (page - 1) * per_page
    data = {
        "_embedded": {"candidates": _all[start:start + per_page]},
        "total": len(_all),
    }
    if since:
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


# ---- New: search / discovery primitives -----------------------------------

async def tool_search_candidates(args: dict):
    """Free-text candidate lookup — the primary unlock for name-based queries.
    Uses CATS's Search endpoint (free-text, accepts Boolean strings per user
    reports) rather than Filter (structured field matching). Search coverage
    of resume full-text vs the CATS UI search bar is not confirmed — if a
    known candidate doesn't surface, fall back to list_recent_candidates or
    ask for their candidate_id directly."""
    query = args["query"]
    page = args.get("page", 1)
    per_page = args.get("per_page", 25)
    data = await cats_get("/candidates/search", {"query": query, "page": page, "per_page": per_page})
    return data


async def tool_search_pipelines_by_status(args: dict):
    """Bulk pipeline query for candidates at a given pipeline status.

    CATS v3 has NO 'filter pipelines' endpoint (Pipelines is not one of the
    five filterable entities), so this is built on the endpoints that DO
    exist: pipelines are listed per-job (GET /jobs/{id}/pipelines) or per-
    candidate. We therefore always work job-by-job and filter status locally.

    - job_id given: list that one job's pipelines, filter to the target
      status. status name is resolved against that job's own workflow.
    - no job_id: iterate open jobs, pull each job's pipelines, filter each to
      the target status_id. status_id is REQUIRED here (a status *name* is
      per-workflow and can't be resolved without a job), and the scan is
      capped to keep the call bounded — narrow with since where possible.
    """
    status_id = args.get("status_id")
    status_name = args.get("status")
    job_id = args.get("job_id")
    since = args.get("since")
    per_page = min(args.get("per_page", 100), 200)
    max_jobs = min(args.get("max_jobs", 50), 100)

    async def _resolve_status_id_for_job(jid):
        job = await cats_get(f"/jobs/{jid}")
        wf_id = job.get("pipeline_workflow_id")
        if not wf_id or not status_name:
            return None
        statuses = await cats_get(f"/pipelines/workflows/{wf_id}/statuses", {"per_page": 100})
        for s in statuses.get("_embedded", {}).get("statuses", []):
            if (s.get("title") or s.get("name", "")).lower() == status_name.lower():
                return s["id"]
        return None

    async def _pull_job_pipelines(jid):
        """All pipeline entries for one job, paged internally."""
        out = []
        page = 1
        while True:
            data = await cats_get(f"/jobs/{jid}/pipelines", {"per_page": per_page, "page": page})
            items = data.get("_embedded", {}).get("pipelines", [])
            out.extend(items)
            total = data.get("total", 0)
            if len(out) >= total or not items:
                break
            page += 1
        return out

    # Build the list of (job_id, target_status_id) pairs to scan.
    scan = []
    truncated_jobs = False

    if job_id:
        resolved = status_id
        if resolved is None:
            resolved = await _resolve_status_id_for_job(job_id)
        if resolved is None:
            return {
                "error": f"Could not resolve status '{status_name}' to a status_id for job {job_id}. "
                         "Pass status_id directly (use get_workflow_statuses on the job's workflow).",
            }
        scan.append((job_id, resolved))
    else:
        # Cross-job: needs an explicit status_id (names are per-workflow).
        if status_id is None:
            return {
                "error": "Cross-job search needs status_id (a status name is per-workflow and can't be "
                         "resolved without a job_id). Either pass job_id to scope to one job, or pass "
                         "status_id from get_workflow_statuses. Note: cross-job scanning assumes the same "
                         "status_id across jobs sharing a workflow.",
            }
        jobs_data = await cats_get("/jobs", {"per_page": 100, "page": 1})
        jobs = jobs_data.get("_embedded", {}).get("jobs", [])
        if len(jobs) > max_jobs:
            jobs = jobs[:max_jobs]
            truncated_jobs = True
        for j in jobs:
            scan.append((j["id"], status_id))

    # Pull and filter.
    matched = []
    for jid, target_status in scan:
        try:
            pipelines = await _pull_job_pipelines(jid)
        except HTTPException:
            continue
        for p in pipelines:
            if p.get("status_id") == target_status:
                matched.append(p)

    if since:
        matched = _since_filter(matched, since)

    # Hydrate job titles and candidate contact details.
    job_ids = {m.get("job_id") for m in matched if m.get("job_id")}
    job_titles = {}
    for jid in job_ids:
        try:
            job = await cats_get(f"/jobs/{jid}")
            job_titles[jid] = job.get("title")
        except HTTPException:
            continue

    candidate_ids = {m.get("candidate_id") for m in matched if m.get("candidate_id")}
    candidate_info = {}
    for cid in candidate_ids:
        try:
            cand = await cats_get(f"/candidates/{cid}")
            candidate_info[cid] = {
                "name": f"{cand.get('first_name', '')} {cand.get('last_name', '')}".strip(),
                "email": (cand.get("emails") or {}).get("primary"),
                "mobile": (cand.get("phones") or {}).get("mobile"),
            }
        except HTTPException:
            continue

    rows = []
    for m in matched:
        cid = m.get("candidate_id")
        info = candidate_info.get(cid, {})
        rows.append({
            "candidate_id": cid,
            "name": info.get("name"),
            "email": info.get("email"),
            "mobile": info.get("mobile"),
            "job_id": m.get("job_id"),
            "job_title": job_titles.get(m.get("job_id")),
            "status_name": status_name or f"status_id {status_id}",
            "date_modified": m.get("date_modified"),
        })

    note = None
    if truncated_jobs:
        note = (f"Scanned the first {max_jobs} jobs only — cross-job results may be incomplete. "
                "Scope with job_id, or raise max_jobs.")
    return {
        "count": len(rows),
        "rows": rows,
        "jobs_scanned": len(scan),
        "truncated": truncated_jobs,
        "note": note,
    }


async def tool_search_candidates_deep(args: dict):
    """Full-text CV keyword search — for terms profile search misses
    (canonical case: security clearances). Slow tool — always bound with
    since or max_results. Includes built-in synonym handling for common
    Australian security clearance terminology."""
    keyword = args["keyword"]
    since = args.get("since")
    max_results = min(args.get("max_results", 50), 100)

    SYNONYMS = {
        "nv1": ["nv1", "nv-1", "negative vetting 1", "negative vetting level 1"],
        "nv2": ["nv2", "nv-2", "negative vetting 2", "negative vetting level 2"],
        "baseline": ["baseline", "baseline clearance", "baseline vetting"],
        "pv": ["pv", "positive vetting"],
    }
    kw_lower = keyword.lower().strip()
    variants = SYNONYMS.get(kw_lower, [kw_lower])
    if kw_lower in SYNONYMS:
        variants = variants + ["agsva"]  # broader clearance context marker

    candidates_checked = 0
    matches = []

    # Newest-first so recent CVs are actually reached (CATS lists oldest-first).
    pool = await _fetch_candidates_newest_first(pages=10, per_page=100)
    if since:
        pool = _since_filter(pool, since)

    for cand in pool:
        if len(matches) >= max_results or candidates_checked >= max_results * 8:
            break
        candidates_checked += 1
        cid = cand["id"]
        try:
            attachments = await cats_get(f"/candidates/{cid}/attachments", {"per_page": 10})
        except HTTPException:
            continue
        resumes = [a for a in attachments.get("_embedded", {}).get("attachments", []) if a.get("is_resume")]
        if not resumes:
            continue
        latest = resumes[0]
        try:
            content, _ = await cats_get_binary(f"/attachments/{latest['id']}/download")
            text = extract_text_from_bytes(content, latest.get("filename", ""))
        except Exception:
            continue

        text_lower = text.lower()
        hit_terms = [v for v in variants if v in text_lower]
        if hit_terms:
            idx = text_lower.find(hit_terms[0])
            start = max(0, idx - 60)
            end = min(len(text), idx + 60)
            snippet = text[start:end].replace("\n", " ").strip()
            matches.append({
                "candidate_id": cid,
                "name": f"{cand.get('first_name', '')} {cand.get('last_name', '')}".strip(),
                "email": (cand.get("emails") or {}).get("primary"),
                "mobile": (cand.get("phones") or {}).get("mobile"),
                "matched_terms": hit_terms,
                "snippet": snippet,
                "attachment_id": latest["id"],
            })

    return {
        "keyword": keyword,
        "variants_matched_against": variants,
        "candidates_scanned": candidates_checked,
        "match_count": len(matches),
        "matches": matches,
        "note": "Snippets shown for verification — check context before acting (e.g. 'NV1 sought' vs 'holds active NV1' read very differently).",
    }


async def tool_search_companies(args: dict):
    query = args["query"]
    page = args.get("page", 1)
    per_page = args.get("per_page", 25)
    return await cats_get("/companies/search", {"query": query, "page": page, "per_page": per_page})


async def tool_search_contacts(args: dict):
    """Endpoint path inferred from CATS's consistent /search pattern used by
    candidates and companies — not directly confirmed in docs. Report back
    if this errors."""
    query = args["query"]
    page = args.get("page", 1)
    per_page = args.get("per_page", 25)
    return await cats_get("/contacts/search", {"query": query, "page": page, "per_page": per_page})


async def tool_add_candidate_tag(args: dict):
    """Adds (does not replace) tags on one or more candidates. Built against
    CATS's additive 'Attach tags' pattern, not the destructive 'Replace tags'
    pattern — this preserves any existing tags (including flags like 'Never
    Employ') rather than overwriting them. Exact endpoint path is inferred
    from the equivalent, confirmed job/company tag endpoints — report back
    if this errors."""
    candidate_ids = args.get("candidate_ids") or ([args["candidate_id"]] if args.get("candidate_id") else [])
    tag = args["tag"]

    if not candidate_ids:
        return {"error": "Provide candidate_id or candidate_ids."}

    if not args.get("confirm"):
        names = []
        for cid in candidate_ids:
            try:
                cand = await cats_get(f"/candidates/{cid}")
                names.append(f"{cand.get('first_name', '')} {cand.get('last_name', '')}".strip())
            except HTTPException:
                names.append(f"(candidate {cid} — could not fetch name)")
        return {
            "preview": True,
            "action": "add_candidate_tag",
            "candidate_ids": candidate_ids,
            "candidate_names": names,
            "would_add_tag": tag,
            "note": "This adds the tag without removing any existing tags. Nothing has changed yet. "
                    "Call again with confirm: true to actually apply this in CATS.",
        }

    results = []
    for cid in candidate_ids:
        try:
            result = await cats_post(f"/candidates/{cid}/tags", {"tags": [tag]})
            results.append({"candidate_id": cid, "success": True, "result": result})
        except HTTPException as e:
            results.append({"candidate_id": cid, "success": False, "error": str(e.detail)})

    return {"changed": True, "action": "add_candidate_tag", "tag": tag, "results": results}



# ============================ v2 additions ============================

# ---- Server-side filtering (one call replaces multi-page pulls) ------

_JOB_FILTER_FIELDS = {"date_created", "date_modified", "status_id", "company_id", "title"}
_CAND_FILTER_FIELDS = {"date_created", "date_modified", "source", "city", "state"}


def _apply_local_filters(items: list, filters: list) -> list:
    out = []
    for item in items:
        ok = True
        for f in filters:
            field, op, value = f.get("field"), f.get("op", "eq"), f.get("value")
            actual = item.get(field)
            if op in (">=", "gte") and field.startswith("date"):
                if not actual or str(actual)[:19] < str(value)[:19]:
                    ok = False
            elif op in ("<=", "lte") and field.startswith("date"):
                if not actual or str(actual)[:19] > str(value)[:19]:
                    ok = False
            elif op == "contains":
                if value is None or actual is None or str(value).lower() not in str(actual).lower():
                    ok = False
            else:  # eq
                if str(actual) != str(value):
                    ok = False
            if not ok:
                break
        if ok:
            out.append(item)
    return out


async def _paged_filter(entity: str, key: str, filters: list, max_pages: int = 10):
    """Pull pages server-side (in Vercel, not in Claude) and filter locally.
    One MCP call regardless of CATS page count."""
    all_items = []
    page = 1
    pages_seen = 0
    truncated = False
    while True:
        data = await cats_get(f"/{entity}", {"per_page": 100, "page": page})
        items = data.get("_embedded", {}).get(key, [])
        all_items.extend(items)
        pages_seen += 1
        total = data.get("total", 0)
        if len(all_items) >= total or not items:
            break
        if pages_seen >= max_pages:
            truncated = True
            break
        page += 1
    matched = _apply_local_filters(all_items, filters)
    return {
        key: matched,
        "total": len(matched),
        "scanned": len(all_items),
        "truncated": truncated,
        "note": ("Scan capped at %d pages — results may be incomplete; narrow the filter." % max_pages) if truncated else None,
    }


async def tool_filter_jobs(args: dict):
    """One-call filtered job pull. Filters run inside the connector (Vercel)
    so Claude never pages through the full job list. Supported fields:
    date_created, date_modified (ops: >=, <=), status_id, company_id (eq),
    title (contains)."""
    filters = args["filters"]
    for f in filters:
        if f.get("field") not in _JOB_FILTER_FIELDS:
            raise HTTPException(status_code=422, detail=f"Unsupported job filter field: {f.get('field')}. Supported: {sorted(_JOB_FILTER_FIELDS)}")
    result = await _paged_filter("jobs", "jobs", filters)
    statuses = await cats_get("/jobs/statuses", {"per_page": 100})
    status_map = {s["id"]: s.get("title") or s.get("name") for s in statuses.get("_embedded", {}).get("statuses", [])}
    for job in result["jobs"]:
        job["status_name"] = status_map.get(job.get("status_id"), f"Unknown ({job.get('status_id')})")
    return result


async def tool_filter_candidates(args: dict):
    """One-call filtered candidate pull — same mechanics as filter_jobs.
    Supported fields: date_created, date_modified (>=, <=), source, city,
    state (eq/contains). Scan is capped at 10 pages (1000 newest candidates,
    sorted by date_modified descending) — use date bounds to stay inside it."""
    filters = args["filters"]
    for f in filters:
        if f.get("field") not in _CAND_FILTER_FIELDS:
            raise HTTPException(status_code=422, detail=f"Unsupported candidate filter field: {f.get('field')}. Supported: {sorted(_CAND_FILTER_FIELDS)}")
    all_items = await _fetch_candidates_newest_first(pages=10, per_page=100)
    truncated = len(all_items) >= 1000
    matched = _apply_local_filters(all_items, filters)
    return {"candidates": matched, "total": len(matched), "scanned": len(all_items),
            "truncated": truncated,
            "note": ("Scanned the newest 1000 candidates; older records not checked — narrow with a date bound if needed." if truncated else None)}


# ---- Activities (interaction history — calls, emails, meetings) ------

async def tool_list_candidate_activities(args: dict):
    """Full interaction history for a candidate — logged calls, emails,
    meetings, notes, with dates and authors. Endpoint follows the confirmed
    /candidates/{id}/attachments and /tags sub-resource pattern — inferred,
    report back if it errors."""
    candidate_id = args["candidate_id"]
    per_page = args.get("per_page", 50)
    page = args.get("page", 1)
    return await cats_get(f"/candidates/{candidate_id}/activities", {"per_page": per_page, "page": page})


async def tool_create_candidate_activity(args: dict):
    """Log a call, email summary, meeting, or note against a candidate as a
    proper timestamped activity (the durable alternative to overwriting the
    single notes field). 'date' is when the activity occurred; CATS sets
    date_created itself. Endpoint inferred — report back if it errors."""
    candidate_id = args["candidate_id"]
    activity_type = args.get("type", "note")
    notes = args["notes"]
    date = args.get("date")

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "create_candidate_activity",
            "candidate_id": candidate_id,
            "would_log": {"type": activity_type, "notes": notes, "date": date},
            "note": "Nothing has changed yet. Call again with confirm: true to log this activity in CATS.",
        }

    body = {"type": activity_type, "notes": notes}
    if date:
        body["date"] = date
    result = await cats_post(f"/candidates/{candidate_id}/activities", body)
    return {"changed": True, "action": "create_candidate_activity", "candidate_id": candidate_id, "result": result}


async def tool_list_contact_activities(args: dict):
    """Interaction history for a contact (client-side: hiring manager calls,
    briefs, feedback). Endpoint inferred from the candidate pattern —
    report back if it errors."""
    contact_id = args["contact_id"]
    per_page = args.get("per_page", 50)
    page = args.get("page", 1)
    return await cats_get(f"/contacts/{contact_id}/activities", {"per_page": per_page, "page": page})


async def tool_create_contact_activity(args: dict):
    """Log a call, meeting, or note against a contact. Preview-by-default.
    Endpoint inferred — report back if it errors."""
    contact_id = args["contact_id"]
    activity_type = args.get("type", "note")
    notes = args["notes"]
    date = args.get("date")

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "create_contact_activity",
            "contact_id": contact_id,
            "would_log": {"type": activity_type, "notes": notes, "date": date},
            "note": "Nothing has changed yet. Call again with confirm: true to log this activity in CATS.",
        }

    body = {"type": activity_type, "notes": notes}
    if date:
        body["date"] = date
    result = await cats_post(f"/contacts/{contact_id}/activities", body)
    return {"changed": True, "action": "create_contact_activity", "contact_id": contact_id, "result": result}


# ---- Lists: read side (previously write-only) -------------------------

async def tool_list_candidate_lists(args: dict):
    """List all candidate lists (hot lists) with their ids."""
    per_page = args.get("per_page", 100)
    page = args.get("page", 1)
    return await cats_get("/candidates/lists", {"per_page": per_page, "page": page})


async def tool_get_list_items(args: dict):
    """Read back the candidates on a list — the missing half of the lists
    workflow (create/add existed; contents were unreadable)."""
    list_id = args["list_id"]
    per_page = args.get("per_page", 100)
    page = args.get("page", 1)
    return await cats_get(f"/candidates/lists/{list_id}/items", {"per_page": per_page, "page": page})


async def tool_remove_list_item(args: dict):
    """Remove a candidate from a list (does not delete the candidate).
    Preview-by-default. Endpoint inferred — report back if it errors."""
    list_id = args["list_id"]
    item_id = args["item_id"]

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "remove_list_item",
            "list_id": list_id,
            "item_id": item_id,
            "note": "Nothing has changed yet. Call again with confirm: true to remove this item from the list.",
        }
    result = await cats_delete(f"/candidates/lists/{list_id}/items/{item_id}")
    return {"changed": True, "action": "remove_list_item", "list_id": list_id, "item_id": item_id, "result": result}


# ---- Candidate write operations ---------------------------------------

async def tool_create_candidate(args: dict):
    """Create a new candidate record (e.g. entering a sourced candidate from
    LinkedIn). Preview-by-default. Duplicate check: run search_candidates on
    the name/email first."""
    fields = {k: v for k, v in args.items() if k != "confirm" and v is not None}

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "create_candidate",
            "would_create": fields,
            "note": "Nothing has changed yet. Check for duplicates with search_candidates first, then call again with confirm: true.",
        }
    result = await cats_post("/candidates", fields)
    return {"changed": True, "action": "create_candidate", "result": result}


async def tool_update_candidate(args: dict):
    """Update fields on an existing candidate (phone, email, title, city
    etc.) — general-purpose companion to update_candidate_notes. Preview-by-
    default; only the fields you pass change."""
    candidate_id = args["candidate_id"]
    fields = {k: v for k, v in args.items() if k not in ("confirm", "candidate_id") and v is not None}

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "update_candidate",
            "candidate_id": candidate_id,
            "would_update": fields,
            "note": "Nothing has changed yet. Call again with confirm: true to apply.",
        }
    result = await cats_put(f"/candidates/{candidate_id}", fields)
    return {"changed": True, "action": "update_candidate", "candidate_id": candidate_id, "result": result}


async def tool_upload_candidate_attachment(args: dict):
    """Upload a file (formatted CV, submission cover page, interview pack)
    onto a candidate record. Content is base64. Set is_resume true for CVs
    so they appear in the resume history. Preview-by-default."""
    import base64
    candidate_id = args["candidate_id"]
    filename = args["filename"]
    is_resume = bool(args.get("is_resume", False))

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "upload_candidate_attachment",
            "candidate_id": candidate_id,
            "would_upload": {"filename": filename, "is_resume": is_resume,
                             "size_bytes": len(base64.b64decode(args["content_base64"]))},
            "note": "Nothing has changed yet. Call again with confirm: true to upload.",
        }
    content = base64.b64decode(args["content_base64"])
    result = await cats_post_multipart(
        f"/candidates/{candidate_id}/attachments", filename, content,
        {"is_resume": "true" if is_resume else "false"},
    )
    return {"changed": True, "action": "upload_candidate_attachment", "candidate_id": candidate_id, "result": result}


async def tool_remove_candidate_tag(args: dict):
    """Remove a single named tag from a candidate — the safe undo for
    add_candidate_tag (which is additive). Deliberately single-tag: there is
    still no replace-all-tags tool, so flags like 'Never Employ' can never be
    wiped in bulk. Preview-by-default. Endpoint inferred — report back if it
    errors."""
    candidate_id = args["candidate_id"]
    tag_id = args["tag_id"]

    if not args.get("confirm"):
        return {
            "preview": True,
            "action": "remove_candidate_tag",
            "candidate_id": candidate_id,
            "tag_id": tag_id,
            "note": "Nothing has changed yet. Get tag_id from get_candidate_tags. Call again with confirm: true to remove this one tag.",
        }
    result = await cats_delete(f"/candidates/{candidate_id}/tags/{tag_id}")
    return {"changed": True, "action": "remove_candidate_tag", "candidate_id": candidate_id, "tag_id": tag_id, "result": result}


# ---- Contacts & jobs: small read gaps ----------------------------------

async def tool_get_contact(args: dict):
    """Get a contact's full record by CATS contact id (search_contacts finds
    them; this reads them)."""
    contact_id = args["contact_id"]
    return await cats_get(f"/contacts/{contact_id}")


async def tool_get_job_applications(args: dict):
    """Application records for a job — application-level data distinct from
    pipeline entries. Endpoint inferred — report back if it errors."""
    job_id = args["job_id"]
    per_page = args.get("per_page", 100)
    page = args.get("page", 1)
    return await cats_get(f"/jobs/{job_id}/applications", {"per_page": per_page, "page": page})


TOOLS = {
    # ---- v2 additions ----
    "filter_jobs": {
        "description": "Filtered job pull in ONE call — the connector scans all job pages internally and returns only matches. Filters: [{field, op, value}]. Fields: date_created/date_modified (ops '>=', '<='), status_id/company_id (eq), title (contains). Use this instead of paging list_jobs for 'jobs since date X' questions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "filters": {"type": "array", "items": {"type": "object", "properties": {
                    "field": {"type": "string"}, "op": {"type": "string", "default": "eq"}, "value": {}}, "required": ["field", "value"]}},
            },
            "required": ["filters"],
        },
        "handler": tool_filter_jobs,
    },
    "filter_candidates": {
        "description": "Filtered candidate pull in ONE call, scanning the 1000 most recently modified candidates internally. Filters: [{field, op, value}]. Fields: date_created/date_modified ('>=','<='), source/city/state (eq or contains). Use date bounds to stay inside the scan window.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "filters": {"type": "array", "items": {"type": "object", "properties": {
                    "field": {"type": "string"}, "op": {"type": "string", "default": "eq"}, "value": {}}, "required": ["field", "value"]}},
            },
            "required": ["filters"],
        },
        "handler": tool_filter_candidates,
    },
    "list_candidate_activities": {
        "description": "Full interaction history for a candidate — logged calls, emails, meetings and notes with dates and authors. Read this before screening or contacting anyone: it is the relationship record. (Endpoint inferred — report back if it errors.)",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}, "per_page": {"type": "integer", "default": 50}, "page": {"type": "integer", "default": 1}},
            "required": ["candidate_id"],
        },
        "handler": tool_list_candidate_activities,
    },
    "create_candidate_activity": {
        "description": "Log a call, email summary, meeting or note against a candidate as a timestamped activity — use this instead of update_candidate_notes for interaction records. 'date' = when it occurred. Preview-by-default; requires confirm: true to execute.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer"},
                "type": {"type": "string", "description": "call, email, meeting, note, other", "default": "note"},
                "notes": {"type": "string"},
                "date": {"type": "string", "description": "ISO 8601, when the activity occurred"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["candidate_id", "notes"],
        },
        "handler": tool_create_candidate_activity,
    },
    "list_contact_activities": {
        "description": "Interaction history for a client contact — hiring manager calls, briefs, feedback. (Endpoint inferred — report back if it errors.)",
        "inputSchema": {
            "type": "object",
            "properties": {"contact_id": {"type": "integer"}, "per_page": {"type": "integer", "default": 50}, "page": {"type": "integer", "default": 1}},
            "required": ["contact_id"],
        },
        "handler": tool_list_contact_activities,
    },
    "create_contact_activity": {
        "description": "Log a call, meeting or note against a client contact. Preview-by-default; requires confirm: true.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "contact_id": {"type": "integer"},
                "type": {"type": "string", "default": "note"},
                "notes": {"type": "string"},
                "date": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["contact_id", "notes"],
        },
        "handler": tool_create_contact_activity,
    },
    "list_candidate_lists": {
        "description": "List all candidate lists (hot lists) with their ids — pair with get_list_items to read contents.",
        "inputSchema": {"type": "object", "properties": {"per_page": {"type": "integer", "default": 100}, "page": {"type": "integer", "default": 1}}},
        "handler": tool_list_candidate_lists,
    },
    "get_list_items": {
        "description": "Read back the candidates on a list — contents were previously write-only.",
        "inputSchema": {
            "type": "object",
            "properties": {"list_id": {"type": "integer"}, "per_page": {"type": "integer", "default": 100}, "page": {"type": "integer", "default": 1}},
            "required": ["list_id"],
        },
        "handler": tool_get_list_items,
    },
    "remove_list_item": {
        "description": "Remove a candidate from a list (candidate record untouched). Preview-by-default; requires confirm: true. (Endpoint inferred.)",
        "inputSchema": {
            "type": "object",
            "properties": {"list_id": {"type": "integer"}, "item_id": {"type": "integer"}, "confirm": {"type": "boolean", "default": False}},
            "required": ["list_id", "item_id"],
        },
        "handler": tool_remove_list_item,
    },
    "create_candidate": {
        "description": "Create a new candidate record (e.g. sourced from LinkedIn). Run search_candidates first to avoid duplicates. Preview-by-default; requires confirm: true. Common fields: first_name, last_name, emails, phones, title, city, state, source.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "first_name": {"type": "string"}, "last_name": {"type": "string"},
                "title": {"type": "string"}, "city": {"type": "string"}, "state": {"type": "string"},
                "source": {"type": "string"},
                "emails": {"type": "object"}, "phones": {"type": "object"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["first_name", "last_name"],
        },
        "handler": tool_create_candidate,
    },
    "update_candidate": {
        "description": "Update fields on an existing candidate (title, city, source etc.) — only the fields passed are changed. Preview-by-default; requires confirm: true.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer"},
                "first_name": {"type": "string"}, "last_name": {"type": "string"},
                "title": {"type": "string"}, "city": {"type": "string"}, "state": {"type": "string"},
                "source": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["candidate_id"],
        },
        "handler": tool_update_candidate,
    },
    "upload_candidate_attachment": {
        "description": "Upload a file (formatted CV, submission cover page, interview pack) onto a candidate record. content_base64 = base64 of the file; is_resume: true makes it appear in resume history. Preview-by-default; requires confirm: true.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer"},
                "filename": {"type": "string"},
                "content_base64": {"type": "string"},
                "is_resume": {"type": "boolean", "default": False},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["candidate_id", "filename", "content_base64"],
        },
        "handler": tool_upload_candidate_attachment,
    },
    "remove_candidate_tag": {
        "description": "Remove ONE named tag from a candidate — the safe undo for add_candidate_tag. Get tag_id from get_candidate_tags. Deliberately single-tag; no bulk replace exists so protective flags can't be wiped. Preview-by-default; requires confirm: true. (Endpoint inferred.)",
        "inputSchema": {
            "type": "object",
            "properties": {"candidate_id": {"type": "integer"}, "tag_id": {"type": "integer"}, "confirm": {"type": "boolean", "default": False}},
            "required": ["candidate_id", "tag_id"],
        },
        "handler": tool_remove_candidate_tag,
    },
    "get_contact": {
        "description": "Get a client contact's full record by CATS contact id (find the id with search_contacts).",
        "inputSchema": {
            "type": "object",
            "properties": {"contact_id": {"type": "integer"}},
            "required": ["contact_id"],
        },
        "handler": tool_get_contact,
    },
    "get_job_applications": {
        "description": "Application records for a job — application-level data distinct from pipeline entries. (Endpoint inferred — report back if it errors.)",
        "inputSchema": {
            "type": "object",
            "properties": {"job_id": {"type": "integer"}, "per_page": {"type": "integer", "default": 100}, "page": {"type": "integer", "default": 1}},
            "required": ["job_id"],
        },
        "handler": tool_get_job_applications,
    },

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
    "search_candidates": {
        "description": "Free-text candidate lookup by name or keyword — the primary way to find a candidate_id when you only have a name. Uses CATS's free-text Search (accepts Boolean strings, but coverage of resume full-text vs the CATS UI search bar is unconfirmed). E.g. 'get me Karen Crabb's mobile' -> search_candidates('Karen Crabb') -> get_candidate.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "page": {"type": "integer", "default": 1},
                "per_page": {"type": "integer", "default": 25},
            },
            "required": ["query"],
        },
        "handler": tool_search_candidates,
    },
    "search_pipelines_by_status": {
        "description": "Find candidates at a given pipeline status. Best used scoped to one job (pass job_id + a status name like 'Qualifying', resolved automatically against that job's workflow). Cross-job search (no job_id) requires status_id directly (from get_workflow_statuses) since status names are per-workflow, scans open jobs job-by-job, and is capped by max_jobs — narrow with since where possible. Returns name, email, mobile, job, and status per row. Built on CATS's per-job pipeline endpoints (there is no server-side pipeline filter in the CATS API).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "Readable status name, e.g. 'Qualifying'. Requires job_id to resolve."},
                "status_id": {"type": "integer", "description": "Pipeline status_id (from get_workflow_statuses). Required for cross-job search."},
                "job_id": {"type": "integer", "description": "Scope to one job — strongly preferred; lets 'status' be resolved by name."},
                "since": {"type": "string", "description": "ISO 8601 timestamp to bound results by date_modified."},
                "max_jobs": {"type": "integer", "default": 50, "description": "Cross-job scan cap (max 100)."},
                "per_page": {"type": "integer", "default": 100},
            },
        },
        "handler": tool_search_pipelines_by_status,
    },
    "search_candidates_deep": {
        "description": "Full-text CV keyword search for terms that profile/name search misses — canonical case: security clearances ('everyone who mentions NV1'). Built-in synonym handling for nv1/nv2/baseline/pv clearance variants. SLOW — always pass since to bound scope, or rely on the default max_results cap. Returns a matched snippet per hit so false positives ('NV1 sought' vs 'holds active NV1') are visible before acting.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string"},
                "since": {"type": "string", "description": "ISO 8601 timestamp — strongly recommended to bound scope."},
                "max_results": {"type": "integer", "default": 50},
            },
            "required": ["keyword"],
        },
        "handler": tool_search_candidates_deep,
    },
    "search_companies": {
        "description": "Free-text search for client companies by name or keyword.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "page": {"type": "integer", "default": 1},
                "per_page": {"type": "integer", "default": 25},
            },
            "required": ["query"],
        },
        "handler": tool_search_companies,
    },
    "search_contacts": {
        "description": "Free-text search for contacts (people at client companies) by name or keyword. Endpoint path inferred from CATS's consistent /search pattern used by candidates and companies — not directly confirmed in docs, report back if this errors.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "page": {"type": "integer", "default": 1},
                "per_page": {"type": "integer", "default": 25},
            },
            "required": ["query"],
        },
        "handler": tool_search_contacts,
    },
    "add_candidate_tag": {
        "description": "Add a tag to one or more candidates (e.g. tag everyone found via search_candidates_deep with 'NV1' so future queries are instant tag lookups instead of CV re-scans). ADDITIVE — does not remove existing tags. PREVIEW BY DEFAULT: call without confirm first to see candidate names and the tag to be applied. Call again with confirm: true to actually apply. Endpoint path inferred from the equivalent confirmed job/company tag endpoints — report back if this errors.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer", "description": "For a single candidate — use candidate_ids for bulk."},
                "candidate_ids": {"type": "array", "items": {"type": "integer"}, "description": "For bulk tagging."},
                "tag": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
            },
            "required": ["tag"],
        },
        "handler": tool_add_candidate_tag,
    },
}


# ---- MCP JSON-RPC over Streamable HTTP ------------------------------------

def rpc_result(id_, result):
    return {"jsonrpc": "2.0", "id": id_, "result": result}


def rpc_error(id_, code, message, data=None):
    err = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message, **({"data": data} if data is not None else {})}}


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
            "serverInfo": {"name": "cats-connector", "version": SERVER_VERSION},
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
        except Exception as e:
            import traceback
            return JSONResponse(rpc_error(id_, -32001,
                f"{tool_name} failed: {type(e).__name__}: {e}",
                data={"trace": traceback.format_exc()[-1500:]}))
        result = auto_shape(result)
        return JSONResponse(rpc_result(id_, {
            "content": [{"type": "text", "text": json.dumps(result)}]
        }))

    return JSONResponse(rpc_error(id_, -32601, f"Unknown method: {method}"))


@app.get("/api/mcp/{key}")
async def health(key: str):
    return {"status": "ok", "server": "cats-connector", "version": SERVER_VERSION, "time": datetime.now(timezone.utc).isoformat()}
