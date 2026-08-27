import json
import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI()

STATIC_DIR = Path(__file__).parent / "static"

CRM_BASE = "https://api.ninjavan.co/global/salescrm/api/v1"
RECORD_TYPE_INDONESIA = "12"
LOOKBACK_DAYS = 90
EXCLUDE_NAME_SUBSTR = "UNAUTHORIZED OPPORTUNITY"

OPEN_STAGE_ORDER = [
    "Prospecting", "New", "Proposal Submitted", "Negotiation",
    "EKYC Approval", "Contract Sent", "Agreed to Ship", "Onboarding",
    "Ready to Ship",
]
PRODUCT_LINE_ORDER = [
    "Restock", "LTL", "Cold Chain", "Cross-border", "Fulfillment",
    "Last Mile – Parcel", "Complex Logistics", "Last Mile – Cargo",
    "Last Mile – Document", "Digital +", "Cross-border SG",
    "Forward Stocking Locations", "Ninja FieldSight",
]
# service_level and core_product are multi_picklist fields — membership uses
# "contains", not "equals" (equals silently returns 0 rows for this field type).
SERVICE_LEVEL_VALUES = ["Same Day", "Standard", "LTL", "Next Day", "FTL", "Dedicated", "FCL", "LCL"]

_cache: dict[str, Any] = {"data": None, "fetched_at": 0.0}
CACHE_TTL_SECONDS = 300


async def fetch_all_opportunities(client: httpx.AsyncClient) -> list[dict]:
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")

    created_since = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    filters = {
        "logic": "AND",
        "conditions": [
            {"field": "record_type_id", "operator": "equals", "value": RECORD_TYPE_INDONESIA},
            {"field": "created_at", "operator": "greater_than", "value": created_since},
        ],
    }

    items: list[dict] = []
    page = 1
    while True:
        resp = await client.get(
            f"{CRM_BASE}/objects/Opportunity/records",
            headers={"X-API-Key": api_key},
            params={"filters": json.dumps(filters), "page_size": 100, "page": page},
            timeout=20,
        )
        if resp.status_code == 401:
            raise HTTPException(
                status_code=502,
                detail="CRM API key rejected (expired or revoked) — rotate CRM_API_KEY",
            )
        resp.raise_for_status()
        data = resp.json()
        items.extend(data["items"])
        if not data.get("has_next"):
            break
        page += 1

    return [r for r in items if EXCLUDE_NAME_SUBSTR not in (r.get("name") or "")]


def build_dashboard(items: list[dict]) -> dict:
    total = len(items)
    stage_counts = Counter(r.get("stage") or "" for r in items)

    won = stage_counts.get("Closed–Won", 0)
    lost = stage_counts.get("Closed–Lost", 0)
    future = stage_counts.get("Future Opportunity", 0)
    open_stages = [{"name": s, "count": stage_counts.get(s, 0)} for s in OPEN_STAGE_ORDER]
    open_active = sum(s["count"] for s in open_stages)
    closed_total = won + lost

    product_counts = Counter(r.get("nv_product_line") or "" for r in items)
    product_lines = [
        {"name": p, "count": product_counts.get(p, 0)}
        for p in PRODUCT_LINE_ORDER
        if product_counts.get(p, 0) > 0
    ]

    service_counts = {
        v: sum(1 for r in items if v in (r.get("service_level") or []))
        for v in SERVICE_LEVEL_VALUES
    }
    service_levels = [
        {"name": k, "count": v}
        for k, v in sorted(service_counts.items(), key=lambda x: -x[1])
        if v > 0
    ]
    service_blank = sum(1 for r in items if not r.get("service_level"))

    owner_counts = Counter(r.get("owner_name") or "(blank)" for r in items)
    owners = [{"name": k, "count": v} for k, v in owner_counts.most_common()]

    revenue = sum(float(r.get("total_potential_revenue_mth") or 0) for r in items)
    committed = sum(float(r.get("committed_revenue_mth") or 0) for r in items)

    return {
        "total": total,
        "open_pipeline": open_active + future,
        "open_active": open_active,
        "open_future": future,
        "closed_won": won,
        "closed_lost": lost,
        "closed_total": closed_total,
        "win_rate": round(won / closed_total * 100, 1) if closed_total else 0,
        "revenue_potential_mth": revenue,
        "revenue_committed_mth": committed,
        "open_stages": open_stages,
        "closed_stages": [
            {"name": "Closed–Won", "count": won},
            {"name": "Closed–Lost", "count": lost},
            {"name": "Future Opportunity", "count": future},
        ],
        "product_lines": product_lines,
        "service_levels": service_levels,
        "service_blank": service_blank,
        "owners": owners,
        "lookback_days": LOOKBACK_DAYS,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/pipeline")
async def get_pipeline():
    now = time.time()
    if _cache["data"] is not None and now - _cache["fetched_at"] < CACHE_TTL_SECONDS:
        return _cache["data"]

    async with httpx.AsyncClient() as client:
        items = await fetch_all_opportunities(client)

    result = build_dashboard(items)
    result["snapshot_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _cache["data"] = result
    _cache["fetched_at"] = now
    return result


@app.get("/{full_path:path}")
def serve_dashboard(full_path: str):
    return FileResponse(STATIC_DIR / "index.html")
