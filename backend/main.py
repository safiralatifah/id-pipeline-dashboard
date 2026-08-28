import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI()

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"

with open(BASE_DIR / "team_roster.json", encoding="utf-8") as f:
    TEAM_ROSTER: dict[str, dict] = json.load(f)

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

_DASH_RE = re.compile("[-‐‑‒–—―]")


def _normalize_stage(s: str) -> str:
    return _DASH_RE.sub("-", s).strip().lower()


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


AGING_BUCKETS = [
    ("0-7d", 0, 7),
    ("7-15d", 8, 15),
    ("15-30d", 16, 30),
    ("30d+", 31, None),
]


def _bucket_for(days: int) -> str:
    for label, lo, hi in AGING_BUCKETS:
        if days >= lo and (hi is None or days <= hi):
            return label
    return "unknown"


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
    now = datetime.now(timezone.utc)
    total = len(items)
    # Stage names come back from the CRM with inconsistent dash characters
    # (hyphen vs en dash) — normalize before matching against known buckets.
    stage_counts = Counter(_normalize_stage(r.get("stage") or "") for r in items)

    # NOTE: the CRM also exposes is_won/is_closed booleans, but they came back
    # as truthy for every record when tried here (likely serialized as the
    # string "false", which Python treats as truthy) — stick to the
    # dash-normalized stage name, which has been verified correct.
    won = stage_counts.get(_normalize_stage("Closed–Won"), 0)
    lost = stage_counts.get(_normalize_stage("Closed–Lost"), 0)
    future = stage_counts.get(_normalize_stage("Future Opportunity"), 0)
    open_stages = [
        {"name": s, "count": stage_counts.get(_normalize_stage(s), 0)} for s in OPEN_STAGE_ORDER
    ]
    open_active = sum(s["count"] for s in open_stages)
    closed_total = won + lost
    closed_stage_names = {_normalize_stage("Closed–Won"), _normalize_stage("Closed–Lost")}
    open_items = [r for r in items if _normalize_stage(r.get("stage") or "") not in closed_stage_names]

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

    # Aging: how long each OPEN deal has sat since creation.
    aging_bucket_counts = Counter()
    for r in open_items:
        created = _parse_dt(r.get("created_at"))
        if created:
            aging_bucket_counts[_bucket_for((now - created).days)] += 1
    aging_buckets = [
        {"label": label, "count": aging_bucket_counts.get(label, 0)}
        for label, _, _ in AGING_BUCKETS
    ]

    # Stage duration: how long each OPEN deal has sat in its CURRENT stage,
    # and the per-stage age-bucket breakdown that surfaces bottlenecks: for
    # each stage, how many of its open deals are fresh vs. stuck.
    stage_duration_days: dict[str, list[int]] = {}
    stage_duration_bucket_counts = Counter()
    stage_bucket_matrix: dict[str, Counter] = {}
    for r in open_items:
        changed = _parse_dt(r.get("stage_last_changed_at")) or _parse_dt(r.get("created_at"))
        if not changed:
            continue
        days = (now - changed).days
        bucket = _bucket_for(days)
        stage_name = r.get("stage") or "(blank)"
        stage_duration_bucket_counts[bucket] += 1
        stage_duration_days.setdefault(stage_name, []).append(days)
        stage_bucket_matrix.setdefault(stage_name, Counter())[bucket] += 1
    stage_duration = sorted(
        (
            {"name": stage, "avg_days": round(sum(ds) / len(ds), 1), "count": len(ds)}
            for stage, ds in stage_duration_days.items()
        ),
        key=lambda x: -x["avg_days"],
    )
    stage_duration_buckets = [
        {"label": label, "count": stage_duration_bucket_counts.get(label, 0)}
        for label, _, _ in AGING_BUCKETS
    ]
    bucket_labels = [label for label, _, _ in AGING_BUCKETS]
    stage_grid_order = OPEN_STAGE_ORDER + ["Future Opportunity"]
    stage_duration_grid = sorted(
        (
            {
                "stage": stage_name,
                "total": sum(counts.values()),
                "buckets": [{"label": lbl, "count": counts.get(lbl, 0)} for lbl in bucket_labels],
            }
            for stage_name, counts in stage_bucket_matrix.items()
        ),
        key=lambda row: (
            stage_grid_order.index(row["stage"]) if row["stage"] in stage_grid_order else len(stage_grid_order)
        ),
    )

    # Forecast: OPEN deals' expected close date, bucketed by month, for the
    # current month plus the next two.
    month_keys = []
    cursor = now.replace(day=1)
    for _ in range(3):
        month_keys.append(cursor.strftime("%Y-%m"))
        cursor = (cursor + timedelta(days=32)).replace(day=1)
    forecast_by_month = {k: {"count": 0, "revenue": 0.0} for k in month_keys}
    for r in open_items:
        close_date = _parse_dt(r.get("expected_close_date"))
        if not close_date:
            continue
        key = close_date.strftime("%Y-%m")
        if key in forecast_by_month:
            forecast_by_month[key]["count"] += 1
            forecast_by_month[key]["revenue"] += float(r.get("total_potential_revenue_mth") or 0)
    forecast_next_3_months = [
        {"month": k, "count": v["count"], "revenue": v["revenue"]}
        for k, v in forecast_by_month.items()
    ]

    # Created-by-team: every matched deal (open + closed), grouped by the
    # month it was created and by the owner's Manager / Sales Head, per the
    # team roster (Active reps only — see backend/team_roster.json).
    created_month_keys = [now.strftime("%Y-%m")]
    cursor = now.replace(day=1)
    for _ in range(2):
        cursor = (cursor - timedelta(days=1)).replace(day=1)
        created_month_keys.append(cursor.strftime("%Y-%m"))
    created_month_keys = sorted(set(created_month_keys))

    manager_totals: dict[str, dict[str, int]] = {}
    sales_head_totals: dict[str, dict[str, int]] = {}
    unmapped_owners = set()
    for r in items:
        created = _parse_dt(r.get("created_at"))
        if not created:
            continue
        month_key = created.strftime("%Y-%m")
        owner = r.get("owner_name") or ""
        roster_entry = TEAM_ROSTER.get(owner)
        manager = (roster_entry or {}).get("manager") or "Unmapped"
        sales_head = (roster_entry or {}).get("sales_head") or "Unmapped"
        if not roster_entry:
            unmapped_owners.add(owner)
        manager_totals.setdefault(manager, {}).setdefault(month_key, 0)
        manager_totals[manager][month_key] += 1
        sales_head_totals.setdefault(sales_head, {}).setdefault(month_key, 0)
        sales_head_totals[sales_head][month_key] += 1

    def _rollup(totals: dict[str, dict[str, int]]) -> list[dict]:
        rows = []
        for name, by_month in totals.items():
            counts = {mk: by_month.get(mk, 0) for mk in created_month_keys}
            rows.append({"name": name, "counts": counts, "total": sum(counts.values())})
        return sorted(rows, key=lambda x: -x["total"])

    created_by_month = {
        "months": created_month_keys,
        "by_manager": _rollup(manager_totals),
        "by_sales_head": _rollup(sales_head_totals),
        "unmapped_owner_count": len(unmapped_owners),
        "unmapped_owners": sorted(o for o in unmapped_owners if o),
    }

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
        "aging_buckets": aging_buckets,
        "stage_duration": stage_duration,
        "stage_duration_buckets": stage_duration_buckets,
        "stage_duration_grid": stage_duration_grid,
        "forecast_next_3_months": forecast_next_3_months,
        "created_by_month": created_by_month,
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
