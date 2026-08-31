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


# Notebook staleness tiers — reps are expected to add a Notebook entry to
# every open opportunity at least every 7 days.
ACTIVITY_BUCKETS = [
    ("< 7d", 0, 6, "good"),
    ("7-14d", 7, 14, "warning"),
    ("14-30d", 15, 30, "orange"),
    ("30d+", 31, None, "critical"),
]


def _activity_bucket_for(days: int) -> tuple[str, str]:
    for label, lo, hi, color in ACTIVITY_BUCKETS:
        if days >= lo and (hi is None or days <= hi):
            return label, color
    return "30d+", "critical"


async def fetch_all_opportunities(client: httpx.AsyncClient) -> list[dict]:
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")

    # All Indonesia Opportunity records, full history — no created_at cutoff.
    filters = {
        "logic": "AND",
        "conditions": [
            {"field": "record_type_id", "operator": "equals", "value": RECORD_TYPE_INDONESIA},
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


async def fetch_notebook_last_touch(client: httpx.AsyncClient) -> dict[int, datetime]:
    """Most recent Notebook entry per Opportunity record_id, across all countries
    (the /notebook/entries endpoint has no record_type/country filter — we only
    ever look up ids that belong to our Indonesia open-opportunity set)."""
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")

    last_touch: dict[int, datetime] = {}
    page = 1
    while True:
        resp = await client.get(
            f"{CRM_BASE}/notebook/entries",
            headers={"X-API-Key": api_key},
            params={"object_type": "Opportunity", "page_size": 100, "page": page},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        for entry in data.get("items") or []:
            record_id = entry.get("record_id")
            created = _parse_dt(entry.get("created_at"))
            if record_id is None or not created:
                continue
            if record_id not in last_touch or created > last_touch[record_id]:
                last_touch[record_id] = created
        if not data.get("has_next"):
            break
        page += 1

    return last_touch


def build_dashboard(items: list[dict], notebook_last_touch: dict[int, datetime]) -> dict:
    now = datetime.now(timezone.utc)
    total = len(items)

    # Opportunities created this calendar month vs last, for the headline stat.
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = this_month_start - timedelta(seconds=1)
    last_month_start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    created_this_month = 0
    created_last_month = 0
    for r in items:
        created = _parse_dt(r.get("created_at"))
        if not created:
            continue
        if created >= this_month_start:
            created_this_month += 1
        elif last_month_start <= created <= last_month_end:
            created_last_month += 1
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

    # Product line, service level, and owner leaderboard only consider OPEN
    # (+ future) deals — closed-won/lost are excluded here per the user's ask.
    product_counts = Counter(r.get("nv_product_line") or "" for r in open_items)
    product_lines = [
        {"name": p, "count": product_counts.get(p, 0)}
        for p in PRODUCT_LINE_ORDER
        if product_counts.get(p, 0) > 0
    ]

    service_counts = {
        v: sum(1 for r in open_items if v in (r.get("service_level") or []))
        for v in SERVICE_LEVEL_VALUES
    }
    service_levels = [
        {"name": k, "count": v}
        for k, v in sorted(service_counts.items(), key=lambda x: -x[1])
        if v > 0
    ]
    service_blank = sum(1 for r in open_items if not r.get("service_level"))

    owner_counts = Counter(r.get("owner_name") or "(blank)" for r in open_items)
    owners = [{"name": k, "count": v} for k, v in owner_counts.most_common()]

    revenue = sum(float(r.get("total_potential_revenue_mth") or 0) for r in items)
    committed = sum(float(r.get("committed_revenue_mth") or 0) for r in items)

    # Aging: how long each OPEN deal has sat since creation, and which stage
    # it's currently sitting in — surfaces "old since creation, stuck at X".
    aging_bucket_counts = Counter()
    aging_stage_matrix: dict[str, Counter] = {}
    for r in open_items:
        created = _parse_dt(r.get("created_at"))
        if not created:
            continue
        bucket = _bucket_for((now - created).days)
        stage_name = r.get("stage") or "(blank)"
        aging_bucket_counts[bucket] += 1
        aging_stage_matrix.setdefault(bucket, Counter())[stage_name] += 1
    aging_buckets = [
        {"label": label, "count": aging_bucket_counts.get(label, 0)}
        for label, _, _ in AGING_BUCKETS
    ]
    aging_stage_order = OPEN_STAGE_ORDER + ["Future Opportunity"]
    aging_grid = []
    for label, _, _ in AGING_BUCKETS:
        counts = aging_stage_matrix.get(label, Counter())
        ordered = [s for s in aging_stage_order if counts.get(s)]
        ordered += [s for s in counts if s not in aging_stage_order]
        aging_grid.append({
            "label": label,
            "total": sum(counts.values()),
            "stages": [{"name": s, "count": counts[s]} for s in ordered],
        })

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

    # Forecast: OPEN deals by expected close date. Deals whose close date has
    # already passed go in a leading "Overdue" bucket; the rest are grouped
    # by month, from the current month through however far the data goes —
    # every stage represented, so it's obvious whether deals due to close
    # soon are actually in a late stage (Agreed to Ship / Ready to Ship) or
    # still early (an unrealistic forecast).
    def _stage_breakdown(rows: list[dict]) -> list[dict]:
        counts = Counter(r.get("stage") or "(blank)" for r in rows)
        revenue_by_stage: dict[str, float] = {}
        for r in rows:
            s = r.get("stage") or "(blank)"
            revenue_by_stage[s] = revenue_by_stage.get(s, 0.0) + float(r.get("total_potential_revenue_mth") or 0)
        order = OPEN_STAGE_ORDER + ["Future Opportunity"]
        ordered = [s for s in order if counts.get(s)] + [s for s in counts if s not in order]
        return [{"name": s, "count": counts[s], "revenue": revenue_by_stage[s]} for s in ordered]

    today = now.date()
    current_month_start = today.replace(day=1)
    overdue_items = []
    month_items: dict[str, list[dict]] = {}
    max_month_start = current_month_start
    undated_count = 0
    for r in open_items:
        close_date = _parse_dt(r.get("expected_close_date"))
        if not close_date:
            undated_count += 1
            continue
        close_day = close_date.date()
        if close_day < today:
            overdue_items.append(r)
        else:
            key = close_day.strftime("%Y-%m")
            month_items.setdefault(key, []).append(r)
            month_start = close_day.replace(day=1)
            if month_start > max_month_start:
                max_month_start = month_start

    months_seq = []
    cursor = current_month_start
    while cursor <= max_month_start:
        months_seq.append(cursor.strftime("%Y-%m"))
        cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)

    def _bucket(key: str, rows: list[dict]) -> dict:
        return {
            "key": key,
            "count": len(rows),
            "revenue": sum(float(r.get("total_potential_revenue_mth") or 0) for r in rows),
            "stages": _stage_breakdown(rows),
        }

    forecast = [_bucket("overdue", overdue_items)] + [
        _bucket(mk, month_items.get(mk, [])) for mk in months_seq
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

    # Activity: how recently each OPEN deal's Notebook was last touched.
    # Reps are expected to add an entry at least every 7 days. Falls back to
    # created_at when an opportunity has no Notebook entry at all (never
    # touched is not the same as "recently touched").
    activity_bucket_counts = Counter()
    owner_activity: dict[str, Counter] = {}
    owner_totals: dict[str, int] = {}
    activity_undated_count = 0

    for r in open_items:
        last_touch = notebook_last_touch.get(r.get("id")) or _parse_dt(r.get("created_at"))
        if not last_touch:
            activity_undated_count += 1
            continue
        days = (now - last_touch).days
        label, _color = _activity_bucket_for(days)
        activity_bucket_counts[label] += 1
        owner = r.get("owner_name") or "(blank)"
        owner_activity.setdefault(owner, Counter())[label] += 1
        owner_totals[owner] = owner_totals.get(owner, 0) + 1

    activity_labels = [b[0] for b in ACTIVITY_BUCKETS]
    activity_summary = [
        {"label": label, "color": color, "count": activity_bucket_counts.get(label, 0)}
        for label, _lo, _hi, color in ACTIVITY_BUCKETS
    ]
    touched_within_7d = activity_bucket_counts.get(activity_labels[0], 0)
    activity_touched_pct = (
        round(touched_within_7d / len(open_items) * 100, 1) if open_items else 0.0
    )
    activity_by_owner = sorted(
        (
            {
                "name": owner,
                "counts": {label: counts.get(label, 0) for label in activity_labels},
                "total": owner_totals[owner],
                "touched_pct": round(
                    counts.get(activity_labels[0], 0) / owner_totals[owner] * 100, 1
                ),
            }
            for owner, counts in owner_activity.items()
        ),
        key=lambda row: (-row["counts"][activity_labels[-1]], -row["total"]),
    )

    return {
        "total": total,
        "created_this_month": created_this_month,
        "created_last_month": created_last_month,
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
        "aging_grid": aging_grid,
        "stage_duration": stage_duration,
        "stage_duration_buckets": stage_duration_buckets,
        "stage_duration_grid": stage_duration_grid,
        "forecast": forecast,
        "forecast_undated_count": undated_count,
        "created_by_month": created_by_month,
        "activity_summary": activity_summary,
        "activity_touched_pct": activity_touched_pct,
        "activity_by_owner": activity_by_owner,
        "activity_undated_count": activity_undated_count,
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
        notebook_last_touch = await fetch_notebook_last_touch(client)

    result = build_dashboard(items, notebook_last_touch)
    result["snapshot_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _cache["data"] = result
    _cache["fetched_at"] = now
    return result


@app.get("/{full_path:path}")
def serve_dashboard(full_path: str):
    return FileResponse(STATIC_DIR / "index.html")
