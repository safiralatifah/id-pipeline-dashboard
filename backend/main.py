import asyncio
import calendar
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from starlette.concurrency import run_in_threadpool

import storage

app = FastAPI()

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"


def _name_key(name: str | None) -> str:
    """Canonical key for case- AND whitespace-insensitive name matching. The
    roster, the SSO email map, and CRM owner_names are all hand-maintained and
    don't agree on casing or spacing, so every viewer-scope and owner/manager
    comparison canonicalizes through this. Mirrors Task Progress Monitor's
    casefold-based scoping — a casing mismatch must never resolve a viewer's
    scope to empty (which used to fall open to "see everything")."""
    return " ".join((name or "").split()).casefold()


with open(BASE_DIR / "team_roster.json", encoding="utf-8") as f:
    _RAW_TEAM_ROSTER: dict[str, dict] = json.load(f)
# Canonicalize roster keys the same way CRM owner_names are canonicalized at
# ingestion (see _normalize_owner_name, defined below), so the two sides
# always join even if a roster entry is authored with stray whitespace.
TEAM_ROSTER: dict[str, dict] = {" ".join(k.split()): v for k, v in _RAW_TEAM_ROSTER.items()}
# Canonical-key views of the roster for case/whitespace-insensitive lookups.
TEAM_ROSTER_BY_KEY: dict[str, dict] = {_name_key(k): v for k, v in TEAM_ROSTER.items()}
TEAM_ROSTER_KEY_TO_NAME: dict[str, str] = {_name_key(k): k for k in TEAM_ROSTER}

# Maps the viewer's SSO email (from the platform's unspoofable
# X-Forwarded-Email header) to their name in TEAM_ROSTER, so the dashboard
# can scope itself to "my own pipeline" / "my team's pipeline" automatically.
# Kept out of the (public) repo entirely — set as a Substrait env var, a
# JSON object of {"email": "Roster Name"}.
try:
    TEAM_EMAIL_MAP: dict[str, str] = {
        k.strip().lower(): v for k, v in json.loads(os.environ.get("TEAM_EMAIL_MAP") or "{}").items()
    }
except (json.JSONDecodeError, AttributeError):
    TEAM_EMAIL_MAP = {}

CRM_BASE = "https://api.ninjavan.co/global/salescrm/api/v1"
CRM_OPPORTUNITY_URL_BASE = "https://salescrm.ninjavan.co/nv/objects/Opportunity/records"
CRM_TASK_URL_BASE = "https://salescrm.ninjavan.co/nv/objects/Task/records"
RECORD_TYPE_INDONESIA = "12"
# Leads use a different record type for Indonesia than Opportunities do.
RECORD_TYPE_INDONESIA_LEAD = "9"
# Lead statuses still "in play" (an open lead), regardless of when it was
# created — everything else (Disqualified Suspect/Prospect, Converted) is
# terminal. Spelled exactly as the CRM's Lead Status picklist.
LEAD_OPEN_STATUSES = ["Suspect", "Suspect follow up", "Prospect Qualified"]
# Full display order for the Lead Status breakdown (open first, then terminal).
LEAD_STATUS_ORDER = [
    "Suspect", "Suspect follow up", "Prospect Qualified",
    "Disqualified Suspect", "Disqualified Prospect", "Converted",
]
# How far back to pull *terminal* leads for the "created per month" trend — only
# the last few months are shown, so no need to pull a full year of converted/
# disqualified leads on top of every open one. Open leads are pulled at any age.
LEAD_CREATED_HISTORY_DAYS = 100
CLOSED_HISTORY_DAYS = 365
EXCLUDE_NAME_SUBSTR = "UNAUTHORIZED OPPORTUNITY"

OPEN_STAGE_ORDER = [
    "Prospecting", "New", "Proposal Submitted", "Negotiation",
    "EKYC Approval", "Contract Sent", "Agreed to Ship", "Onboarding",
    "Ready to Ship",
]
# "Deals by Stage" hides Prospecting (a legacy stage no longer offered in
# the CRM's own filter dropdown) — but OPEN_STAGE_ORDER above stays
# untouched so a stray Prospecting deal is still counted in Open Pipeline
# and every other stage-grouped panel, just not given its own bar here.
DISPLAY_STAGE_ORDER = [s for s in OPEN_STAGE_ORDER if s != "Prospecting"]
PRODUCT_LINE_ORDER = [
    "Restock", "LTL", "Cold Chain", "Cross-border", "Fulfillment",
    "Last Mile – Parcel", "Complex Logistics", "Last Mile – Cargo",
    "Last Mile – Document", "Digital +", "Cross-border SG",
    "Forward Stocking Locations", "Ninja FieldSight",
]
# service_level and core_product are multi_picklist fields — membership uses
# "contains", not "equals" (equals silently returns 0 rows for this field type).
SERVICE_LEVEL_VALUES = ["Same Day", "Standard", "LTL", "Next Day", "FTL", "Dedicated", "FCL", "LCL"]

# Industry lives on Account, not Opportunity — Opportunity only carries
# account_id. Looked up in chunks via an `id in [...]` filter against
# /objects/Account/records rather than pulling the whole Account table; see
# fetch_account_industries. One request per chunk since page_size matches
# the chunk size, so every match for that chunk fits on page 1.
ACCOUNT_INDUSTRY_CHUNK_SIZE = 100

# Task is a global object (642k+ rows across every country) with no
# record_type_id we can filter Indonesia by, so instead we scope the fetch to
# just the owner_ids in our Indonesia team roster — cheap, and correct since
# a rep only owns their own country's work.
TASK_OWNER_IDS = sorted({v["owner_id"] for v in TEAM_ROSTER.values() if v.get("owner_id")})
TASK_UNMAPPED_REPS = sorted(name for name, v in TEAM_ROSTER.items() if not v.get("owner_id"))
TASK_OPEN_STATUSES = ["Not Started", "In Progress"]

# Opportunity.type values that count as new business for "Created" metrics —
# excludes Pricing/Up-Selling/Regain/Others, which are opportunities raised
# against an existing account rather than new pipeline.
CREATED_OPPORTUNITY_TYPES = {"Acquisition", "Cross-Selling"}

# Opportunity.type values excluded from Activity (Notebook/Task/CRM Updated
# %) and Action Items — Up-Selling/Pricing opportunities on an existing
# account aren't held to the same update cadence as new-business pipeline.
EXCLUDED_ACTIVITY_TYPES = {"Up-Selling", "Pricing"}

# Action Items thresholds — deliberately the same numbers already used
# elsewhere on the dashboard (Stage Bottlenecks' implicit "long" stage stay,
# and the Notebook Activity 14d tier) so this panel doesn't introduce a
# second, inconsistent definition of "stuck".
ACTION_ITEM_STALE_STAGE_DAYS = 30
ACTION_ITEM_STALE_NOTEBOOK_DAYS = 14
ACTION_ITEM_LIST_LIMIT = 8

# Each Active rep is expected to create 10 new opportunities a month; a
# manager's target is just that number times their headcount. "Unmapped"
# isn't a real manager, so it has no target (MANAGER_TARGETS.get returns
# None for it).
INDIVIDUAL_MONTHLY_TARGET = 10
MANAGER_REP_COUNTS = Counter(v.get("manager") for v in TEAM_ROSTER.values() if v.get("manager"))
MANAGER_TARGETS = {name: count * INDIVIDUAL_MONTHLY_TARGET for name, count in MANAGER_REP_COUNTS.items()}
OWNER_TARGETS = {name: INDIVIDUAL_MONTHLY_TARGET for name in TEAM_ROSTER}

# Same idea for Closed-Won, at a lower monthly bar per rep.
CLOSED_WON_MONTHLY_TARGET = 3
CLOSED_WON_MANAGER_TARGETS = {name: count * CLOSED_WON_MONTHLY_TARGET for name, count in MANAGER_REP_COUNTS.items()}
CLOSED_WON_OWNER_TARGETS = {name: CLOSED_WON_MONTHLY_TARGET for name in TEAM_ROSTER}

# PIP Radar: a screening view over four signals (Created, Closed-Won, CRM
# Updated %, Pipeline Runway) to flag reps who may need a Performance
# Improvement Plan conversation. A signal is "red" below 50% of its target;
# 2+ red signals -> Recommend PIP Review, 1 -> Watch, 0 -> On Track.
PIP_RUNWAY_TARGET_PER_MONTH = 30_000_000
PIP_RUNWAY_MONTHS = 2
PIP_RUNWAY_TARGET = PIP_RUNWAY_TARGET_PER_MONTH * PIP_RUNWAY_MONTHS
PIP_RED_THRESHOLD_PCT = 50

_cache: dict[str, Any] = {
    "items": None, "notebook_last_touch": None, "tasks": None, "leads": None,
    "snapshot_at": None,
    "fetched_at": 0.0, "error": None, "refreshing": False, "last_attempt_at": None,
    "consecutive_failures": 0, "last_full_pull_at": None, "account_industry": None,
}
# A full pull is ~730 paginated requests (72k+ Indonesia Opportunities,
# all-time) — far too slow to run inside a request, so it only ever runs on
# this background timer, never on-demand from get_pipeline().
REFRESH_INTERVAL_SECONDS = 900
# If the CRM is rate-limiting us, retrying again in exactly 15 minutes
# regardless — forever — just keeps knocking on a door that isn't open yet.
# Back off further after each consecutive failure (30min, 60min, ...), up to
# this cap, and reset to the normal interval as soon as one attempt succeeds.
REFRESH_BACKOFF_CAP_SECONDS = 3600
# Most 15-minute refreshes fetch only the delta (records touched since the
# last successful refresh) — orders of magnitude cheaper than the full pull,
# and the main reason the CRM's rate limiter used to trip. A full pull still
# runs once a day to reconcile anything a pure delta can't reach (e.g. the
# rare record whose record_type_id itself changed) and whenever there's no
# cache yet to delta against.
FULL_PULL_INTERVAL_SECONDS = 24 * 3600
# Where the cache snapshot is persisted (object storage) so a restart loads
# a warm cache instead of starting from nothing — see _save_snapshot /
# _load_snapshot. Restoring last_full_pull_at from it too means the first
# refresh after a restart is a cheap delta, not a full pull.
_SNAPSHOT_KEY = "cache/pipeline_snapshot.json"

_DASH_RE = re.compile("[-‐‑‒–—―]")


def _normalize_stage(s: str) -> str:
    return _DASH_RE.sub("-", s).strip().lower()


def _normalize_owner_name(name: str | None) -> str | None:
    """Collapse any run of whitespace to a single space and trim, so a CRM
    owner_name carrying a stray double space (e.g. "Deny  Ardianto") still
    matches its single-spaced team_roster key. Applied at ingestion so every
    downstream manager join, filter, dropdown, and grouping sees one
    canonical form — an owner is never split between their real manager and
    the "Unmapped" bucket over invisible whitespace."""
    if not name:
        return name
    return " ".join(name.split())


# Data-quality filter: "Last Mile – Parcel" and "Last Mile – Document" are
# only valid for Raden Roro Inggil Pratiwi's opportunities; excluded for
# everyone else. Compared dash-normalized since the CRM inconsistently
# returns nv_product_line with a hyphen vs. an en dash.
RESTRICTED_PRODUCT_LINES = {_normalize_stage("Last Mile – Parcel"), _normalize_stage("Last Mile – Document")}
# "Cross-border" is excluded entirely, for every owner — not a valid NV
# Product Line for this dashboard.
EXCLUDED_PRODUCT_LINES = {_normalize_stage("Cross-border")}


def _opportunity_included(r: dict) -> bool:
    """Whether a raw Opportunity record belongs on the dashboard at all —
    shared by the full pull and the delta merge so a record that changes in
    a way that newly excludes it (e.g. its product line is edited to
    Cross-border) drops out on the next delta cycle instead of lingering
    forever."""
    if EXCLUDE_NAME_SUBSTR in (r.get("name") or ""):
        return False
    product_line = _normalize_stage(r.get("nv_product_line") or "")
    if product_line in RESTRICTED_PRODUCT_LINES and _name_key(r.get("owner_name")) != _name_key("Raden Roro Inggil Pratiwi"):
        return False
    if product_line in EXCLUDED_PRODUCT_LINES:
        return False
    return True


def _normalize_opportunity(r: dict) -> dict:
    """service_level is a multi-select field, but the CRM returns a bare
    string instead of a 1-item list for some records — spreading/joining
    that string elsewhere would silently iterate its characters instead of
    treating it as one value, so normalize to a list once, here."""
    sl = r.get("service_level")
    if isinstance(sl, str):
        r["service_level"] = [sl] if sl else []
    elif not sl:
        r["service_level"] = []
    r["owner_name"] = _normalize_owner_name(r.get("owner_name"))
    return r


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        # Task.due_date comes back with no offset at all (e.g.
        # "2026-09-07T00:00:00") — treat as UTC like every other timestamp
        # here, otherwise arithmetic against an aware `now` raises.
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
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


_PAGE_CONCURRENCY = 3
_RATE_LIMIT_MAX_RETRIES = 8
_RETRYABLE_STATUS_CODES = {429, 502, 503, 504}


async def _get_with_retry(client: httpx.AsyncClient, url: str, headers: dict, params: dict, timeout: int = 20):
    """GET with retry-with-backoff on 429 (rate limited) and 502/503/504
    (the CRM's own service being down or overloaded) — honoring Retry-After
    when the CRM sends one. Concurrent paginated fetches (Opportunities,
    Notebook entries, Tasks all running at once on every refresh) can trip
    the CRM's rate limiter, especially right after a fresh restart — without
    this, a single bad response aborted the whole background refresh."""
    for attempt in range(_RATE_LIMIT_MAX_RETRIES + 1):
        resp = await client.get(url, headers=headers, params=params, timeout=timeout)
        if resp.status_code not in _RETRYABLE_STATUS_CODES or attempt == _RATE_LIMIT_MAX_RETRIES:
            return resp
        retry_after = resp.headers.get("Retry-After")
        try:
            delay = float(retry_after) if retry_after else min(2 ** attempt, 60)
        except ValueError:
            delay = min(2 ** attempt, 60)
        await asyncio.sleep(delay)
    return resp


async def _fetch_paginated(
    client: httpx.AsyncClient, url: str, headers: dict, params: dict, page_size: int = 100,
    page_concurrency: int | None = None,
) -> list[dict]:
    """Fetch every page of a {items,total,page,page_size,has_next} envelope.
    Page 1 is fetched first to learn the total, then the rest are fetched
    concurrently (bounded) instead of one-at-a-time — this matters once the
    collection is large (e.g. all-time Opportunities, or the ~1500-entry
    Notebook feed). Pass page_concurrency=1 for a gentle, sequential pull that
    is far less likely to trip the CRM's rate limiter (used for Leads, which
    refresh rarely and don't need to be fast)."""
    first = await _get_with_retry(client, url, headers, {**params, "page_size": page_size, "page": 1})
    if first.status_code == 401:
        raise HTTPException(
            status_code=502,
            detail="CRM API key rejected (expired or revoked) — rotate CRM_API_KEY",
        )
    first.raise_for_status()
    data = first.json()
    items: list[dict] = list(data.get("items") or [])
    total = data.get("total")
    if not data.get("has_next") or not total:
        return items

    total_pages = -(-total // page_size)  # ceil division
    sem = asyncio.Semaphore(page_concurrency or _PAGE_CONCURRENCY)

    async def fetch_page(page: int) -> list[dict]:
        async with sem:
            resp = await _get_with_retry(client, url, headers, {**params, "page_size": page_size, "page": page})
            resp.raise_for_status()
            return list(resp.json().get("items") or [])

    for page_items in await asyncio.gather(*(fetch_page(p) for p in range(2, total_pages + 1))):
        items.extend(page_items)
    return items


async def fetch_all_opportunities(client: httpx.AsyncClient) -> list[dict]:
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")

    # Indonesia has 72,721 Opportunity records all-time — fetching (and
    # holding in memory) every one blew both the platform's ~30s gateway
    # timeout and its 512MB memory limit, even from the background refresh.
    # Fetch instead: every OPEN deal regardless of age (so no old stale lead
    # is ever missed) OR anything created within the last year (so recent
    # closed-won/lost history, win rate, etc. still have real data).
    closed_since = (datetime.now(timezone.utc) - timedelta(days=CLOSED_HISTORY_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    filters = {
        "logic": "AND",
        "conditions": [
            {"field": "record_type_id", "operator": "equals", "value": RECORD_TYPE_INDONESIA},
            {
                "logic": "OR",
                "conditions": [
                    {"field": "stage", "operator": "not_in", "value": ["Closed-Won", "Closed-Lost"]},
                    {"field": "created_at", "operator": "greater_than", "value": closed_since},
                ],
            },
        ],
    }
    items = await _fetch_paginated(
        client,
        f"{CRM_BASE}/objects/Opportunity/records",
        {"X-API-Key": api_key},
        {"filters": json.dumps(filters)},
    )
    return [_normalize_opportunity(r) for r in items if _opportunity_included(r)]


async def fetch_opportunities_delta(client: httpx.AsyncClient, since: str) -> list[dict]:
    """Only Indonesia Opportunities touched since the last successful
    refresh, for merging into the cached set with _merge_opportunities.
    Re-running the full ~72k-record filtered pull on every 15-minute refresh
    was most of what tripped the CRM's rate limiter — this is orders of
    magnitude cheaper and is what every refresh uses once a full pull has
    populated the cache (see FULL_PULL_INTERVAL_SECONDS for when a full pull
    still runs)."""
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")

    filters = {
        "logic": "AND",
        "conditions": [
            {"field": "record_type_id", "operator": "equals", "value": RECORD_TYPE_INDONESIA},
            {"field": "updated_at", "operator": "greater_than", "value": since},
        ],
    }
    return await _fetch_paginated(
        client,
        f"{CRM_BASE}/objects/Opportunity/records",
        {"X-API-Key": api_key},
        {"filters": json.dumps(filters)},
    )


def _merge_opportunities(existing: list[dict], delta_raw: list[dict]) -> list[dict]:
    """Upsert a delta batch (raw, unfiltered) into the cached item list by
    id. A delta record that no longer passes _opportunity_included (e.g. its
    product line was edited to an excluded one) is dropped rather than
    upserted, so it still leaves the set instead of lingering as a stale
    copy. Then prune anything that has aged out of the open-or-recent window
    purely from elapsed time — a Closed-Won/Lost record whose created_at
    fell more than CLOSED_HISTORY_DAYS behind "now" leaves the set here even
    though nothing about the record itself changed, so it never needs to
    show up in a delta at all."""
    by_id = {r["id"]: r for r in existing}
    for r in delta_raw:
        rid = r.get("id")
        if rid is None:
            continue
        by_id.pop(rid, None)
        if _opportunity_included(r):
            by_id[rid] = _normalize_opportunity(r)

    cutoff = datetime.now(timezone.utc) - timedelta(days=CLOSED_HISTORY_DAYS)

    def _in_window(r: dict) -> bool:
        if r.get("stage") not in ("Closed-Won", "Closed-Lost"):
            return True
        created = _parse_dt(r.get("created_at"))
        return created is not None and created > cutoff

    return [r for r in by_id.values() if _in_window(r)]


async def fetch_account_industries(client: httpx.AsyncClient, account_ids: list[int]) -> dict[int, str | None]:
    """Industry for exactly the given account_ids, chunked through an `id in
    [...]` filter against /objects/Account/records. Called only with
    account_ids not already known (see _refresh_dashboard_cache) — on a
    steady-state delta refresh that's typically zero or a handful, not the
    whole Account table, which is what keeps this from being its own source
    of CRM rate-limit pressure."""
    if not account_ids:
        return {}
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")

    result: dict[int, str | None] = {}
    for i in range(0, len(account_ids), ACCOUNT_INDUSTRY_CHUNK_SIZE):
        chunk = account_ids[i : i + ACCOUNT_INDUSTRY_CHUNK_SIZE]
        filters = {"logic": "AND", "conditions": [{"field": "id", "operator": "in", "value": chunk}]}
        records = await _fetch_paginated(
            client,
            f"{CRM_BASE}/objects/Account/records",
            {"X-API-Key": api_key},
            {"filters": json.dumps(filters)},
            page_size=ACCOUNT_INDUSTRY_CHUNK_SIZE,
        )
        for acc in records:
            aid = acc.get("id")
            if aid is not None:
                result[aid] = acc.get("industry")
    return result


async def fetch_notebook_last_touch(client: httpx.AsyncClient) -> dict[int, dict]:
    """Most recent Notebook entry per Opportunity record_id, across all countries
    (the /notebook/entries endpoint has no record_type/country filter — we only
    ever look up ids that belong to our Indonesia open-opportunity set).
    summary_preview (the AI-generated note summary) is only populated by the
    CRM for the entry's own creator or an admin — for any other viewer it
    comes back null, so the Opportunity List's Notebook Content column can be
    blank for entries our API key's user didn't write and isn't admin on."""
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")

    entries = await _fetch_paginated(
        client,
        f"{CRM_BASE}/notebook/entries",
        {"X-API-Key": api_key},
        {"object_type": "Opportunity"},
    )
    last_touch: dict[int, dict] = {}
    for entry in entries:
        record_id = entry.get("record_id")
        created = _parse_dt(entry.get("created_at"))
        if record_id is None or not created:
            continue
        existing = last_touch.get(record_id)
        if existing is None or created > existing["last_touch"]:
            last_touch[record_id] = {"last_touch": created, "content": entry.get("summary_preview")}
    return last_touch


async def fetch_open_tasks(client: httpx.AsyncClient) -> list[dict]:
    """Not Started / In Progress Tasks owned by our Indonesia reps, linked to
    an Opportunity record (Tasks against a Lead/Account/Contact aren't
    joinable to the Opportunity List, so they're excluded at the source)."""
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")
    if not TASK_OWNER_IDS:
        return []

    filters = {
        "logic": "AND",
        "conditions": [
            {"field": "owner_id", "operator": "in", "value": TASK_OWNER_IDS},
            {"field": "status", "operator": "in", "value": TASK_OPEN_STATUSES},
        ],
    }
    tasks = await _fetch_paginated(
        client,
        f"{CRM_BASE}/objects/Task/records",
        {"X-API-Key": api_key},
        {"filters": json.dumps(filters)},
    )
    # Filtered client-side rather than as a server-side condition above —
    # related_object_type's filter support on this endpoint isn't confirmed.
    opp_tasks = [t for t in tasks if t.get("related_object_type") == "Opportunity"]
    for t in opp_tasks:
        t["owner_name"] = _normalize_owner_name(t.get("owner_name"))
    return opp_tasks


async def fetch_leads(client: httpx.AsyncClient) -> list[dict]:
    """Indonesia Leads for the Leads tab. Fetches every currently-open lead
    (Suspect / Suspect follow up / Prospect Qualified) regardless of age, plus
    anything created within the last year — so "open leads" is complete and
    "created per month" has recent history, mirroring the Opportunity fetch.
    Projected down to just the fields the tab needs, to keep the cache small."""
    api_key = os.environ.get("CRM_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="CRM_API_KEY is not configured")
    created_since = (datetime.now(timezone.utc) - timedelta(days=LEAD_CREATED_HISTORY_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    filters = {
        "logic": "AND",
        "conditions": [
            {"field": "record_type_id", "operator": "equals", "value": RECORD_TYPE_INDONESIA_LEAD},
            {
                "logic": "OR",
                "conditions": [
                    {"field": "status", "operator": "in", "value": LEAD_OPEN_STATUSES},
                    {"field": "created_at", "operator": "greater_than", "value": created_since},
                ],
            },
        ],
    }
    leads = await _fetch_paginated(
        client,
        f"{CRM_BASE}/objects/Lead/records",
        {"X-API-Key": api_key},
        {"filters": json.dumps(filters)},
        page_concurrency=1,  # gentle sequential pull — leads refresh rarely
    )
    return [
        {
            "id": lead.get("id"),
            "owner_name": _normalize_owner_name(lead.get("owner_name")),
            "status": lead.get("status"),
            "created_at": lead.get("created_at"),
        }
        for lead in leads
    ]


UNMAPPED_MANAGER_LABEL = "Unmapped"


def _owner_manager(owner: str) -> str:
    """The manager filter value for an owner — their real manager, or the
    synthetic "Unmapped" bucket for an owner with no team_roster entry (or
    a roster entry with no manager field, e.g. a Sales Head). Matches the owner
    name case- and whitespace-insensitively (see _name_key), so a CRM spelling
    that differs from the roster's casing still resolves the right manager
    instead of falling into "Unmapped"."""
    return (TEAM_ROSTER_BY_KEY.get(_name_key(owner)) or {}).get("manager") or UNMAPPED_MANAGER_LABEL


def _filter_options(
    items: list[dict],
    allowed_owners: set[str] | None = None,
    selected_owners: list[str] | None = None,
    selected_managers: list[str] | None = None,
) -> dict:
    """Dropdown choices for the top filter bar — computed from the full
    population (not the request's own filter selections) so picking one
    filter doesn't shrink the other dropdowns' own options, with one
    deliberate exception: Salesperson and Manager narrow each other
    (selecting a Manager narrows which Salespeople are offered, and
    selecting Salespeople narrows which Managers are offered), since that's
    the one pairing users expect to be dependent. When the viewer is
    identity-scoped (allowed_owners), the lists are narrowed to their
    permitted names/managers so the UI never offers a name they can't
    actually select."""
    owners_pool = {r.get("owner_name") for r in items if r.get("owner_name")}
    managers_pool = {v["manager"] for v in TEAM_ROSTER.values() if v.get("manager")}
    has_unmapped = any(_owner_manager(o) == UNMAPPED_MANAGER_LABEL for o in owners_pool)
    if allowed_owners is not None:
        allowed_canon = {_name_key(a) for a in allowed_owners}
        owners_pool = {o for o in owners_pool if _name_key(o) in allowed_canon}
        managers_pool = {
            (TEAM_ROSTER_BY_KEY.get(_name_key(name)) or {}).get("manager")
            for name in allowed_owners
        }
        managers_pool.discard(None)
        has_unmapped = any(_owner_manager(o) == UNMAPPED_MANAGER_LABEL for o in owners_pool)
    if has_unmapped:
        managers_pool.add(UNMAPPED_MANAGER_LABEL)

    result_owners = owners_pool
    if selected_managers:
        managers_set = {_name_key(m) for m in selected_managers}
        result_owners = {o for o in owners_pool if _name_key(_owner_manager(o)) in managers_set}

    result_managers = managers_pool
    if selected_owners:
        owners_set = {_name_key(o) for o in selected_owners}
        result_managers = {_owner_manager(o) for o in owners_pool if _name_key(o) in owners_set}

    return {
        "owners": sorted(result_owners),
        "managers": sorted(result_managers),
        "product_lines": list(PRODUCT_LINE_ORDER),
        "service_levels": list(SERVICE_LEVEL_VALUES),
        # Industry has no fixed display order like the two above (81+
        # picklist values on the CRM side) — just every value actually
        # present on a matched Opportunity's Account, alphabetical.
        "industries": sorted({r.get("industry") for r in items if r.get("industry")}),
    }


def _apply_filters(
    items: list[dict],
    owners: list[str] | None,
    managers: list[str] | None,
    product_lines: list[str] | None,
    service_levels: list[str] | None,
    industries: list[str] | None = None,
) -> list[dict]:
    owners_set = {_name_key(o) for o in owners} if owners else None
    managers_set = {_name_key(m) for m in managers} if managers else None
    product_lines_set = set(product_lines) if product_lines else None
    service_levels_set = set(service_levels) if service_levels else None
    industries_set = set(industries) if industries else None
    if not any([owners_set, managers_set, product_lines_set, service_levels_set, industries_set]):
        return items

    def keep(r: dict) -> bool:
        owner = r.get("owner_name") or ""
        if owners_set is not None and _name_key(owner) not in owners_set:
            return False
        if managers_set is not None and _name_key(_owner_manager(owner)) not in managers_set:
            return False
        if product_lines_set is not None and r.get("nv_product_line") not in product_lines_set:
            return False
        if service_levels_set is not None and not (set(r.get("service_level") or []) & service_levels_set):
            return False
        if industries_set is not None and r.get("industry") not in industries_set:
            return False
        return True

    return [r for r in items if keep(r)]


def _apply_task_filters(
    tasks: list[dict], owners: list[str] | None, managers: list[str] | None
) -> list[dict]:
    # Tasks aren't tagged with product line / service level, so those two
    # filters don't apply to the Task population — only owner and manager do.
    owners_set = {_name_key(o) for o in owners} if owners else None
    managers_set = {_name_key(m) for m in managers} if managers else None
    if owners_set is None and managers_set is None:
        return tasks

    def keep(t: dict) -> bool:
        owner = t.get("owner_name") or ""
        if owners_set is not None and _name_key(owner) not in owners_set:
            return False
        if managers_set is not None and _name_key(_owner_manager(owner)) not in managers_set:
            return False
        return True

    return [t for t in tasks if keep(t)]


def _apply_lead_filters(
    leads: list[dict], owners: list[str] | None, managers: list[str] | None
) -> list[dict]:
    # Leads are scoped by owner and manager only (same as Tasks) — the Product
    # Line / Service Level filters are about the Opportunity pipeline, not Leads.
    # Case-/whitespace-insensitive, like every other owner/manager match.
    owners_set = {_name_key(o) for o in owners} if owners else None
    managers_set = {_name_key(m) for m in managers} if managers else None
    if owners_set is None and managers_set is None:
        return leads

    def keep(lead: dict) -> bool:
        owner = lead.get("owner_name") or ""
        if owners_set is not None and _name_key(owner) not in owners_set:
            return False
        if managers_set is not None and _name_key(_owner_manager(owner)) not in managers_set:
            return False
        return True

    return [lead for lead in leads if keep(lead)]


def _viewer_scope_names(viewer_name: str) -> set[str]:
    """Everyone the viewer is allowed to see: always themself, plus every rep
    reporting to them as Manager, plus every rep under them as Sales Head — the
    sales_head field is already flat across the whole chain, so this covers a
    Sales Head's full downstream team in one pass, not just their direct
    reports. All name matching is case- and whitespace-insensitive (see
    _name_key). The viewer is ALWAYS in their own scope, so a mapped viewer's
    scope is never empty — a casing mismatch can no longer collapse it to empty
    and fall open to "see everything" (a plain rep just sees their own rows)."""
    vkey = _name_key(viewer_name)
    scope = {viewer_name}
    for name, v in TEAM_ROSTER.items():
        if _name_key(name) == vkey:  # viewer is a roster rep — use its spelling
            scope.add(name)
        if _name_key(v.get("manager")) == vkey or _name_key(v.get("sales_head")) == vkey:
            scope.add(name)
    return scope


def _resolve_viewer(request: Request) -> tuple[str | None, set[str] | None]:
    """(viewer_name, allowed_owner_names) for this request's authenticated
    viewer, or (None, None) if unrestricted (unmapped viewer, or SSO not
    enabled — X-Forwarded-Email is unspoofable when the platform's Google
    SSO gate is on, and simply absent otherwise, which this treats as "no
    restriction" rather than an error). A viewer matched in the email map is
    always scoped (at least to their own rows), never unrestricted."""
    email = (request.headers.get("x-forwarded-email") or "").strip().lower()
    if not email:
        return None, None
    viewer_name = TEAM_EMAIL_MAP.get(email)
    if not viewer_name:
        return None, None
    return viewer_name, _viewer_scope_names(viewer_name)


def _effective_owners(
    request: Request, owners: list[str] | None
) -> tuple[str | None, set[str] | None, list[str] | None]:
    """(viewer_name, allowed_owners, effective_owners) — effective_owners is
    the owners filter actually applied: the client's own selection clamped
    to the viewer's identity scope, or the full scope if they selected
    nothing, or just the client's own selection if the viewer is
    unrestricted. Clamping is case-/whitespace-insensitive."""
    viewer_name, allowed_owners = _resolve_viewer(request)
    effective_owners = owners
    if allowed_owners is not None:
        if owners:
            allowed_canon = {_name_key(a) for a in allowed_owners}
            effective_owners = [o for o in owners if _name_key(o) in allowed_canon]
        else:
            effective_owners = list(allowed_owners)
    return viewer_name, allowed_owners, effective_owners


def _scoped_roster_names(owners: list[str] | None, managers: list[str] | None) -> set[str]:
    """Which team_roster names the By Salesperson table pre-seeds with
    zero-count rows — narrowed to match the active Salesperson/Manager
    filters (same AND semantics as _apply_filters, case-/whitespace-
    insensitive) so e.g. filtering to one manager only lists that manager's
    own reps, not the whole company."""
    scope = set(TEAM_ROSTER)
    if owners:
        owners_canon = {_name_key(o) for o in owners}
        scope = {name for name in scope if _name_key(name) in owners_canon}
    if managers:
        managers_canon = {_name_key(m) for m in managers}
        scope &= {name for name in TEAM_ROSTER if _name_key(_owner_manager(name)) in managers_canon}
    return scope


def _task_is_active(t: dict, now: datetime, today) -> bool:
    """Same rule the Activity panel's Task side and Action Items' Tasks
    Awaiting Action use: touched within 7 days AND not overdue."""
    last_activity = _parse_dt(t.get("updated_at")) or _parse_dt(t.get("created_at"))
    due = _parse_dt(t.get("due_date"))
    recent = bool(last_activity) and (now - last_activity).days <= 7
    not_overdue = due is None or due.date() >= today
    return recent and not_overdue


def _build_opportunity_rows(
    items: list[dict], notebook_last_touch: dict[int, dict], tasks: list[dict] | None = None
) -> list[dict]:
    """One row per matched Opportunity for the Opportunity List tab — a flat
    detail view alongside the aggregated numbers build_dashboard() produces."""
    now = datetime.now(timezone.utc)
    today = now.date()
    won_norm = _normalize_stage("Closed–Won")
    lost_norm = _normalize_stage("Closed–Lost")
    future_norm = _normalize_stage("Future Opportunity")
    closed_stage_names = {won_norm, lost_norm}

    # Tasks are already Opportunity-only (fetch_open_tasks filters at the
    # source), so related_record_id maps 1:1 to an Opportunity id.
    tasks_by_opp: dict[int, list[dict]] = {}
    for t in tasks or []:
        opp_id = t.get("related_record_id")
        if opp_id is not None:
            tasks_by_opp.setdefault(opp_id, []).append(t)

    rows = []
    for r in items:
        rid = r.get("id")
        stage = r.get("stage") or ""
        stage_norm = _normalize_stage(stage)
        is_open = stage_norm not in closed_stage_names
        if stage_norm == won_norm:
            stage_group = "closed_won"
        elif stage_norm == lost_norm:
            stage_group = "closed_lost"
        elif stage_norm == future_norm:
            stage_group = "future"
        else:
            stage_group = "open"
        owner = r.get("owner_name") or ""
        manager = _owner_manager(owner) if owner else None
        created = _parse_dt(r.get("created_at"))
        changed = _parse_dt(r.get("stage_last_changed_at")) or _parse_dt(r.get("updated_at"))
        close_date = _parse_dt(r.get("expected_close_date"))
        nb_entry = notebook_last_touch.get(rid)
        nb_touch = nb_entry["last_touch"] if nb_entry else None
        nb_effective = nb_touch or created
        nb_days = (now - nb_effective).days if nb_effective else None
        nb_label = _activity_bucket_for(nb_days)[0] if nb_days is not None else None
        opp_tasks = tasks_by_opp.get(rid, [])
        rows.append({
            "id": rid,
            "name": r.get("name"),
            "crm_url": f"{CRM_OPPORTUNITY_URL_BASE}/{rid}" if rid is not None else None,
            "owner_name": owner or None,
            "manager": manager,
            "stage": r.get("stage"),
            "stage_group": stage_group,
            "type": r.get("type"),
            "nv_product_line": r.get("nv_product_line"),
            "industry": r.get("industry"),
            "service_level": r.get("service_level") or [],
            "created_at": r.get("created_at"),
            "aging_days": (now - created).days if created else None,
            "stage_last_changed_at": r.get("stage_last_changed_at") or r.get("updated_at"),
            "last_stage_duration_days": (now - changed).days if changed else None,
            "expected_close_date": r.get("expected_close_date"),
            "overdue": bool(is_open and close_date and close_date.date() < today),
            "total_potential_revenue_mth": float(r.get("total_potential_revenue_mth") or 0),
            "committed_revenue_mth": float(r.get("committed_revenue_mth") or 0),
            "revenue_blank": r.get("total_potential_revenue_mth") in (None, "") or r.get("committed_revenue_mth") in (None, ""),
            "notebook_last_touch": nb_touch.strftime("%Y-%m-%dT%H:%M:%SZ") if nb_touch else None,
            "notebook_days_since_touch": nb_days,
            "notebook_freshness": nb_label,
            "notebook_content": nb_entry.get("content") if nb_entry else None,
            "open_task_count": len(opp_tasks),
            "task_not_updated_count": sum(1 for t in opp_tasks if not _task_is_active(t, now, today)),
        })
    return rows


def _build_task_rows(tasks: list[dict], opp_by_id: dict[int, dict]) -> list[dict]:
    """Every open Task joined to the Opportunity row it belongs to, for the
    Opportunity List's Task List section — tasks whose Opportunity didn't
    match the current filters are dropped here, not re-filtered on their
    own fields."""
    now = datetime.now(timezone.utc)
    today = now.date()
    rows = []
    for t in tasks:
        opp = opp_by_id.get(t.get("related_record_id"))
        if not opp:
            continue
        last_activity = _parse_dt(t.get("updated_at")) or _parse_dt(t.get("created_at"))
        rows.append({
            "id": t.get("id"),
            "subject": t.get("subject") or "(no subject)",
            "status": t.get("status"),
            "owner_name": t.get("owner_name"),
            "due_date": t.get("due_date"),
            "days_since_update": (now - last_activity).days if last_activity else None,
            "active": _task_is_active(t, now, today),
            "opportunity_id": opp["id"],
            "opportunity_name": opp["name"],
            "opportunity_crm_url": opp["crm_url"],
            "opportunity_stage": opp["stage"],
            "opportunity_owner_name": opp["owner_name"],
        })
    rows.sort(key=lambda x: (x["active"], -(x["days_since_update"] or 0)))
    return rows


def _action_item_row(r: dict, day_field: str) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "crm_url": r["crm_url"],
        "owner_name": r["owner_name"],
        "days": r[day_field],
    }


def _build_action_items(opp_rows: list[dict], tasks: list[dict], roster_scope: set[str]) -> dict:
    """Personalized "what needs attention" for the Action Items panel — built
    from the same per-opportunity rows the Opportunity List tab uses, plus
    Task activity, so every threshold here matches what those other panels
    already show (no second definition of "stuck" or "stale")."""
    now = datetime.now(timezone.utc)
    today = now.date()

    # Future Opportunity is deliberately parked, not actively worked — an
    # old Future Opportunity deal isn't "stuck" the way an active one is.
    # Up-Selling/Pricing opportunities on an existing account aren't held to
    # the same update cadence as new-business pipeline, so they're excluded
    # too (same scope as Activity's Notebook/CRM Updated % computation).
    active_rows = [
        r for r in opp_rows
        if r["stage_group"] == "open" and r["type"] not in EXCLUDED_ACTIVITY_TYPES
    ]

    overdue = sorted((r for r in active_rows if r["overdue"]), key=lambda r: -(r["last_stage_duration_days"] or 0))
    stalled = sorted(
        (r for r in active_rows if (r["last_stage_duration_days"] or 0) >= ACTION_ITEM_STALE_STAGE_DAYS),
        key=lambda r: -(r["last_stage_duration_days"] or 0),
    )
    notebook_stale = sorted(
        (r for r in active_rows if (r["notebook_days_since_touch"] or 0) >= ACTION_ITEM_STALE_NOTEBOOK_DAYS),
        key=lambda r: -(r["notebook_days_since_touch"] or 0),
    )
    missing_revenue = sorted(
        (r for r in active_rows if r["revenue_blank"]),
        key=lambda r: -(r["aging_days"] or 0),
    )

    # Tasks pending: open Tasks (already Not Started / In Progress only, per
    # fetch_open_tasks) that aren't "Active" by the same rule Task Activity
    # uses — touched within 7 days AND not overdue.
    # The task links straight to its own CRM record; the linked Opportunity's
    # id and name ride along as a small caption. Name comes from the fetched
    # opportunity set — absent when the Opportunity is deleted or out of the
    # dashboard's window (id is still shown so the row stays traceable).
    opp_name_by_id = {r["id"]: r.get("name") for r in opp_rows}
    pending_tasks = []
    for t in tasks:
        if _task_is_active(t, now, today):
            continue
        last_activity = _parse_dt(t.get("updated_at")) or _parse_dt(t.get("created_at"))
        task_id = t.get("id")
        related_id = t.get("related_record_id") if t.get("related_object_type") == "Opportunity" else None
        pending_tasks.append({
            "id": task_id,
            "subject": t.get("subject") or "(no subject)",
            "owner_name": t.get("owner_name"),
            "days_since_update": (now - last_activity).days if last_activity else None,
            "crm_url": f"{CRM_TASK_URL_BASE}/{task_id}" if task_id is not None else None,
            "opportunity_id": related_id,
            "opportunity_name": opp_name_by_id.get(related_id) if related_id is not None else None,
        })
    pending_tasks.sort(key=lambda t: -(t["days_since_update"] or 0))

    # Monthly pace: every rep in scope, even at 0 — same "don't hide the
    # people doing nothing" approach as the Created Opportunities table.
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    created_counts: Counter = Counter()
    for r in opp_rows:
        created = _parse_dt(r.get("created_at"))
        if created and created >= this_month_start and r.get("type") in CREATED_OPPORTUNITY_TYPES:
            created_counts[r.get("owner_name")] += 1
    monthly_pace = sorted(
        (
            {"name": name, "created": created_counts.get(name, 0), "target": INDIVIDUAL_MONTHLY_TARGET}
            for name in roster_scope
        ),
        key=lambda p: p["created"] - p["target"],
    )

    def _category(rows: list[dict], day_field: str | None) -> dict:
        # Rows arrive pre-sorted worst-first; day_field turns raw opportunity
        # rows into the compact item shape, tasks are already in it.
        final_items = [_action_item_row(r, day_field) for r in rows] if day_field else rows

        # Grouped by owner using the same final item dicts (so a manager's
        # per-rep breakdown can drill into the actual deals/tasks, hyperlinks
        # included, instead of just a bare count) — sort order within each
        # owner's group is inherited from the overall worst-first sort.
        grouped: dict[str, list[dict]] = {}
        for it in final_items:
            owner = it.get("owner_name")
            if owner:
                grouped.setdefault(owner, []).append(it)
        by_owner = sorted(
            ({"name": name, "count": len(its), "items": its} for name, its in grouped.items()),
            key=lambda x: -x["count"],
        )

        return {"items": final_items[:ACTION_ITEM_LIST_LIMIT], "total": len(rows), "by_owner": by_owner}

    return {
        "overdue": _category(overdue, "last_stage_duration_days"),
        "stalled": _category(stalled, "last_stage_duration_days"),
        "notebook_stale": _category(notebook_stale, "notebook_days_since_touch"),
        "missing_revenue": _category(missing_revenue, "aging_days"),
        "tasks_pending": _category(pending_tasks, None),
        "monthly_pace": monthly_pace,
        # Tells the frontend whether to show per-deal examples (one person in
        # scope) or a per-rep breakdown (a manager/admin looking at a team) —
        # the two lists above only differ in that framing, not the numbers.
        "scope_size": len(roster_scope),
    }


def _build_pip_radar(
    items: list[dict],
    created_by_month: dict,
    crm_updated_by_owner: list[dict],
    roster_scope: set[str],
    now: datetime,
) -> dict:
    """Draft PIP screening view: Created, Closed-Won, and CRM Updated % are
    the exact same numbers already computed for Overview/Created
    Opportunities — this just re-reads them per rep. Pipeline Runway
    (committed revenue on open, non-Future-Opportunity deals expected to
    close this month or next) is the one new signal, computed here."""
    future_norm = _normalize_stage("Future Opportunity")
    closed_stage_names = {_normalize_stage("Closed–Won"), _normalize_stage("Closed–Lost")}
    active_items = [
        r for r in items
        if _normalize_stage(r.get("stage") or "") not in closed_stage_names | {future_norm}
    ]

    this_month_key = now.strftime("%Y-%m")
    next_month_key = (now.replace(day=1) + timedelta(days=32)).replace(day=1).strftime("%Y-%m")
    runway_by_owner: dict[str, float] = {name: 0.0 for name in roster_scope}
    for r in active_items:
        close_date = _parse_dt(r.get("expected_close_date"))
        if not close_date or close_date.strftime("%Y-%m") not in (this_month_key, next_month_key):
            continue
        owner = r.get("owner_name") or ""
        if owner in runway_by_owner:
            runway_by_owner[owner] += float(r.get("committed_revenue_mth") or 0)

    created_by_owner = {row["name"]: row for row in created_by_month["by_owner"]}
    crm_by_owner = {row["name"]: row for row in crm_updated_by_owner}
    current_month_key = created_by_month["months"][-1]

    # Created and Closed-Won are cumulative-through-the-month counts, so
    # checking them against the full month's target is unfairly harsh in
    # the first couple weeks (day 3 of 30 would need 33% of the month's
    # target already banked just to look "on track"). Instead, compare
    # against how much of the target should be banked by *today*, given
    # how far through the month we are — a pace check, not an end-of-month
    # check. CRM Updated % and Pipeline Runway are point-in-time snapshots,
    # not cumulative counts, so they aren't prorated.
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    month_progress_pct = round(now.day / days_in_month * 100, 1)

    def _pct(value: float, target: float) -> float:
        return min(value / target * 100, 999) if target else 0.0

    def _pace_pct(value: float, target: float) -> float:
        if not target:
            return 0.0
        expected_so_far = target * now.day / days_in_month
        return min(value / expected_so_far * 100, 999) if expected_so_far else 100.0

    rows = []
    for name in roster_scope:
        created_row = created_by_owner.get(name)
        created_count = created_row["counts"].get(current_month_key, 0) if created_row else 0
        created_target = created_row["target"] if created_row else None
        won_count = created_row["won_counts"].get(current_month_key, 0) if created_row else 0
        won_target = created_row["won_target"] if created_row else None
        crm_row = crm_by_owner.get(name)
        crm_pct = crm_row["pct"] if crm_row else 0.0
        runway = runway_by_owner.get(name, 0.0)

        created_pace_pct = _pace_pct(created_count, created_target)
        won_pace_pct = _pace_pct(won_count, won_target)
        signal_pcts = [
            created_pace_pct,
            won_pace_pct,
            crm_pct,
            _pct(runway, PIP_RUNWAY_TARGET),
        ]
        red_count = sum(1 for p in signal_pcts if p < PIP_RED_THRESHOLD_PCT)
        flag = "review" if red_count >= 2 else "watch" if red_count == 1 else "ontrack"

        rows.append({
            "name": name,
            "manager": _owner_manager(name),
            "created": created_count,
            "created_target": created_target,
            "created_pace_pct": round(created_pace_pct, 1),
            "won": won_count,
            "won_target": won_target,
            "won_pace_pct": round(won_pace_pct, 1),
            "crm_updated_pct": crm_pct,
            "runway": runway,
            "runway_target": PIP_RUNWAY_TARGET,
            "red_count": red_count,
            "flag": flag,
        })

    rows.sort(key=lambda r: (-r["red_count"], r["name"]))
    return {
        "rows": rows,
        "review_count": sum(1 for r in rows if r["flag"] == "review"),
        "watch_count": sum(1 for r in rows if r["flag"] == "watch"),
        "runway_target_per_month": PIP_RUNWAY_TARGET_PER_MONTH,
        "runway_months": PIP_RUNWAY_MONTHS,
        "month_progress_pct": month_progress_pct,
    }


def build_dashboard(
    items: list[dict],
    notebook_last_touch: dict[int, dict],
    tasks: list[dict],
    roster_scope: set[str] | None = None,
    include_action_items: bool = False,
) -> dict:
    now = datetime.now(timezone.utc)
    total = len(items)
    # Which roster names the By Salesperson table pre-seeds with zero-count
    # rows — narrowed by the caller to match the active Salesperson/Manager
    # filters, so filtering to one manager doesn't still list the entire
    # company's reps at 0/10.
    if roster_scope is None:
        roster_scope = set(TEAM_ROSTER)

    # Opportunities created this calendar month vs last, for the headline
    # stat — "Created" only counts new-business Types (Acquisition,
    # Cross-Selling), not Pricing/Up-Selling/Regain/Others on existing deals.
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = this_month_start - timedelta(seconds=1)
    last_month_start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    new_business_items = [r for r in items if r.get("type") in CREATED_OPPORTUNITY_TYPES]
    created_this_month = 0
    created_last_month = 0
    created_this_month_ids: set = set()
    for r in new_business_items:
        created = _parse_dt(r.get("created_at"))
        if not created:
            continue
        if created >= this_month_start:
            created_this_month += 1
            created_this_month_ids.add(r.get("id"))
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

    # Closed-Won/Lost "this month" — based on when the deal last changed
    # stage (i.e. when it actually closed), not when it was created.
    won_norm = _normalize_stage("Closed–Won")
    lost_norm = _normalize_stage("Closed–Lost")
    closed_won_this_month = 0
    closed_lost_this_month = 0
    closed_won_last_month = 0
    closed_lost_last_month = 0
    closed_won_this_month_ids: set = set()
    closed_lost_this_month_ids: set = set()
    for r in items:
        stage_norm = _normalize_stage(r.get("stage") or "")
        if stage_norm not in (won_norm, lost_norm):
            continue
        changed = _parse_dt(r.get("stage_last_changed_at")) or _parse_dt(r.get("updated_at"))
        if not changed:
            continue
        if changed >= this_month_start:
            if stage_norm == won_norm:
                closed_won_this_month += 1
                closed_won_this_month_ids.add(r.get("id"))
            else:
                closed_lost_this_month += 1
                closed_lost_this_month_ids.add(r.get("id"))
        elif last_month_start <= changed <= last_month_end:
            if stage_norm == won_norm:
                closed_won_last_month += 1
            else:
                closed_lost_last_month += 1
    # Lead time to Closed-Won: days from creation to the stage change that
    # closed the deal. The CRM has no separate "entered New stage" timestamp,
    # so created_at is used as a proxy for it (opportunities are created
    # directly into New) — the same approximation Deal Aging already relies
    # on for "days since creation".
    lead_time_days: list[int] = []
    for r in items:
        if _normalize_stage(r.get("stage") or "") != won_norm:
            continue
        created = _parse_dt(r.get("created_at"))
        closed_at = _parse_dt(r.get("stage_last_changed_at")) or _parse_dt(r.get("updated_at"))
        if not created or not closed_at or closed_at < created:
            continue
        lead_time_days.append((closed_at - created).days)
    lead_time_to_won_avg_days = (
        round(sum(lead_time_days) / len(lead_time_days), 1) if lead_time_days else None
    )
    lead_time_to_won_count = len(lead_time_days)

    open_stages = [
        {"name": s, "count": stage_counts.get(_normalize_stage(s), 0)} for s in DISPLAY_STAGE_ORDER
    ]
    open_active = sum(stage_counts.get(_normalize_stage(s), 0) for s in OPEN_STAGE_ORDER)
    closed_total = won + lost
    closed_stage_names = {_normalize_stage("Closed–Won"), _normalize_stage("Closed–Lost")}
    open_items = [r for r in items if _normalize_stage(r.get("stage") or "") not in closed_stage_names]
    # Pipeline Conversion denominator: the *unique* deals in play this month
    # — created, still open, or resolved either way — not a sum of the four
    # counts (which would double-count e.g. a deal created and closed within
    # the same month).
    pipeline_conversion_ids = (
        created_this_month_ids
        | {r.get("id") for r in open_items}
        | closed_won_this_month_ids
        | closed_lost_this_month_ids
    )

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
    # Pre-seeded with every rep in scope so someone who created nothing this
    # window still shows a row (0/10 is exactly the signal a target is for).
    owner_created_totals: dict[str, dict[str, int]] = {name: {} for name in roster_scope}
    unmapped_owners = set()
    for r in new_business_items:
        created = _parse_dt(r.get("created_at"))
        if not created:
            continue
        month_key = created.strftime("%Y-%m")
        owner = r.get("owner_name") or ""
        roster_name = TEAM_ROSTER_KEY_TO_NAME.get(_name_key(owner))
        manager = _owner_manager(owner)
        if not roster_name:
            unmapped_owners.add(owner)
        manager_totals.setdefault(manager, {}).setdefault(month_key, 0)
        manager_totals[manager][month_key] += 1
        if roster_name and roster_name in owner_created_totals:
            owner_created_totals[roster_name][month_key] = owner_created_totals[roster_name].get(month_key, 0) + 1

    # Closed-Won by the same 3-month window, keyed by when the deal actually
    # closed (stage_last_changed_at, falling back to updated_at) — not Type
    # filtered, matching the "Closed-Won This Month" summary card. Shown
    # side-by-side with Created in the same table, not just its own rows.
    manager_won_totals: dict[str, dict[str, int]] = {}
    owner_won_totals: dict[str, dict[str, int]] = {}
    for r in items:
        if _normalize_stage(r.get("stage") or "") != won_norm:
            continue
        changed = _parse_dt(r.get("stage_last_changed_at")) or _parse_dt(r.get("updated_at"))
        if not changed:
            continue
        month_key = changed.strftime("%Y-%m")
        if month_key not in created_month_keys:
            continue
        owner = r.get("owner_name") or ""
        # Key the owner rollup by the roster's spelling so it joins the Created
        # by_owner rows even when the CRM owner_name differs in case/spacing.
        owner_key = TEAM_ROSTER_KEY_TO_NAME.get(_name_key(owner), owner)
        manager = _owner_manager(owner)
        manager_won_totals.setdefault(manager, {}).setdefault(month_key, 0)
        manager_won_totals[manager][month_key] += 1
        owner_won_totals.setdefault(owner_key, {}).setdefault(month_key, 0)
        owner_won_totals[owner_key][month_key] += 1

    def _rollup(
        totals: dict[str, dict[str, int]],
        targets: dict[str, int],
        won_totals: dict[str, dict[str, int]],
        won_targets: dict[str, int],
    ) -> list[dict]:
        rows = []
        for name, by_month in totals.items():
            counts = {mk: by_month.get(mk, 0) for mk in created_month_keys}
            won_by_month = won_totals.get(name, {})
            won_counts = {mk: won_by_month.get(mk, 0) for mk in created_month_keys}
            rows.append({
                "name": name,
                "counts": counts,
                "total": sum(counts.values()),
                "target": targets.get(name),
                "won_counts": won_counts,
                "won_total": sum(won_counts.values()),
                "won_target": won_targets.get(name),
            })
        return sorted(rows, key=lambda x: -x["total"])

    created_by_month = {
        "months": created_month_keys,
        "by_manager": _rollup(manager_totals, MANAGER_TARGETS, manager_won_totals, CLOSED_WON_MANAGER_TARGETS),
        "by_owner": _rollup(owner_created_totals, OWNER_TARGETS, owner_won_totals, CLOSED_WON_OWNER_TARGETS),
        "unmapped_owner_count": len(unmapped_owners),
        "unmapped_owners": sorted(o for o in unmapped_owners if o),
    }

    # Activity (Notebook, Task, CRM Updated %) and Action Items are scoped
    # to active, new-business-cadence pipeline only — Future Opportunity is
    # parked (not being worked), and Up-Selling/Pricing opportunities on an
    # existing account aren't held to the same update cadence as new
    # business, so both are excluded from this population.
    future_norm_activity = _normalize_stage("Future Opportunity")
    activity_scope_items = [
        r for r in open_items
        if _normalize_stage(r.get("stage") or "") != future_norm_activity
        and r.get("type") not in EXCLUDED_ACTIVITY_TYPES
    ]

    # Tasks are scoped the same way, via the Opportunity each is linked to
    # (a Task has no type/stage of its own) — a Task with no matching
    # Opportunity in `items` is kept rather than dropped, since that's a
    # data gap, not evidence the Task is out of scope.
    opp_stage_type_by_id = {r.get("id"): (r.get("stage") or "", r.get("type")) for r in items}

    def _task_opportunity_in_scope(t: dict) -> bool:
        info = opp_stage_type_by_id.get(t.get("related_record_id"))
        if not info:
            return True
        stage, opp_type = info
        return _normalize_stage(stage) != future_norm_activity and opp_type not in EXCLUDED_ACTIVITY_TYPES

    tasks_in_scope = [t for t in tasks if _task_opportunity_in_scope(t)]

    # Activity: how recently each OPEN deal's Notebook was last touched.
    # Reps are expected to add an entry at least every 7 days. Falls back to
    # created_at when an opportunity has no Notebook entry at all (never
    # touched is not the same as "recently touched").
    activity_bucket_counts = Counter()
    owner_activity: dict[str, Counter] = {}
    owner_totals: dict[str, int] = {}
    activity_undated_count = 0

    for r in activity_scope_items:
        nb_entry = notebook_last_touch.get(r.get("id"))
        last_touch = (nb_entry["last_touch"] if nb_entry else None) or _parse_dt(r.get("created_at"))
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
        round(touched_within_7d / len(activity_scope_items) * 100, 1) if activity_scope_items else 0.0
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

    # Task Activity: of every open (Not Started / In Progress) Task a rep
    # owns, how many were touched in the last 7 days AND aren't overdue.
    # "Touched" = updated_at, falling back to created_at when a task has
    # never been edited since it was logged (same fallback idea as Notebook
    # activity above). Tasks is a 642k-row global object with no country
    # field, so fetch_open_tasks() already scoped this to our own reps'
    # owner_ids — reps with no owner_id in the roster (TASK_UNMAPPED_REPS)
    # can't be measured here at all.
    today = now.date()
    task_total_by_owner: Counter = Counter()
    task_active_by_owner: Counter = Counter()
    for t in tasks_in_scope:
        owner = t.get("owner_name") or "(blank)"
        task_total_by_owner[owner] += 1
        last_activity = _parse_dt(t.get("updated_at")) or _parse_dt(t.get("created_at"))
        due = _parse_dt(t.get("due_date"))
        recent = bool(last_activity) and (now - last_activity).days <= 7
        not_overdue = due is None or due.date() >= today
        if recent and not_overdue:
            task_active_by_owner[owner] += 1

    task_total = sum(task_total_by_owner.values())
    task_active_total = sum(task_active_by_owner.values())
    task_activity_pct = round(task_active_total / task_total * 100, 1) if task_total else 0.0
    task_activity_by_owner = sorted(
        (
            {
                "name": owner,
                "total": total,
                "active": task_active_by_owner.get(owner, 0),
                "active_pct": round(task_active_by_owner.get(owner, 0) / total * 100, 1) if total else 0.0,
            }
            for owner, total in task_total_by_owner.items()
        ),
        key=lambda row: row["active_pct"],
    )

    # CRM Updated %: the single final KPI — Notebook and Task activity summed
    # into one numerator/denominator rather than averaged, so a rep with e.g.
    # 50 open deals and 2 open tasks isn't dragged down (or propped up) by
    # treating both signals as equally weighted.
    crm_updated_numerator = touched_within_7d + task_active_total
    crm_updated_denominator = len(activity_scope_items) + task_total
    crm_updated_pct = (
        round(crm_updated_numerator / crm_updated_denominator * 100, 1)
        if crm_updated_denominator else 0.0
    )
    crm_updated_owner_names = set(owner_activity) | set(task_total_by_owner)
    crm_updated_by_owner = sorted(
        (
            {
                "name": owner,
                "updated": (
                    owner_activity.get(owner, Counter()).get(activity_labels[0], 0)
                    + task_active_by_owner.get(owner, 0)
                ),
                "total": owner_totals.get(owner, 0) + task_total_by_owner.get(owner, 0),
                "pct": (
                    round(
                        (
                            owner_activity.get(owner, Counter()).get(activity_labels[0], 0)
                            + task_active_by_owner.get(owner, 0)
                        )
                        / (owner_totals.get(owner, 0) + task_total_by_owner.get(owner, 0))
                        * 100,
                        1,
                    )
                    if owner_totals.get(owner, 0) + task_total_by_owner.get(owner, 0) else 0.0
                ),
            }
            for owner in crm_updated_owner_names
        ),
        key=lambda row: -row["pct"],
    )

    # Pipeline Conversion: of the unique deals "in play" this month — newly
    # created, still open, or resolved (won or lost) this month — what share
    # actually became Closed-Won this month. pipeline_conversion_ids is
    # already de-duplicated (a deal created and closed within the same
    # month only counts once), computed just after open_items above.
    pipeline_conversion_denominator = len(pipeline_conversion_ids)
    pipeline_conversion_pct = (
        round(closed_won_this_month / pipeline_conversion_denominator * 100, 1)
        if pipeline_conversion_denominator else 0.0
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
        "closed_won_this_month": closed_won_this_month,
        "closed_lost_this_month": closed_lost_this_month,
        "closed_won_last_month": closed_won_last_month,
        "closed_lost_last_month": closed_lost_last_month,
        "closed_total": closed_total,
        "pipeline_conversion_pct": pipeline_conversion_pct,
        "pipeline_conversion_numerator": closed_won_this_month,
        "pipeline_conversion_denominator": pipeline_conversion_denominator,
        "lead_time_to_won_avg_days": lead_time_to_won_avg_days,
        "lead_time_to_won_count": lead_time_to_won_count,
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
        "task_total": task_total,
        "task_active_total": task_active_total,
        "task_activity_pct": task_activity_pct,
        "task_activity_by_owner": task_activity_by_owner,
        "task_unmapped_reps": TASK_UNMAPPED_REPS,
        "crm_updated_pct": crm_updated_pct,
        "crm_updated_numerator": crm_updated_numerator,
        "crm_updated_denominator": crm_updated_denominator,
        "crm_updated_by_owner": crm_updated_by_owner,
        "action_items": (
            _build_action_items(_build_opportunity_rows(items, notebook_last_touch, tasks), tasks_in_scope, roster_scope)
            if include_action_items else None
        ),
        "pip_radar": _build_pip_radar(items, created_by_month, crm_updated_by_owner, roster_scope, now),
    }


@app.get("/health")
def health():
    return {"status": "ok"}


def _serialize_notebook_last_touch(nlt: dict[int, dict]) -> dict[str, dict]:
    return {str(rid): {"last_touch": v["last_touch"].isoformat(), "content": v.get("content")} for rid, v in nlt.items()}


def _deserialize_notebook_last_touch(raw: dict) -> dict[int, dict]:
    result: dict[int, dict] = {}
    for rid, v in raw.items():
        touched = _parse_dt(v.get("last_touch"))
        if touched is None:
            continue
        result[int(rid)] = {"last_touch": touched, "content": v.get("content")}
    return result


async def _save_snapshot() -> None:
    """Best-effort persist of the cache to object storage, so the next
    restart can warm-start from it instead of a cold cache. A save failure
    never fails the refresh itself — the in-memory cache this request cycle
    produced is already correct; only durability across a restart is lost,
    and the next successful refresh will just try saving again."""
    try:
        payload = json.dumps({
            "items": _cache["items"],
            "notebook_last_touch": _serialize_notebook_last_touch(_cache["notebook_last_touch"]),
            "tasks": _cache["tasks"],
            "leads": _cache["leads"],
            "snapshot_at": _cache["snapshot_at"],
            "last_full_pull_at": _cache["last_full_pull_at"],
            "account_industry": {str(k): v for k, v in (_cache["account_industry"] or {}).items()},
        }).encode("utf-8")
        await run_in_threadpool(storage.put_bytes, _SNAPSHOT_KEY, payload, content_type="application/json")
    except Exception:
        pass


async def _load_snapshot() -> None:
    """Warm the cache from the last persisted snapshot at startup, if any —
    so the dashboard serves (slightly stale) data immediately instead of
    every restart blocking on a fresh full pull, and so the first refresh
    after a restart is a cheap delta rather than the ~730-request pull that
    used to trip the CRM's rate limiter right after every deploy."""
    try:
        raw = await run_in_threadpool(storage.get_bytes, _SNAPSHOT_KEY)
        data = json.loads(raw)
        _cache["items"] = data["items"]
        _cache["notebook_last_touch"] = _deserialize_notebook_last_touch(data["notebook_last_touch"])
        _cache["tasks"] = data["tasks"]
        _cache["leads"] = data.get("leads")
        for _l in _cache["leads"] or []:
            _l["owner_name"] = _normalize_owner_name(_l.get("owner_name"))
        # Snapshots written before owner_name canonicalization can hold stray
        # double spaces; normalize on warm-load so the fix takes effect on the
        # first restart, not only after the next full pull re-ingests.
        for _r in _cache["items"] or []:
            _r["owner_name"] = _normalize_owner_name(_r.get("owner_name"))
        for _t in _cache["tasks"] or []:
            _t["owner_name"] = _normalize_owner_name(_t.get("owner_name"))
        _cache["snapshot_at"] = data["snapshot_at"]
        _cache["last_full_pull_at"] = data.get("last_full_pull_at")
        _cache["account_industry"] = {int(k): v for k, v in (data.get("account_industry") or {}).items()}
        _cache["fetched_at"] = time.time()
    except Exception:
        pass  # no snapshot yet (first-ever deploy), or object storage not configured/reachable


async def _refresh_dashboard_cache() -> None:
    if _cache["refreshing"]:
        return
    _cache["refreshing"] = True
    # Recorded on every attempt (success or failure) — separate from
    # snapshot_at, which only advances on success. Once one refresh has
    # succeeded, a later failure keeps serving the old (still valid) data
    # with no visible error, so without this a string of silent failures
    # is invisible until someone happens to notice the snapshot has gone
    # stale. Surfaced in /api/pipeline so the frontend can flag it.
    _cache["last_attempt_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        async with httpx.AsyncClient() as client:
            # Run one at a time rather than gathered — three concurrent
            # paginated fetches (each already firing several requests at
            # once internally) was enough to trip the CRM's rate limiter,
            # especially right after a fresh restart.
            do_full_pull = (
                _cache["items"] is None
                or _cache["last_full_pull_at"] is None
                or time.time() - _cache["last_full_pull_at"] >= FULL_PULL_INTERVAL_SECONDS
            )
            if do_full_pull:
                items = await fetch_all_opportunities(client)
                # Full pull re-resolves industry for every account in play,
                # so a changed Account.industry (rare, but it happens) is
                # caught here rather than never.
                account_ids = sorted({r.get("account_id") for r in items if r.get("account_id") is not None})
                account_industry = await fetch_account_industries(client, account_ids)
            else:
                delta = await fetch_opportunities_delta(client, _cache["snapshot_at"])
                items = _merge_opportunities(_cache["items"], delta)
                # Steady-state delta: only look up accounts we haven't seen
                # before, typically zero or a handful — not the whole set of
                # accounts behind every cached Opportunity.
                account_industry = dict(_cache["account_industry"] or {})
                account_ids = {r.get("account_id") for r in items if r.get("account_id") is not None}
                missing_ids = sorted(account_ids - account_industry.keys())
                if missing_ids:
                    account_industry.update(await fetch_account_industries(client, missing_ids))
            for r in items:
                r["industry"] = account_industry.get(r.get("account_id"))
            notebook_last_touch = await fetch_notebook_last_touch(client)
            tasks = await fetch_open_tasks(client)
        # Only the raw fetch is cached — build_dashboard() re-runs per
        # request (cheap, pure in-memory aggregation) so the filter bar can
        # slice owners/managers/product lines/service levels without
        # waiting for the next 15-minute refresh.
        _cache["items"] = items
        _cache["notebook_last_touch"] = notebook_last_touch
        _cache["tasks"] = tasks
        _cache["account_industry"] = account_industry
        _cache["snapshot_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _cache["fetched_at"] = time.time()
        _cache["error"] = None
        _cache["consecutive_failures"] = 0
        # Only recorded once the whole cycle (including Notebook/Tasks)
        # actually commits — otherwise a full Opportunities pull followed by
        # a failed Notebook/Task fetch would mark the day's reconciliation
        # done without it ever landing in the cache.
        if do_full_pull:
            _cache["last_full_pull_at"] = time.time()
        await _save_snapshot()
    except Exception as e:
        _cache["error"] = str(e)
        _cache["consecutive_failures"] += 1
    finally:
        _cache["refreshing"] = False


async def _refresh_loop() -> None:
    while True:
        await _refresh_dashboard_cache()
        failures = _cache["consecutive_failures"]
        delay = (
            min(REFRESH_INTERVAL_SECONDS * (2 ** failures), REFRESH_BACKOFF_CAP_SECONDS)
            if failures else REFRESH_INTERVAL_SECONDS
        )
        await asyncio.sleep(delay)


# Leads have no cheap delta and the open-lead set is thousands of records, so
# they refresh on their own gentle schedule, fully decoupled from the 15-minute
# opportunity refresh — a heavy or rate-limited lead pull can neither stall nor
# be stalled by it. Refreshed a few times a day; retried sooner while it has
# failed or nothing is cached yet.
LEADS_REFRESH_INTERVAL_SECONDS = 6 * 3600
LEADS_RETRY_SECONDS = 20 * 60
LEADS_STARTUP_DELAY_SECONDS = 60


async def _refresh_leads_cache() -> None:
    """Best-effort independent refresh of the Leads cache. A failure here never
    touches the opportunity/notebook/task cache — it leaves the previous leads
    in place (or None) to be retried on the next cycle."""
    try:
        async with httpx.AsyncClient() as client:
            _cache["leads"] = await fetch_leads(client)
        await _save_snapshot()
    except Exception:
        pass


async def _leads_refresh_loop() -> None:
    # Let the opportunity load settle first so two large pulls don't hit the
    # CRM at once right after a restart.
    await asyncio.sleep(LEADS_STARTUP_DELAY_SECONDS)
    while True:
        await _refresh_leads_cache()
        await asyncio.sleep(
            LEADS_REFRESH_INTERVAL_SECONDS if _cache["leads"] is not None else LEADS_RETRY_SECONDS
        )


@app.on_event("startup")
async def _start_background_refresh() -> None:
    await _load_snapshot()
    asyncio.create_task(_refresh_loop())
    asyncio.create_task(_leads_refresh_loop())


@app.get("/api/pipeline")
async def get_pipeline(
    request: Request,
    owners: list[str] | None = Query(None),
    managers: list[str] | None = Query(None),
    product_lines: list[str] | None = Query(None),
    service_levels: list[str] | None = Query(None),
    industries: list[str] | None = Query(None),
):
    if _cache["items"] is None:
        if _cache["error"]:
            raise HTTPException(
                status_code=502,
                detail=f"Initial data load failed: {_cache['error']}",
            )
        raise HTTPException(
            status_code=503,
            detail="Initial data load in progress (fetching ~73k records in the background) — retry shortly",
        )

    # Identity-scoped viewing: a mapped Salesperson only ever sees their own
    # pipeline, a Manager/Sales Head their team's — this is the real
    # boundary (clamps whatever the client asked for), not just a UI
    # default, since X-Forwarded-Email can't be spoofed once SSO is on.
    viewer_name, allowed_owners, effective_owners = _effective_owners(request, owners)

    filtered_items = _apply_filters(
        _cache["items"], effective_owners, managers, product_lines, service_levels, industries
    )
    filtered_tasks = _apply_task_filters(_cache["tasks"], effective_owners, managers)
    roster_scope = _scoped_roster_names(effective_owners, managers)
    # Action Items is personalized, so it only appears for an identity-scoped
    # viewer or when an admin/unmapped viewer has deliberately filtered down
    # to a Salesperson/Manager — never on an unfiltered, unrestricted view.
    show_action_items = allowed_owners is not None or bool(owners) or bool(managers)
    result = build_dashboard(
        filtered_items, _cache["notebook_last_touch"], filtered_tasks, roster_scope, show_action_items
    )
    result["snapshot_at"] = _cache["snapshot_at"]
    result["filter_options"] = _filter_options(_cache["items"], allowed_owners, effective_owners, managers)
    result["viewer_name"] = viewer_name
    result["viewer_scoped"] = allowed_owners is not None
    # Non-null only when the most recent background refresh attempt failed
    # — data below is still the last good snapshot (from snapshot_at), not
    # broken, but it's gone stale silently since that attempt.
    result["last_refresh_error"] = _cache["error"]
    result["last_refresh_attempt_at"] = _cache["last_attempt_at"] if _cache["error"] else None
    result["consecutive_refresh_failures"] = _cache["consecutive_failures"]
    return result


@app.get("/api/opportunities")
async def get_opportunities(
    request: Request,
    owners: list[str] | None = Query(None),
    managers: list[str] | None = Query(None),
    product_lines: list[str] | None = Query(None),
    service_levels: list[str] | None = Query(None),
    industries: list[str] | None = Query(None),
):
    """Flat per-opportunity rows for the Opportunity List tab — same
    filters, same identity scoping, and the same cached fetch as
    /api/pipeline, just not pre-aggregated."""
    if _cache["items"] is None:
        if _cache["error"]:
            raise HTTPException(
                status_code=502,
                detail=f"Initial data load failed: {_cache['error']}",
            )
        raise HTTPException(
            status_code=503,
            detail="Initial data load in progress (fetching ~73k records in the background) — retry shortly",
        )

    viewer_name, allowed_owners, effective_owners = _effective_owners(request, owners)
    filtered_items = _apply_filters(
        _cache["items"], effective_owners, managers, product_lines, service_levels, industries
    )
    all_tasks = _cache["tasks"] or []
    rows = _build_opportunity_rows(filtered_items, _cache["notebook_last_touch"], all_tasks)

    # Task List: every open Task joined to its Opportunity row — scoped by
    # which opportunities matched above (Salesperson/Manager/Product
    # Line/Service Level all apply transitively through the join), not by
    # re-filtering Tasks on their own.
    opp_by_id = {row["id"]: row for row in rows}
    task_rows = _build_task_rows(all_tasks, opp_by_id)

    return {
        "rows": rows,
        "total": len(rows),
        "tasks": task_rows,
        "task_total": len(task_rows),
        "snapshot_at": _cache["snapshot_at"],
        "viewer_name": viewer_name,
        "viewer_scoped": allowed_owners is not None,
    }


@app.get("/api/leads")
async def get_leads(
    request: Request,
    owners: list[str] | None = Query(None),
    managers: list[str] | None = Query(None),
    product_lines: list[str] | None = Query(None),
    service_levels: list[str] | None = Query(None),
):
    """Lead counts for the Leads tab: how many leads were created (this month,
    last month, and per month) with their status breakdown, plus how many leads
    are still open (Suspect / Suspect follow up / Prospect Qualified) regardless
    of when they were created. Same identity scoping as the rest of the app;
    Product Line / Service Level filters don't apply to Leads."""
    if _cache["leads"] is None:
        raise HTTPException(
            status_code=503,
            detail="Leads are still loading from the CRM — check back in a minute.",
        )

    viewer_name, allowed_owners, effective_owners = _effective_owners(request, owners)
    leads = _apply_lead_filters(_cache["leads"], effective_owners, managers)

    now = datetime.now(timezone.utc)
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_end = this_month_start - timedelta(seconds=1)
    last_month_start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    month_keys = [now.strftime("%Y-%m")]
    cursor = now.replace(day=1)
    for _ in range(2):
        cursor = (cursor - timedelta(days=1)).replace(day=1)
        month_keys.append(cursor.strftime("%Y-%m"))
    month_keys = sorted(set(month_keys))

    created_this_month = 0
    created_last_month = 0
    created_status_counts: Counter = Counter()
    created_month_counts: Counter = Counter()
    open_status_counts: Counter = Counter()
    for lead in leads:
        status = lead.get("status")
        if status in LEAD_OPEN_STATUSES:
            open_status_counts[status] += 1
        created = _parse_dt(lead.get("created_at"))
        if not created:
            continue
        month_key = created.strftime("%Y-%m")
        if month_key in month_keys:
            created_month_counts[month_key] += 1
        if created >= this_month_start:
            created_this_month += 1
            created_status_counts[status] += 1
        elif last_month_start <= created <= last_month_end:
            created_last_month += 1

    return {
        "created_this_month": created_this_month,
        "created_last_month": created_last_month,
        "created_by_status": [
            {"status": s, "count": created_status_counts.get(s, 0)}
            for s in LEAD_STATUS_ORDER if created_status_counts.get(s, 0)
        ],
        "open_total": sum(open_status_counts.values()),
        "open_by_status": [
            {"status": s, "count": open_status_counts.get(s, 0)} for s in LEAD_OPEN_STATUSES
        ],
        "created_by_month": [
            {"month": mk, "count": created_month_counts.get(mk, 0)} for mk in month_keys
        ],
        "total_in_scope": len(leads),
        "snapshot_at": _cache["snapshot_at"],
        "viewer_name": viewer_name,
        "viewer_scoped": allowed_owners is not None,
    }


@app.get("/{full_path:path}")
def serve_dashboard(full_path: str):
    return FileResponse(
        STATIC_DIR / "index.html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )
