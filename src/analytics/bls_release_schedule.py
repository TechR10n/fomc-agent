"""Fetch and parse BLS release schedules from bls.gov.

BLS publishes "Schedule of News Releases" pages (typically ET) that list future
release dates/times for a program (e.g., CPI, Employment Situation).

This module scrapes those pages into structured "scheduled release" events.
It is intentionally stdlib-only so it can run in lightweight environments.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone, tzinfo
from html.parser import HTMLParser
from typing import Any
from zoneinfo import ZoneInfo

from src.helpers.http_client import fetch_text

BLS_SCHEDULE_BASE_URL = "https://www.bls.gov/schedule/news_release"

# BLS site pages are generally in Eastern Time.
DEFAULT_SCHEDULE_TIMEZONE = "America/New_York"

USER_AGENT = os.environ.get(
    "BLS_USER_AGENT",
    "fomc-agent/1.0 (release-schedule; contact: ryan.hammang@outlook.com)",
)


@dataclass(frozen=True)
class ScheduleSource:
    series: str
    release: str
    url: str


DEFAULT_SCHEDULE_SOURCES: dict[str, ScheduleSource] = {
    # Common, high-signal BLS releases.
    "cu": ScheduleSource(
        series="cu",
        release="Consumer Price Index",
        url=f"{BLS_SCHEDULE_BASE_URL}/cpi.htm",
    ),
    "ce": ScheduleSource(
        series="ce",
        release="Employment Situation",
        url=f"{BLS_SCHEDULE_BASE_URL}/empsit.htm",
    ),
    "ln": ScheduleSource(
        series="ln",
        release="Employment Situation",
        url=f"{BLS_SCHEDULE_BASE_URL}/empsit.htm",
    ),
    "jt": ScheduleSource(
        series="jt",
        release="Job Openings and Labor Turnover Survey",
        url=f"{BLS_SCHEDULE_BASE_URL}/jolts.htm",
    ),
    "ci": ScheduleSource(
        series="ci",
        release="Employment Cost Index",
        url=f"{BLS_SCHEDULE_BASE_URL}/eci.htm",
    ),
    "pr": ScheduleSource(
        series="pr",
        release="Productivity and Costs",
        url=f"{BLS_SCHEDULE_BASE_URL}/prod2.htm",
    ),
}


def _get_tz(name: str | None) -> tzinfo:
    tz_name = (name or "").strip() or DEFAULT_SCHEDULE_TIMEZONE
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


def _to_utc_iso(dt: datetime) -> str:
    aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_header(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip()).lower()
    s = re.sub(r"[\u00a0\t]", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_ordinal_suffixes(s: str) -> str:
    return re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", s, flags=re.IGNORECASE)


def _parse_date(s: str) -> datetime | None:
    raw = re.sub(r"\s+", " ", (s or "").strip())
    if not raw:
        return None
    if raw.lower() in {"tbd", "to be determined"}:
        return None

    raw = re.sub(
        r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday),\s+",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    raw = _strip_ordinal_suffixes(raw)

    # Common month abbreviations with trailing dots (e.g., "Feb. 10, 2026")
    raw = raw.replace("Sept.", "Sep.")

    candidates = [
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
        "%m/%d/%Y",
        "%Y-%m-%d",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


_TIME_RE = re.compile(
    r"(?P<h>\d{1,2})(?::(?P<m>\d{2}))?\s*(?P<ampm>a\.?m\.?|p\.?m\.?|am|pm)\b",
    flags=re.IGNORECASE,
)


def _parse_time(s: str) -> tuple[int, int] | None:
    raw = re.sub(r"\s+", " ", (s or "").strip())
    if not raw:
        return None
    m = _TIME_RE.search(raw)
    if not m:
        return None
    hour = int(m.group("h"))
    minute = int(m.group("m") or "0")
    ampm = (m.group("ampm") or "").lower()

    is_pm = ampm.startswith("p")
    is_am = ampm.startswith("a")
    if hour == 12 and is_am:
        hour = 0
    elif hour != 12 and is_pm:
        hour += 12
    return hour, minute


class _HTMLTableExtractor(HTMLParser):
    """Extract HTML tables as nested lists of (row -> cells)."""

    def __init__(self):
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._table_depth = 0
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._in_cell = False
        self._cell_parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._current_table = []
        if self._table_depth >= 1 and tag == "tr":
            self._current_row = []
        if self._table_depth >= 1 and tag in {"td", "th"} and self._current_row is not None:
            self._in_cell = True
            self._cell_parts = []

    def handle_endtag(self, tag):
        if self._table_depth >= 1 and tag in {"td", "th"} and self._in_cell and self._current_row is not None:
            text = re.sub(r"\s+", " ", "".join(self._cell_parts)).strip()
            self._current_row.append(text)
            self._in_cell = False
            self._cell_parts = []

        if self._table_depth >= 1 and tag == "tr" and self._current_row is not None and self._current_table is not None:
            if any(c.strip() for c in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = None

        if tag == "table" and self._table_depth >= 1:
            if self._table_depth == 1 and self._current_table is not None:
                if self._current_table:
                    self.tables.append(self._current_table)
                self._current_table = None
            self._table_depth -= 1

    def handle_data(self, data):
        if self._in_cell:
            self._cell_parts.append(data)


def extract_tables(html: str) -> list[list[list[str]]]:
    parser = _HTMLTableExtractor()
    parser.feed(html or "")
    return parser.tables


def _select_schedule_table(tables: list[list[list[str]]]) -> tuple[list[list[str]], int, int] | None:
    """Return (table, date_col_idx, time_col_idx) for the most likely schedule table."""
    for table in tables:
        if not table:
            continue
        header = table[0]
        norms = [_normalize_header(h) for h in header]

        date_idx = None
        time_idx = None
        for i, h in enumerate(norms):
            if date_idx is None and ("release date" in h or h == "release date" or h == "date"):
                date_idx = i
            if time_idx is None and ("release time" in h or h == "release time" or h == "time"):
                time_idx = i

        # Some pages omit "Release" and just use "Date" / "Time"
        if date_idx is not None and time_idx is None:
            for i, h in enumerate(norms):
                if h.endswith("time") or h == "time":
                    time_idx = i
                    break

        if date_idx is not None and time_idx is not None:
            return table, date_idx, time_idx
    return None


def parse_schedule_html(
    html: str,
    *,
    series_id: str,
    release: str,
    url: str,
    schedule_tz: str | None = None,
) -> list[dict[str, Any]]:
    """Parse one BLS schedule page into scheduled release events."""
    tz = _get_tz(schedule_tz)
    tables = extract_tables(html)
    selected = _select_schedule_table(tables)
    if not selected:
        return []
    table, date_idx, time_idx = selected

    out: list[dict[str, Any]] = []
    for row in table[1:]:
        if date_idx >= len(row):
            continue
        date_raw = row[date_idx]
        time_raw = row[time_idx] if time_idx < len(row) else ""

        date_dt = _parse_date(date_raw)
        time_parts = _parse_time(time_raw)
        if not date_dt or not time_parts:
            continue

        hour, minute = time_parts
        local_dt = datetime(
            date_dt.year,
            date_dt.month,
            date_dt.day,
            hour,
            minute,
            tzinfo=tz,
        )

        out.append({
            "series": series_id,
            "release": release,
            "url": url,
            "scheduled_time": _to_utc_iso(local_dt),
            "scheduled_time_local": local_dt.isoformat(),
            "time_zone": getattr(tz, "key", str(tz)),
        })
    return out


def fetch_schedule_html(url: str) -> str:
    return fetch_text(url, headers={"User-Agent": USER_AGENT}, timeout=30, retries=3)


def _parse_schedule_overrides(raw: str | None) -> dict[str, ScheduleSource]:
    """Parse env overrides for schedule URLs / release names.

    Supports:
      - JSON: {"cu": {"url": "...", "release": "..."}, "ce": "https://..."}
      - CSV:  cu=https://...,ce=https://...
    """
    if not raw:
        return {}

    text = raw.strip()
    if not text:
        return {}

    # JSON format
    if text.startswith("{"):
        try:
            payload = json.loads(text)
        except Exception:
            return {}
        out: dict[str, ScheduleSource] = {}
        if not isinstance(payload, dict):
            return out
        for series_id, value in payload.items():
            if not series_id:
                continue
            if isinstance(value, str):
                out[str(series_id)] = ScheduleSource(series=str(series_id), release=str(series_id), url=value)
            elif isinstance(value, dict):
                url = value.get("url")
                if not isinstance(url, str) or not url.strip():
                    continue
                release = value.get("release")
                release_name = str(release).strip() if release else str(series_id)
                out[str(series_id)] = ScheduleSource(series=str(series_id), release=release_name, url=url.strip())
        return out

    # CSV format: series=url,series=url
    out: dict[str, ScheduleSource] = {}
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            continue
        series_id, url = part.split("=", 1)
        series_id = series_id.strip()
        url = url.strip()
        if series_id and url:
            out[series_id] = ScheduleSource(series=series_id, release=series_id, url=url)
    return out


def get_schedule_sources(series_list: list[str]) -> dict[str, ScheduleSource]:
    """Return schedule sources for the provided series list.

    Allows overrides via env var `BLS_RELEASE_SCHEDULE_SOURCES`.
    """
    overrides = _parse_schedule_overrides(os.environ.get("BLS_RELEASE_SCHEDULE_SOURCES"))
    sources: dict[str, ScheduleSource] = {}
    for series_id in series_list:
        if series_id in overrides:
            sources[series_id] = overrides[series_id]
        elif series_id in DEFAULT_SCHEDULE_SOURCES:
            sources[series_id] = DEFAULT_SCHEDULE_SOURCES[series_id]
    return sources


def load_scheduled_releases(
    *,
    series_list: list[str],
    start: datetime,
    end: datetime,
    schedule_tz: str | None = None,
) -> list[dict[str, Any]]:
    """Load scheduled releases for the window [start, end]."""
    sources = get_schedule_sources(series_list)
    scheduled: list[dict[str, Any]] = []

    for series_id, src in sources.items():
        try:
            html = fetch_schedule_html(src.url)
        except Exception:
            continue
        scheduled.extend(
            parse_schedule_html(
                html,
                series_id=series_id,
                release=src.release,
                url=src.url,
                schedule_tz=schedule_tz,
            )
        )

    # Filter to window.
    start_utc = start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc = end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc)

    def _parse(dt_str: str | None) -> datetime | None:
        if not dt_str:
            return None
        raw = dt_str.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    filtered: list[dict[str, Any]] = []
    for e in scheduled:
        dt = _parse(e.get("scheduled_time"))
        if dt is None:
            continue
        if start_utc <= dt <= end_utc:
            filtered.append(e)

    filtered.sort(key=lambda r: r.get("scheduled_time", ""))
    return filtered

