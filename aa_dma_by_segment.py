#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Adobe Analytics API 2.0 - Weekly DMA x Last Touch Channel (WW only)

Behavior:
- ONE weekly XLSX file
- Data is pulled per DAY (Date column is YYYY-MM-DD)
- File name pattern:
    adobe_dma_by_segment_WW_<weekStartYYYY-MM-DD>_to_<weekEndYYYY-MM-DD>.xlsx
  where weekEnd is SATURDAY (inclusive)

Fiscal behavior (4-4-5 anchored):
- Weeks are Sunday->Saturday
- Auto mode (no START_DATE/END_DATE env) pulls the last completed Sun->Sat week
- Adds FY/WK tag into filename when a fiscal anchor applies:
    adobe_dma_by_segment_WW_FY2026_WK01_2026-01-04_to_2026-01-10.xlsx

Important cache behavior in this version:
- USE_CACHE defaults to FALSE
- CLEAR_WW_CACHE_ON_START defaults to TRUE
- If cache is enabled, cache key includes a report signature so stale payloads do not get reused

Optional debug:
- DEBUG_SAMPLE_ROWS=true prints a small sample of Adobe's first returned row for a few requests
"""

import os
import time
import json
import shutil
import hashlib
import threading
import ftplib
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date

import requests
import pandas as pd
from tqdm.auto import tqdm

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # for older Python if needed


# ================== Config (env-driven in GitHub Actions) ==================

# Adobe credentials (MANDATORY via env)
AA_CLIENT_ID = os.environ.get("AA_CLIENT_ID", "")
AA_CLIENT_SECRET = os.environ.get("AA_CLIENT_SECRET", "")
AA_API_KEY = os.environ.get("AA_API_KEY", AA_CLIENT_ID or "")
AA_ORG_ID = os.environ.get("AA_ORG_ID", "")
AA_COMPANY_ID = os.environ.get("AA_COMPANY_ID", "")

AUTH_URL = "https://ims-na1.adobelogin.com/ims/token/v3"
REPORT_URL = f"https://analytics.adobe.io/api/{AA_COMPANY_ID}/reports" if AA_COMPANY_ID else ""

# WW only
DEFAULT_BRAND_RSIDS = [
    "vrs_ospgro1_womanwithin",
]
BRAND_RSIDS = [s.strip() for s in os.environ.get("BRAND_RSIDS", "").split(",") if s.strip()] or DEFAULT_BRAND_RSIDS

# WW Segment mapping
SEGMENTS: Dict[str, str] = {
    "s300006919_67dc72805576732634ebe41d": "Affiliate (WW)",
    "s300006919_67dc732feb5ca74056703434": "Cross Brand (WW)",
    "s300006919_67dc72d1216b535a07c6b1b6": "CSE (WW)",
    "s300006919_67dc6f2224a81279e930aca3": "Direct to Site (WW)",
    "s300006919_67dc704524a81279e930acab": "Display - Retargeting  (WW)",
    "s300006919_67dc6ea029ae424c05d2ba0c": "Email (WW)",
    "s300006919_67dc723b5576732634ebe416": "Facebook Advantage (WW)",
    "s300006919_67dc6fbdb29fc63f53cdb4f8": "Natural Search (WW)",
    "s300006919_67dc6f8fb29fc63f53cdb4f5": "Paid Search - Brand (WW)",
    "s300006919_67dc72a9216b535a07c6b1b5": "Paid Search - Non Brand (WW)",
    "s300006919_67dc707829ae424c05d2ba26": "Paid Search - PLA (WW)",
    "s300006919_67dc72eeeb5ca74056703430": "SMS (WW)",
    "s300006919_67dc72165576732634ebe414": "Social - Organic (WW)",
    "s300006919_67dc71992d5fdc38d3a9acc8": "Social - Prospecting (WW)",
    "s300006919_67dc710fb29fc63f53cdb502": "Social - Reactivation (WW)",
    "s300006919_67dc70a0b29fc63f53cdb4fe": "Social - Retargeting (WW)",
    "s300006919_67dc7150216b535a07c6b19f": "Social - Retention (WW)",
    "s300006919_67dc6f5f29ae424c05d2ba1a": "Unspecified (WW)",
}

# Metrics and dimension
VISITS_ID = "metrics/visits"
ORDERS_ID = "metrics/orders"
DEMAND_ID = "cm300006919_5a132314ae06387a6a2fec85"
NMA_ID = "metrics/event233"
NTF_ID = "metrics/event263"
DIMENSION = "variables/geodma"

# Performance
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "2000"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))

# Output / Cache
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

CACHE_DIR = Path(".aa_cache")
CACHE_DIR.mkdir(exist_ok=True, parents=True)

# Cache controls
USE_CACHE = os.environ.get("USE_CACHE", "false").lower() in {"1", "true", "yes"}
CLEAR_WW_CACHE_ON_START = os.environ.get("CLEAR_WW_CACHE_ON_START", "true").lower() in {"1", "true", "yes"}
CACHE_NAMESPACE = os.environ.get("CACHE_NAMESPACE", "ww_dma_v2")

# Debug
DEBUG_SAMPLE_ROWS = os.environ.get("DEBUG_SAMPLE_ROWS", "true").lower() in {"1", "true", "yes"}
DEBUG_MAX_SAMPLES = int(os.environ.get("DEBUG_MAX_SAMPLES", "10"))
_debug_sample_count = 0
_debug_sample_lock = threading.Lock()

# Required output columns in exact order
OUTPUT_COLUMNS = ["Date", "DMA", "Last_Touch_Channel", "Visits", "Orders", "Demand", "NMA", "NTF"]

# OAuth scopes
OAUTH_SCOPES = "openid,AdobeID,read_organizations,additional_info.projectedProductContext,session"

# FTP config
FTP_HOST = os.environ.get("FTP_HOST", "")
FTP_USER = os.environ.get("FTP_USER", "")
FTP_PASS = os.environ.get("FTP_PASS", "")
FTP_DIR = os.environ.get("FTP_DIR", "/")
FTP_TLS = os.environ.get("FTP_TLS", "true").lower() in {"1", "true", "yes"}


# ================== Date / Fiscal helpers ==================

def _week_start_sunday(d: date) -> date:
    days_since_sunday = (d.weekday() + 1) % 7
    return d - timedelta(days=days_since_sunday)

def _parse_ymd(s: str) -> date:
    return date.fromisoformat(s.strip())

def iso_to_date(iso_str: str) -> date:
    return date.fromisoformat(iso_str[:10])

def iter_days(start_d: date, end_d_exclusive: date) -> List[Tuple[str, str, str]]:
    days: List[Tuple[str, str, str]] = []
    cur = start_d
    while cur < end_d_exclusive:
        nxt = cur + timedelta(days=1)
        days.append((f"{cur}T00:00:00.000", f"{nxt}T00:00:00.000", str(cur)))
        cur = nxt
    return days

def parse_fiscal_anchors() -> List[Tuple[int, date]]:
    raw = (os.environ.get("FISCAL_ANCHORS") or "").strip()
    anchors: List[Tuple[int, date]] = []

    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        for p in parts:
            if "=" not in p:
                raise SystemExit(
                    f"Invalid FISCAL_ANCHORS entry '{p}'. Expected like '2026=2026-01-04'."
                )
            fy_s, ymd = [x.strip() for x in p.split("=", 1)]
            fy = int(fy_s)
            d = _parse_ymd(ymd)
            if _week_start_sunday(d) != d:
                raise SystemExit(
                    f"Fiscal anchor for FY{fy} must be a Sunday. Got {d} "
                    f"(Sunday start would be {_week_start_sunday(d)})."
                )
            anchors.append((fy, d))
    else:
        fy = int(os.environ.get("FISCAL_YEAR_LABEL", "2026"))
        d = _parse_ymd(os.environ.get("FISCAL_WEEK1_START_YMD", "2026-01-04"))
        if _week_start_sunday(d) != d:
            raise SystemExit(
                f"FISCAL_WEEK1_START_YMD must be a Sunday. Got {d} "
                f"(Sunday start would be {_week_start_sunday(d)})."
            )
        anchors.append((fy, d))

    anchors.sort(key=lambda x: x[1])
    return anchors

FISCAL_ANCHORS_LIST: List[Tuple[int, date]] = parse_fiscal_anchors()

def fiscal_year_and_week_for_week_start(week_start: date) -> Tuple[Optional[int], Optional[int], Optional[date]]:
    applicable = [a for a in FISCAL_ANCHORS_LIST if a[1] <= week_start]
    if not applicable:
        return None, None, None

    fy, fy_week1 = applicable[-1]
    week_no = ((week_start - fy_week1).days // 7) + 1
    return fy, week_no, fy_week1

def prior_week_sun_to_sun_iso() -> Tuple[str, str, str, str, Optional[int], Optional[int], Optional[date]]:
    tz = ZoneInfo("America/New_York")
    today = datetime.now(tz).date()

    current_week_start = _week_start_sunday(today)
    start = current_week_start - timedelta(days=7)
    end = current_week_start

    fy, wk, fy_week1 = fiscal_year_and_week_for_week_start(start)

    start_iso = f"{start}T00:00:00.000"
    end_iso = f"{end}T00:00:00.000"
    return start_iso, end_iso, str(start), str(end), fy, wk, fy_week1


# ---------- Resolve weekly window ----------
START_DATE = os.environ.get("START_DATE")
END_DATE = os.environ.get("END_DATE")

FISCAL_YEAR: Optional[int] = None
FISCAL_WEEK: Optional[int] = None
FISCAL_WEEK1_START_USED: Optional[date] = None

if not START_DATE or not END_DATE:
    START_DATE, END_DATE, START_D, END_D, FISCAL_YEAR, FISCAL_WEEK, FISCAL_WEEK1_START_USED = prior_week_sun_to_sun_iso()
else:
    START_D, END_D = START_DATE[:10], END_DATE[:10]
    fy, wk, fy_week1 = fiscal_year_and_week_for_week_start(_week_start_sunday(iso_to_date(START_DATE)))
    FISCAL_YEAR, FISCAL_WEEK, FISCAL_WEEK1_START_USED = fy, wk, fy_week1

START_DATE_D = iso_to_date(START_DATE)
END_DATE_D_EXCL = iso_to_date(END_DATE)
DAY_RANGES = iter_days(START_DATE_D, END_DATE_D_EXCL)


# ================== Cache helpers ==================

def clear_cache_for_rsids(rsids: List[str]) -> None:
    for rsid in rsids:
        rsid_dir = CACHE_DIR / CACHE_NAMESPACE / rsid
        if rsid_dir.exists():
            shutil.rmtree(rsid_dir, ignore_errors=True)
            print(f"🧹 Cleared cache: {rsid_dir}")

def payload_signature(rsid: str, segment_id: str, day_start_iso: str, day_end_iso: str, limit: int) -> str:
    stable_payload = {
        "rsid": rsid,
        "segment_id": segment_id,
        "date_range": f"{day_start_iso}/{day_end_iso}",
        "dimension": DIMENSION,
        "metrics": [VISITS_ID, ORDERS_ID, DEMAND_ID, NMA_ID, NTF_ID],
        "limit": limit,
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "nonesBehavior": "return-nones",
        "cache_namespace": CACHE_NAMESPACE,
    }
    raw = json.dumps(stable_payload, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()[:12]

def get_cache_file(rsid: str, segment_id: str, day_label: str, sig: str, page: int) -> Path:
    seg_day_dir = CACHE_DIR / CACHE_NAMESPACE / rsid / segment_id / day_label / sig
    seg_day_dir.mkdir(parents=True, exist_ok=True)
    return seg_day_dir / f"page_{page:05d}.json"


# ================== Token Manager (thread-safe) ==================

class TokenManager:
    def __init__(self):
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def _request_new_token(self) -> Tuple[str, float]:
        data = {
            "grant_type": "client_credentials",
            "client_id": AA_CLIENT_ID,
            "client_secret": AA_CLIENT_SECRET,
            "scope": OAUTH_SCOPES
        }
        resp = requests.post(AUTH_URL, data=data, timeout=30)
        if not resp.ok:
            raise RuntimeError(f"IMS token request failed: {resp.status_code} {resp.text}")
        payload = resp.json()
        token = payload["access_token"]
        expires_at = time.time() + int(payload.get("expires_in", 3600))
        return token, expires_at

    def get_token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._expires_at - 30:
                return self._token
            self._token, self._expires_at = self._request_new_token()
            return self._token

    def force_refresh(self) -> str:
        with self._lock:
            self._token, self._expires_at = self._request_new_token()
            return self._token

TOKEN = TokenManager()

def build_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "x-api-key": AA_API_KEY,
        "x-gw-ims-org-id": AA_ORG_ID,
        "x-proxy-global-company-id": AA_COMPANY_ID,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }


# ================== Request / parse helpers ==================

def clean_channel_name(name_with_ww: str) -> str:
    return name_with_ww.replace(" (WW)", "").replace("  (WW)", "").strip()

def build_payload(rsid: str, segment_id: str, page: int, limit: int, day_start_iso: str, day_end_iso: str) -> Dict[str, Any]:
    return {
        "rsid": rsid,
        "globalFilters": [
            {"type": "segment", "segmentId": segment_id},
            {"type": "dateRange", "dateRange": f"{day_start_iso}/{day_end_iso}"}
        ],
        "metricContainer": {
            "metrics": [
                {"columnId": "0", "id": VISITS_ID, "sort": "desc"},
                {"columnId": "1", "id": ORDERS_ID},
                {"columnId": "2", "id": DEMAND_ID},
                {"columnId": "3", "id": NMA_ID},
                {"columnId": "4", "id": NTF_ID},
            ]
        },
        "dimension": DIMENSION,
        "settings": {
            "countRepeatInstances": True,
            "includeAnnotations": True,
            "limit": limit,
            "page": page,
            "nonesBehavior": "return-nones"
        }
    }

def parse_rows_to_df(rows: List[Dict[str, Any]], day_label: str, ch_name: str) -> pd.DataFrame:
    cleaned = clean_channel_name(ch_name)
    out = []
    for r in rows or []:
        vals = r.get("data", [])
        out.append({
            "Date": day_label,
            "DMA": r.get("value"),
            "Last_Touch_Channel": cleaned,
            "Visits": vals[0] if len(vals) > 0 else None,
            "Orders": vals[1] if len(vals) > 1 else None,
            "Demand": vals[2] if len(vals) > 2 else None,
            "NMA": vals[3] if len(vals) > 3 else None,
            "NTF": vals[4] if len(vals) > 4 else None,
        })
    return pd.DataFrame(out, columns=OUTPUT_COLUMNS)

def maybe_print_debug_sample(rsid: str, segment_id: str, day_label: str, payload: Dict[str, Any], data: Dict[str, Any]) -> None:
    global _debug_sample_count

    if not DEBUG_SAMPLE_ROWS:
        return

    with _debug_sample_lock:
        if _debug_sample_count >= DEBUG_MAX_SAMPLES:
            return
        _debug_sample_count += 1

    rows = data.get("rows", []) or []
    first_row = rows[0] if rows else None

    print("DEBUG_SAMPLE", json.dumps({
        "rsid": rsid,
        "segment_id": segment_id,
        "day": day_label,
        "metrics": [m["id"] for m in payload["metricContainer"]["metrics"]],
        "dimension": payload["dimension"],
        "totalPages": data.get("totalPages"),
        "rowCount": len(rows),
        "firstRow": first_row,
    }, default=str))


# ================== HTTP with retries (threads) ==================

def _sleep_backoff(attempt: int, base: float = 0.75, cap: float = 20.0):
    delay = min(cap, base * (2 ** attempt) + 0.05 * attempt)
    time.sleep(delay)

def fetch_page(
    session: requests.Session,
    rsid: str,
    segment_id: str,
    day_label: str,
    day_start_iso: str,
    day_end_iso: str,
    page: int,
    limit: int,
) -> Dict[str, Any]:
    sig = payload_signature(rsid, segment_id, day_start_iso, day_end_iso, limit)
    cache_file = get_cache_file(rsid, segment_id, day_label, sig, page)

    if USE_CACHE and cache_file.exists():
        with cache_file.open("r", encoding="utf-8") as f:
            return json.load(f)

    max_attempts = 6
    attempt = 0
    payload = build_payload(rsid, segment_id, page, limit, day_start_iso, day_end_iso)

    while attempt < max_attempts:
        token = TOKEN.get_token()
        headers = build_headers(token)
        try:
            resp = session.post(REPORT_URL, headers=headers, json=payload, timeout=120)
            status = resp.status_code

            if status == 200:
                data = resp.json()

                if page == 0:
                    maybe_print_debug_sample(rsid, segment_id, day_label, payload, data)

                if USE_CACHE:
                    with cache_file.open("w", encoding="utf-8") as f:
                        json.dump(data, f)

                return data

            if status == 401:
                TOKEN.force_refresh()
                attempt += 1
                _sleep_backoff(attempt)
                continue

            if status in (429, 500, 502, 503, 504):
                ra = resp.headers.get("Retry-After")
                attempt += 1
                if ra:
                    try:
                        time.sleep(float(ra))
                    except Exception:
                        _sleep_backoff(attempt)
                else:
                    _sleep_backoff(attempt)
                continue

            raise RuntimeError(
                f"HTTP {status} for rsid={rsid}, segment={segment_id}, day={day_label}, page={page}: {resp.text}"
            )

        except requests.Timeout:
            attempt += 1
            _sleep_backoff(attempt)
        except requests.RequestException:
            attempt += 1
            _sleep_backoff(attempt)

    raise RuntimeError(
        f"Failed to fetch rsid={rsid} segment={segment_id} day={day_label} page={page} after {max_attempts} attempts"
    )


# ================== FTP upload helper ==================

def upload_to_ftp(local_path: Path, remote_dir: str, host: str, user: str, pwd: str, try_ftps: bool = True, retries: int = 3):
    def _ensure_dir(ftp, d):
        parts = [p for p in d.split("/") if p]
        for p in parts:
            try:
                ftp.cwd(p)
            except Exception:
                ftp.mkd(p)
                ftp.cwd(p)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            if try_ftps:
                ftp = ftplib.FTP_TLS(timeout=60)
                ftp.connect(host, 21)
                ftp.login(user=user, passwd=pwd)
                ftp.prot_p()
            else:
                ftp = ftplib.FTP(timeout=60)
                ftp.connect(host, 21)
                ftp.login(user=user, passwd=pwd)

            ftp.set_pasv(True)

            if remote_dir and remote_dir != "/":
                try:
                    ftp.cwd("/")
                except Exception:
                    pass
                _ensure_dir(ftp, remote_dir)

            remote_name = local_path.name
            with open(local_path, "rb") as fh:
                ftp.storbinary(f"STOR {remote_name}", fh)

            try:
                ftp.quit()
            except Exception:
                pass

            print(f"✅ FTP upload complete: {host}:{remote_dir}/{remote_name}")
            return
        except Exception as e:
            last_err = e
            print(f"[FTP attempt {attempt}/{retries}] {type(e).__name__}: {e}")
            try_ftps = False
            time.sleep(min(10, 2 * attempt))
    raise RuntimeError(f"FTP upload failed after {retries} attempts: {last_err}")


# ================== Orchestration ==================

def get_first_and_total_pages(
    session: requests.Session,
    rsid: str,
    segment_id: str,
    day_label: str,
    day_start_iso: str,
    day_end_iso: str,
) -> Tuple[Dict[str, Any], int]:
    first = fetch_page(session, rsid, segment_id, day_label, day_start_iso, day_end_iso, 0, PAGE_SIZE)
    total_pages = int(first.get("totalPages", 1))
    return first, total_pages

def fetch_all_pages_for_combo_day(
    rsid: str,
    segment_id: str,
    channel_name: str,
    day_start_iso: str,
    day_end_iso: str,
    day_label: str,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    with requests.Session() as session:
        first, total_pages = get_first_and_total_pages(session, rsid, segment_id, day_label, day_start_iso, day_end_iso)
        frames.append(parse_rows_to_df(first.get("rows", []), day_label, channel_name))

        needed = list(range(1, total_pages))

        if needed:
            pbar = tqdm(
                total=len(needed),
                desc=f"[{rsid} | {clean_channel_name(channel_name)} | {day_label}] pages 1..{total_pages-1}",
                leave=False
            )
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {
                    pool.submit(fetch_page, session, rsid, segment_id, day_label, day_start_iso, day_end_iso, p, PAGE_SIZE): p
                    for p in needed
                }
                for fut in as_completed(futures):
                    data = fut.result()
                    frames.append(parse_rows_to_df(data.get("rows", []), day_label, channel_name))
                    pbar.update(1)
            pbar.close()

    if frames:
        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    return pd.DataFrame(columns=OUTPUT_COLUMNS)

def main():
    problems: List[str] = []
    had_any_sheet = False

    if CLEAR_WW_CACHE_ON_START:
        clear_cache_for_rsids(BRAND_RSIDS)

    week_end_inclusive = (END_DATE_D_EXCL - timedelta(days=1)).strftime("%Y-%m-%d")

    fywk = ""
    if FISCAL_YEAR is not None and FISCAL_WEEK is not None:
        fywk = f"FY{FISCAL_YEAR}_WK{FISCAL_WEEK:02d}_"

    out_path = OUTPUT_DIR / f"adobe_dma_by_segment_WW_{fywk}{START_D}_to_{week_end_inclusive}.xlsx"

    brand_pbar = tqdm(total=len(BRAND_RSIDS), desc="Brands", leave=True)

    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
        for rsid in BRAND_RSIDS:
            try:
                seg_ids = list(SEGMENTS.keys())
                seg_pbar = tqdm(total=len(seg_ids), desc=f"Segments ({rsid})", leave=False)

                frames_for_brand: List[pd.DataFrame] = []

                for seg_id in seg_ids:
                    ch_name = SEGMENTS[seg_id]

                    day_pbar = tqdm(total=len(DAY_RANGES), desc=f"Days ({clean_channel_name(ch_name)})", leave=False)
                    try:
                        for day_start_iso, day_end_iso, day_label in DAY_RANGES:
                            try:
                                df_day = fetch_all_pages_for_combo_day(
                                    rsid=rsid,
                                    segment_id=seg_id,
                                    channel_name=ch_name,
                                    day_start_iso=day_start_iso,
                                    day_end_iso=day_end_iso,
                                    day_label=day_label,
                                )
                                if not df_day.empty:
                                    frames_for_brand.append(df_day)
                            except Exception as e:
                                problems.append(f"{rsid} | {seg_id} | {day_label}: {e}")
                            finally:
                                day_pbar.update(1)
                    finally:
                        day_pbar.close()
                        seg_pbar.update(1)

                seg_pbar.close()

                if frames_for_brand:
                    df_brand = pd.concat(frames_for_brand, ignore_index=True)
                    df_brand = df_brand[OUTPUT_COLUMNS]

                    df_brand.sort_values(by=["Date", "Last_Touch_Channel", "DMA"], inplace=True, kind="mergesort")

                    sheet_name = rsid[:31]
                    df_brand.to_excel(writer, sheet_name=sheet_name, index=False)
                    had_any_sheet = True
                else:
                    problems.append(f"{rsid}: no data collected for any segment/day.")

            except Exception as e:
                problems.append(f"{rsid}: {e}")

            brand_pbar.update(1)

        if not had_any_sheet:
            pd.DataFrame(columns=OUTPUT_COLUMNS).to_excel(writer, sheet_name="Summary", index=False)
            if problems:
                pd.DataFrame({"Issues": problems}).to_excel(writer, sheet_name="Errors", index=False)

    brand_pbar.close()

    with open(OUTPUT_DIR / "latest.txt", "w", encoding="utf-8") as f:
        f.write(str(out_path.resolve()))

    print(f"\n✅ Done. Wrote {out_path}")
    if problems:
        print("Some items had issues:")
        for p in problems:
            print(" -", p)

    print("• Columns:", ", ".join(OUTPUT_COLUMNS))
    print(f"• Week window (exclusive end in AA): {START_D} to {END_D}")
    print(f"• Filename window (inclusive end): {START_D} to {week_end_inclusive}")
    print(f"• USE_CACHE={USE_CACHE}")
    print(f"• CLEAR_WW_CACHE_ON_START={CLEAR_WW_CACHE_ON_START}")
    print(f"• CACHE_NAMESPACE={CACHE_NAMESPACE}")

    if FISCAL_YEAR is not None and FISCAL_WEEK is not None:
        print(f"• Fiscal tag: FY{FISCAL_YEAR} WK{FISCAL_WEEK:02d}")
        if FISCAL_WEEK1_START_USED is not None:
            print(f"  - FY{FISCAL_YEAR} week1 start anchor: {FISCAL_WEEK1_START_USED}")
    else:
        anchors_str = ", ".join([f"FY{fy}={d}" for fy, d in FISCAL_ANCHORS_LIST])
        print(f"• Fiscal tag: (not labeled — no fiscal anchor applies). Anchors: {anchors_str}")

    if FTP_HOST and FTP_USER and FTP_PASS:
        try:
            upload_to_ftp(out_path, FTP_DIR, FTP_HOST, FTP_USER, FTP_PASS, try_ftps=FTP_TLS, retries=3)
        except Exception as e:
            print(f"[WARN] FTP upload failed: {e}")
    else:
        print("ℹ️ FTP upload skipped (FTP_HOST/FTP_USER/FTP_PASS not set).")


if __name__ == "__main__":
    if not REPORT_URL:
        raise SystemExit("AA_COMPANY_ID is not set; REPORT_URL cannot be built.")
    if not (AA_CLIENT_ID and AA_CLIENT_SECRET and AA_ORG_ID and AA_API_KEY and AA_COMPANY_ID):
        raise SystemExit(
            "Missing one or more Adobe API env vars: AA_CLIENT_ID, AA_CLIENT_SECRET, AA_ORG_ID, AA_API_KEY, AA_COMPANY_ID."
        )
    main()
