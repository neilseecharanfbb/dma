#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Adobe Analytics API 2.0 - Weekly DMA x Last Touch Channel (Segments) per RSID
KS-aligned behavior:
- ONE weekly XLSX file
- Data is pulled per DAY (Date column is YYYY-MM-DD)
- File name pattern matches KS:
    adobe_dma_by_segment_KS_<weekStartYYYY-MM-DD>_to_<weekEndYYYY-MM-DD>.xlsx
  where weekEnd is SATURDAY (inclusive)
- Fast via ThreadPoolExecutor
- Resumable per-page cache (day-specific):
    .aa_cache/<rsid>/<segment_id>/<YYYY-MM-DD>/page_XXXXX.json
- Excel output (one sheet per RSID) with columns:
  Date, DMA, Last_Touch_Channel, Visits, Orders, Demand, NMA, NTF

Date behavior:
- Uses START_DATE/END_DATE from env if provided (ISO 8601 with ms),
  otherwise auto-computes the prior Sunday->Sunday (exclusive) in America/New_York.
- Expands that range into individual days and queries each day separately.
"""

import os
import time
import json
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

# Adobe credentials (MANDATORY via env in GitHub Actions)
AA_CLIENT_ID = os.environ.get("AA_CLIENT_ID", "")
AA_CLIENT_SECRET = os.environ.get("AA_CLIENT_SECRET", "")
AA_API_KEY = os.environ.get("AA_API_KEY", AA_CLIENT_ID or "")
AA_ORG_ID = os.environ.get("AA_ORG_ID", "")
AA_COMPANY_ID = os.environ.get("AA_COMPANY_ID", "")

AUTH_URL = "https://ims-na1.adobelogin.com/ims/token/v3"
REPORT_URL = f"https://analytics.adobe.io/api/{AA_COMPANY_ID}/reports" if AA_COMPANY_ID else ""

# RSIDs / brands to run (env override BRAND_RSIDS as comma-separated list)
DEFAULT_BRAND_RSIDS = [
    "vrs_ospgro1_womanwithin",
]
BRAND_RSIDS = [s.strip() for s in os.environ.get("BRAND_RSIDS", "").split(",") if s.strip()] or DEFAULT_BRAND_RSIDS

# Segment mapping (segment_id -> Channel with '(WW)' suffix to be stripped)
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
NMA_ID    = "metrics/event233"
NTF_ID    = "metrics/event263"
DIMENSION = "variables/geodma"

# Performance
PAGE_SIZE   = int(os.environ.get("PAGE_SIZE", "2000"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))

# Output
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
CACHE_DIR = Path(".aa_cache")
CACHE_DIR.mkdir(exist_ok=True, parents=True)

# Required output columns in exact order
OUTPUT_COLUMNS = ["Date", "DMA", "Last_Touch_Channel", "Visits", "Orders", "Demand", "NMA", "NTF"]

# OAuth scopes
OAUTH_SCOPES = "openid,AdobeID,read_organizations,additional_info.projectedProductContext,session"

# -------- FTP config (from env) --------
FTP_HOST = os.environ.get("FTP_HOST", "")
FTP_USER = os.environ.get("FTP_USER", "")
FTP_PASS = os.environ.get("FTP_PASS", "")
FTP_DIR  = os.environ.get("FTP_DIR", "/")  # remote folder (default root)
FTP_TLS  = os.environ.get("FTP_TLS", "true").lower() in {"1", "true", "yes"}  # try FTPS first


# ================== Date range helpers ==================

def prior_week_sun_to_sun_iso() -> Tuple[str, str, str, str]:
    """
    Compute prior Sunday->Sunday (exclusive) as an AA dateRange:
    start = prior Sunday 00:00:00.000
    end   = last Sunday  00:00:00.000 (exclusive)
    Return (start_iso, end_iso, start_date_str, end_date_str)
    """
    tz = ZoneInfo("America/New_York")
    now = datetime.now(tz)
    # Python Mon=0..Sun=6
    days_since_sunday = (now.weekday() + 1) % 7
    last_sunday = (now - timedelta(days=days_since_sunday)).date()
    start = last_sunday - timedelta(days=7)
    end = last_sunday
    start_iso = f"{start}T00:00:00.000"
    end_iso = f"{end}T00:00:00.000"
    return start_iso, end_iso, str(start), str(end)

def iso_to_date(iso_str: str) -> date:
    return date.fromisoformat(iso_str[:10])

def iter_days(start_d: date, end_d_exclusive: date) -> List[Tuple[str, str, str]]:
    """
    Returns list of (day_start_iso, day_end_iso, day_label_YYYY-MM-DD)
    for each day in [start_d, end_d_exclusive).
    """
    days: List[Tuple[str, str, str]] = []
    cur = start_d
    while cur < end_d_exclusive:
        nxt = cur + timedelta(days=1)
        days.append((f"{cur}T00:00:00.000", f"{nxt}T00:00:00.000", str(cur)))
        cur = nxt
    return days

START_DATE = os.environ.get("START_DATE")
END_DATE = os.environ.get("END_DATE")

if not START_DATE or not END_DATE:
    START_DATE, END_DATE, START_D, END_D = prior_week_sun_to_sun_iso()
else:
    START_D, END_D = START_DATE[:10], END_DATE[:10]

START_DATE_D = iso_to_date(START_DATE)
END_DATE_D_EXCL = iso_to_date(END_DATE)

DAY_RANGES = iter_days(START_DATE_D, END_DATE_D_EXCL)  # [(day_start_iso, day_end_iso, day_label), ...]


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
            "NMA":    vals[3] if len(vals) > 3 else None,
            "NTF":    vals[4] if len(vals) > 4 else None,
        })
    return pd.DataFrame(out, columns=OUTPUT_COLUMNS)


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
    # Cache is day-specific so daily pulls don't collide
    seg_day_dir = CACHE_DIR / rsid / segment_id / day_label
    seg_day_dir.mkdir(parents=True, exist_ok=True)
    cache_file = seg_day_dir / f"page_{page:05d}.json"

    if cache_file.exists():
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
    """
    Uploads a file to FTP/FTPS. Tries FTPS first (explicit TLS on port 21), falls back to plain FTP if FTPS fails.
    Uses passive mode and creates remote_dir if it doesn't exist.
    """
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
                ftp.prot_p()  # secure data channel
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
            try_ftps = False  # next attempt: plain FTP
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
    seg_day_dir = CACHE_DIR / rsid / segment_id / day_label
    seg_day_dir.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        first, total_pages = get_first_and_total_pages(session, rsid, segment_id, day_label, day_start_iso, day_end_iso)
        frames.append(parse_rows_to_df(first.get("rows", []), day_label, channel_name))

        cached_pages = {
            int(p.stem.split("_")[-1]) for p in seg_day_dir.glob("page_*.json")
            if p.stem.split("_")[-1].isdigit()
        }
        needed = [p for p in range(1, total_pages) if p not in cached_pages]

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

    # ---------- KS-style weekly filename pattern ----------
    # KS example: adobe_dma_by_segment_KS_2025-12-28_to_2026-01-03.xlsx
    week_end_inclusive = (END_DATE_D_EXCL - timedelta(days=1)).strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"adobe_dma_by_segment_WW_{START_D}_to_{week_end_inclusive}.xlsx"

    brand_pbar = tqdm(total=len(BRAND_RSIDS), desc="Brands", leave=True)

    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
        for rsid in BRAND_RSIDS:
            try:
                seg_ids = list(SEGMENTS.keys())
                seg_pbar = tqdm(total=len(seg_ids), desc=f"Segments ({rsid})", leave=False)

                frames_for_brand: List[pd.DataFrame] = []

                for seg_id in seg_ids:
                    ch_name = SEGMENTS[seg_id]

                    # Day-level loop (daily rows)
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

                    # Stable sort for readability
                    df_brand.sort_values(by=["Date", "Last_Touch_Channel", "DMA"], inplace=True, kind="mergesort")

                    sheet_name = rsid[:31]
                    df_brand.to_excel(writer, sheet_name=sheet_name, index=False)
                    had_any_sheet = True
                else:
                    problems.append(f"{rsid}: no data collected for any segment/day.")

            except Exception as e:
                problems.append(f"{rsid}: {e}")

            brand_pbar.update(1)

        # Ensure file is valid even if empty
        if not had_any_sheet:
            pd.DataFrame(columns=OUTPUT_COLUMNS).to_excel(writer, sheet_name="Summary", index=False)
            if problems:
                pd.DataFrame({"Issues": problems}).to_excel(writer, sheet_name="Errors", index=False)

    brand_pbar.close()

    # Write path for the workflow
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    with open(OUTPUT_DIR / "latest.txt", "w", encoding="utf-8") as f:
        f.write(str(out_path.resolve()))

    print(f"\n✅ Done. Wrote {out_path}")
    if problems:
        print("Some items had issues:")
        for p in problems:
            print(" -", p)

    print("• Resumable cache: .aa_cache/<rsid>/<segment_id>/<YYYY-MM-DD>/page_XXXXX.json")
    print("• Columns:", ", ".join(OUTPUT_COLUMNS))
    print(f"• Week window (exclusive end in AA): {START_D} to {END_D}")
    print(f"• Filename window (inclusive end): {START_D} to {week_end_inclusive}")

    # --- FTP upload if env vars are present ---
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
