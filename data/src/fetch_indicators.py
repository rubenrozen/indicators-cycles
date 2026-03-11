"""
fetch_indicators.py
====================
Fetches economic indicators from:
  - FRED (Federal Reserve St. Louis) → requires FRED_API_KEY env var
  - World Bank API                   → no key required
  - OECD API                         → no key required
  - Eurostat API                     → no key required
  - Yahoo Finance (yfinance)         → no key required

Runs daily at 19h via GitHub Actions.
Only writes indicators.json if at least one value has changed.
"""

import os
import json
import time
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
FRED_API_KEY  = os.environ.get("FRED_API_KEY", "")
DATA_PATH     = Path(__file__).parent.parent / "data" / "indicators.json"
FRED_BASE     = "https://api.stlouisfed.org/fred/series/observations"
WB_BASE       = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&mrv=2"
OECD_BASE     = "https://stats.oecd.org/SDMX-JSON/data/{dataset}/{filter}/all?contentType=csv&startTime={start}"

TIMEOUT = 20  # seconds per request

# ── FRED Series Map ───────────────────────────────────────────────────────────
FRED_SERIES = {
    # Leading
    "T10Y2Y":       {"name": "Yield Curve 10Y-2Y",              "unit": "%",   "category": "leading",   "region": "US"},
    "UMCSENT":      {"name": "Consumer Sentiment (Univ. Mich.)", "unit": "idx", "category": "leading",   "region": "US"},
    "ICSA":         {"name": "Initial Jobless Claims",           "unit": "#",   "category": "leading",   "region": "US"},
    "M2SL":         {"name": "M2 Money Supply",                  "unit": "B$",  "category": "leading",   "region": "US"},
    "PERMIT":       {"name": "Building Permits",                 "unit": "K",   "category": "leading",   "region": "US"},
    "NEWORDER":     {"name": "Mfg. New Orders",                  "unit": "B$",  "category": "leading",   "region": "US"},
    "BAMLH0A0HYM2": {"name": "HY Credit Spread (OAS)",           "unit": "%",   "category": "leading",   "region": "US"},
    "MICH":         {"name": "Inflation Expectations (1Y)",      "unit": "%",   "category": "leading",   "region": "US"},
    "DTWEXBGS":     {"name": "USD Trade-Weighted Index",         "unit": "idx", "category": "leading",   "region": "US"},
    "ACOGNO":       {"name": "New Orders Non-Defense Cap Goods", "unit": "B$",  "category": "leading",   "region": "US"},
    # Coincident
    "INDPRO":       {"name": "Industrial Production",            "unit": "idx", "category": "coincident","region": "US"},
    "PAYEMS":       {"name": "Nonfarm Payrolls",                 "unit": "K",   "category": "coincident","region": "US"},
    "RSXFS":        {"name": "Retail Sales ex-Food",             "unit": "B$",  "category": "coincident","region": "US"},
    "PI":           {"name": "Personal Income",                  "unit": "B$",  "category": "coincident","region": "US"},
    "TCU":          {"name": "Capacity Utilization",             "unit": "%",   "category": "coincident","region": "US"},
    # Lagging
    "CPIAUCSL":     {"name": "CPI (YoY)",                        "unit": "%",   "category": "lagging",   "region": "US"},
    "CPILFESL":     {"name": "Core CPI (YoY)",                   "unit": "%",   "category": "lagging",   "region": "US"},
    "UNRATE":       {"name": "Unemployment Rate",                "unit": "%",   "category": "lagging",   "region": "US"},
    "FEDFUNDS":     {"name": "Fed Funds Rate",                   "unit": "%",   "category": "lagging",   "region": "US"},
    "TOTCI":        {"name": "C&I Loans",                        "unit": "B$",  "category": "lagging",   "region": "US"},
    "MPRIME":       {"name": "Prime Rate",                       "unit": "%",   "category": "lagging",   "region": "US"},
}

# ── World Bank Indicators ────────────────────────────────────────────────────
WB_SERIES = {
    "EU": {
        "NY.GDP.MKTP.KD.ZG":  {"id": "EU_GDP",     "name": "GDP Growth (YoY)",             "unit": "%",   "category": "coincident"},
        "NE.TRD.GNFS.ZS":     {"id": "EU_TRADE",   "name": "Trade (% of GDP)",             "unit": "%",   "category": "coincident"},
        "FP.CPI.TOTL.ZG":     {"id": "EU_CPI_WB",  "name": "CPI Inflation (WB)",           "unit": "%",   "category": "lagging"},
        "SL.UEM.TOTL.ZS":     {"id": "EU_UNEMP_WB","name": "Unemployment (WB)",            "unit": "%",   "category": "lagging"},
    },
    "CN": {
        "NY.GDP.MKTP.KD.ZG":  {"id": "CN_GDP",     "name": "GDP Growth (YoY)",             "unit": "%",   "category": "coincident"},
        "NV.IND.TOTL.KD.ZG":  {"id": "CN_IND",     "name": "Industry Value Added (YoY)",   "unit": "%",   "category": "coincident"},
        "FP.CPI.TOTL.ZG":     {"id": "CN_CPI_WB",  "name": "CPI Inflation (WB)",           "unit": "%",   "category": "lagging"},
    },
    "JP": {
        "NY.GDP.MKTP.KD.ZG":  {"id": "JP_GDP",     "name": "GDP Growth (YoY)",             "unit": "%",   "category": "coincident"},
        "FP.CPI.TOTL.ZG":     {"id": "JP_CPI_WB",  "name": "CPI Inflation (WB)",           "unit": "%",   "category": "lagging"},
    },
    "GB": {
        "NY.GDP.MKTP.KD.ZG":  {"id": "UK_GDP_WB",  "name": "GDP Growth (YoY)",             "unit": "%",   "category": "coincident"},
        "FP.CPI.TOTL.ZG":     {"id": "UK_CPI_WB",  "name": "CPI Inflation (WB)",           "unit": "%",   "category": "lagging"},
    },
}

WB_COUNTRY_MAP = {"EU": "EMU", "CN": "CN", "JP": "JP", "UK": "GB"}


# ── Helpers ──────────────────────────────────────────────────────────────────

def safe_get(url, params=None, retries=3):
    """HTTP GET with retry logic."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def pct_change(new_val, old_val):
    """Safe percentage change."""
    if old_val is None or old_val == 0:
        return 0.0
    return round((new_val - old_val) / abs(old_val) * 100, 2)


def load_existing():
    """Load existing indicators.json."""
    if DATA_PATH.exists():
        with open(DATA_PATH) as f:
            return json.load(f)
    return {}


def fingerprint(data):
    """MD5 hash of the data (to detect changes)."""
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


# ── FRED Fetcher ─────────────────────────────────────────────────────────────

def fetch_fred(series_id):
    """Fetch latest 2 observations from FRED."""
    if not FRED_API_KEY:
        log.warning("FRED_API_KEY not set, skipping FRED fetch")
        return None, None, None

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 10,  # grab 10 to skip "." values
        "observation_start": (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
    }
    r = safe_get(FRED_BASE, params=params)
    if r is None:
        return None, None, None

    obs = [o for o in r.json().get("observations", []) if o["value"] != "."]
    if len(obs) < 1:
        return None, None, None

    latest   = float(obs[0]["value"])
    date_str = obs[0]["date"]
    previous = float(obs[1]["value"]) if len(obs) >= 2 else latest
    return latest, previous, date_str


def build_us_indicators(existing_indicators):
    """Build US indicators dict from FRED."""
    result = {"leading": [], "coincident": [], "lagging": []}

    # Build a lookup from existing data for fallback
    existing_lookup = {}
    for cat in ("leading", "coincident", "lagging"):
        for ind in existing_indicators.get(cat, []):
            existing_lookup[ind["id"]] = ind

    for series_id, meta in FRED_SERIES.items():
        log.info(f"Fetching FRED {series_id}…")
        val, prev, date = fetch_fred(series_id)

        if val is None:
            # Fallback to existing data
            existing = existing_lookup.get(series_id)
            if existing:
                result[meta["category"]].append(existing)
            log.warning(f"  → Using cached value for {series_id}")
            continue

        entry = {
            "id": series_id,
            "name": meta["name"],
            "value": round(val, 4),
            "previous": round(prev, 4),
            "date": date,
            "unit": meta["unit"],
            "source": "FRED",
            "category": meta["category"],
        }
        result[meta["category"]].append(entry)
        log.info(f"  → {val} (prev {prev}) on {date}")
        time.sleep(0.1)  # gentle rate limiting

    return result


# ── World Bank Fetcher ───────────────────────────────────────────────────────

def fetch_worldbank(country_code, wb_indicator):
    """Fetch latest World Bank observation."""
    url = WB_BASE.format(country=country_code, indicator=wb_indicator)
    r = safe_get(url)
    if r is None:
        return None, None, None

    try:
        data = r.json()
        records = data[1]
        valid = [rec for rec in records if rec.get("value") is not None]
        if not valid:
            return None, None, None
        latest   = valid[0]["value"]
        date_str = valid[0].get("date", "N/A")
        previous = valid[1]["value"] if len(valid) >= 2 else latest
        return round(float(latest), 4), round(float(previous), 4), str(date_str)
    except (KeyError, IndexError, TypeError, ValueError) as e:
        log.warning(f"World Bank parse error for {country_code}/{wb_indicator}: {e}")
        return None, None, None


def build_wb_indicators(region):
    """Build World Bank indicators for a region."""
    result = {"leading": [], "coincident": [], "lagging": []}
    series_map = WB_SERIES.get(region, {})
    country_code = WB_COUNTRY_MAP.get(region, region)

    for wb_ind, meta in series_map.items():
        log.info(f"Fetching WorldBank {country_code}/{wb_ind}…")
        val, prev, date = fetch_worldbank(country_code, wb_ind)
        if val is None:
            log.warning(f"  → No data for {wb_ind}")
            continue
        entry = {
            "id": meta["id"],
            "name": meta["name"],
            "value": val,
            "previous": prev,
            "date": date,
            "unit": meta["unit"],
            "source": "WorldBank",
            "category": meta["category"],
        }
        result[meta["category"]].append(entry)
        log.info(f"  → {val} (prev {prev})")
        time.sleep(0.2)

    return result


# ── Eurostat Fetcher (key indicators via JSON API) ───────────────────────────

def fetch_eurostat_indicator(dataset, filter_str, label, unit, category):
    """Fetch a single Eurostat time series via JSON API."""
    base = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
    url  = f"{base}/{dataset}?{filter_str}&format=JSON&lang=EN"
    r = safe_get(url)
    if r is None:
        return None

    try:
        data   = r.json()
        values = list(data["value"].values())
        times  = list(data["dimension"]["time"]["category"]["label"].values())

        if not values:
            return None

        val  = round(float(values[-1]), 4)
        prev = round(float(values[-2]), 4) if len(values) >= 2 else val
        date = times[-1] if times else "N/A"

        return {"value": val, "previous": prev, "date": date, "unit": unit, "category": category, "source": "Eurostat"}
    except Exception as e:
        log.warning(f"Eurostat parse error for {dataset}: {e}")
        return None


EUROSTAT_QUERIES = [
    # (id, name, dataset, filter, unit, category)
    ("EU_CPI",       "HICP Inflation (YoY)",        "prc_hicp_manr",  "filterBy=coicop=CP00&geo=EA&unit=RCH_A",   "%",   "lagging"),
    ("EU_UNEMP",     "Unemployment Rate",            "une_rt_m",       "filterBy=geo=EA&sex=T&age=TOTAL&s_adj=SA", "%",   "lagging"),
    ("EU_IP",        "Industrial Production (YoY)",  "sts_inpr_m",     "filterBy=geo=EA&s_adj=SCA&nace_r2=B-D&unit=PCH_SM", "%", "coincident"),
    ("EU_RETAIL",    "Retail Sales (YoY)",           "sts_trtu_m",     "filterBy=geo=EA&s_adj=SCA&indic_bt=TOVV&unit=PCH_SM", "%","coincident"),
    ("EU_CONS_CONF", "Consumer Confidence",          "ei_bsco_m",      "filterBy=geo=EA&indic=BS-CSMCI",           "pts", "leading"),
    ("EU_ESI",       "Economic Sentiment Indicator", "ei_bssi_m",      "filterBy=geo=EA&indic=BS-ESI-I",           "idx", "leading"),
]


def build_eurostat_indicators():
    """Build EU indicators from Eurostat."""
    result = {"leading": [], "coincident": [], "lagging": []}

    for ind_id, name, dataset, filter_str, unit, category in EUROSTAT_QUERIES:
        log.info(f"Fetching Eurostat {dataset}…")
        data = fetch_eurostat_indicator(dataset, filter_str, name, unit, category)
        if data is None:
            log.warning(f"  → No data for {ind_id}")
            continue
        entry = {"id": ind_id, "name": name, **data}
        result[category].append(entry)
        log.info(f"  → {data['value']} on {data['date']}")
        time.sleep(0.3)

    return result


# ── yFinance Fetcher (VIX, S&P, rates) ──────────────────────────────────────

def fetch_yfinance_indicators():
    """Fetch market indicators via yfinance (no API key needed)."""
    tickers = {
        "^VIX":  {"id": "VIX",        "name": "VIX Volatility Index",  "unit": "pts", "category": "leading",   "region": "US"},
        "^GSPC": {"id": "SP500",       "name": "S&P 500",               "unit": "pts", "category": "coincident","region": "US"},
        "^TNX":  {"id": "US10Y",       "name": "US 10Y Treasury Yield", "unit": "%",   "category": "lagging",   "region": "US"},
        "DX-Y.NYB": {"id": "DXY",      "name": "US Dollar Index (DXY)", "unit": "idx", "category": "leading",   "region": "US"},
        "GC=F":  {"id": "GOLD",        "name": "Gold (USD/oz)",          "unit": "$",   "category": "leading",   "region": "US"},
        "CL=F":  {"id": "OIL_WTI",     "name": "WTI Crude Oil",         "unit": "$",   "category": "coincident","region": "US"},
    }

    results = {}
    for ticker, meta in tickers.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if hist.empty:
                continue
            val  = round(float(hist["Close"].iloc[-1]), 2)
            prev = round(float(hist["Close"].iloc[-2]), 2) if len(hist) >= 2 else val
            date = hist.index[-1].strftime("%Y-%m-%d")

            results[meta["id"]] = {
                "id": meta["id"],
                "name": meta["name"],
                "value": val,
                "previous": prev,
                "date": date,
                "unit": meta["unit"],
                "source": "yFinance",
                "category": meta["category"],
                "region": meta["region"],
            }
            log.info(f"yFinance {ticker}: {val}")
        except Exception as e:
            log.warning(f"yFinance error for {ticker}: {e}")

    return results


# ── Cycle Score Computation ──────────────────────────────────────────────────

def compute_cycle_score(indicators_dict):
    """
    Compute a composite cycle score in [-1, 1].
    Delegates to compute_cycle_score.py logic but replicated here for standalone use.
    """
    from compute_cycle_score import compute_score_from_indicators
    return compute_score_from_indicators(indicators_dict)


# ── Presidential Cycle ───────────────────────────────────────────────────────

def compute_presidential_cycle():
    """Calculate current year in US presidential term."""
    current_year = datetime.now().year
    # Trump 2nd term: Jan 2025 → Jan 2029
    term_start = 2025
    year_in_term = current_year - term_start + 1

    # Fetch S&P YTD via yfinance
    try:
        sp = yf.Ticker("^GSPC")
        hist = sp.history(start=f"{current_year}-01-01")
        if not hist.empty:
            start_price = hist["Close"].iloc[0]
            latest_price = hist["Close"].iloc[-1]
            ytd = round((latest_price - start_price) / start_price * 100, 2)
        else:
            ytd = 0.0
    except Exception:
        ytd = 0.0

    return {
        "current_president": "Trump",
        "term_start_year": term_start,
        "term_end_year": 2028,
        "current_year_in_term": year_in_term,
        "sp500_historical_avg": {"year1": 6.4, "year2": 7.0, "year3": 16.4, "year4": 6.6},
        "sp500_current_ytd": ytd,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Starting indicator fetch…")
    log.info("=" * 60)

    existing = load_existing()
    old_fingerprint = fingerprint(existing)

    # ── US ──
    log.info("\n── Fetching US (FRED) ──")
    us_indicators = build_us_indicators(
        existing.get("regions", {}).get("US", {}).get("indicators", {})
    )

    # Append yFinance data to US indicators
    yf_data = fetch_yfinance_indicators()
    for ind_id, ind in yf_data.items():
        cat = ind["category"]
        # Avoid duplicates
        existing_ids = [i["id"] for i in us_indicators.get(cat, [])]
        if ind_id not in existing_ids:
            us_indicators.setdefault(cat, []).append(ind)

    # ── EU ──
    log.info("\n── Fetching EU (Eurostat + WorldBank) ──")
    eu_indicators = build_eurostat_indicators()
    eu_wb = build_wb_indicators("EU")
    for cat in ("leading", "coincident", "lagging"):
        eu_indicators[cat].extend(eu_wb.get(cat, []))

    # ── CN / JP / UK ──
    log.info("\n── Fetching CN / JP / UK (WorldBank) ──")
    cn_indicators = build_wb_indicators("CN")
    jp_indicators = build_wb_indicators("JP")
    uk_indicators = build_wb_indicators("UK")

    # ── Compute cycle scores ──
    us_score, us_angle, us_phase = compute_cycle_score(us_indicators)
    eu_score, eu_angle, eu_phase = compute_cycle_score(eu_indicators)
    cn_score, cn_angle, cn_phase = compute_cycle_score(cn_indicators)
    jp_score, jp_angle, jp_phase = compute_cycle_score(jp_indicators)
    uk_score, uk_angle, uk_phase = compute_cycle_score(uk_indicators)

    # ── Presidential cycle ──
    presidential = compute_presidential_cycle()

    # ── Seasonal ──
    now = datetime.now(timezone.utc)
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    monthly_avgs = {
        "Jan": 1.1, "Feb": 0.1, "Mar": 1.2, "Apr": 1.5,
        "May": 0.2, "Jun": 0.6, "Jul": 1.8, "Aug": 0.1,
        "Sep": -0.8, "Oct": 0.9, "Nov": 1.7, "Dec": 1.5,
    }
    current_month_name = months[now.month - 1]

    # ── Assemble final payload ──
    payload = {
        "meta": {
            "last_updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "version": "1.0",
        },
        "regions": {
            "US": {"cycle_score": us_score, "cycle_angle_deg": us_angle, "cycle_phase": us_phase, "indicators": us_indicators},
            "EU": {"cycle_score": eu_score, "cycle_angle_deg": eu_angle, "cycle_phase": eu_phase, "indicators": eu_indicators},
            "CN": {"cycle_score": cn_score, "cycle_angle_deg": cn_angle, "cycle_phase": cn_phase, "indicators": cn_indicators},
            "JP": {"cycle_score": jp_score, "cycle_angle_deg": jp_angle, "cycle_phase": jp_phase, "indicators": jp_indicators},
            "UK": {"cycle_score": uk_score, "cycle_angle_deg": uk_angle, "cycle_phase": uk_phase, "indicators": uk_indicators},
        },
        "cycles": {
            "presidential": presidential,
            "kondratieff": existing.get("cycles", {}).get("kondratieff", {"current_season": "Winter", "season_start_year": 2000, "season_end_year_est": 2035, "description": "Deflation/debt reset phase.", "position_pct": 72}),
            "juglar": existing.get("cycles", {}).get("juglar", {"last_trough": 2020, "cycle_length_avg": 9, "current_year_in_cycle": 6, "phase": "Late Expansion", "position_pct": 66}),
            "kitchin": existing.get("cycles", {}).get("kitchin", {"last_trough": 2023, "cycle_length_avg": 4, "current_year_in_cycle": 3, "phase": "Inventory Build", "position_pct": 75}),
            "kuznets": existing.get("cycles", {}).get("kuznets", {"last_trough": 2012, "cycle_length_avg": 18, "current_year_in_cycle": 14, "phase": "Late Expansion", "position_pct": 77}),
            "credit": {
                "phase": "Tightening Plateau",
                "hy_spread": yf_data.get("HY_SPREAD", {}).get("value", existing.get("cycles", {}).get("credit", {}).get("hy_spread", 3.45)),
                "hy_spread_avg": 4.2,
                "lending_standards_net_pct": existing.get("cycles", {}).get("credit", {}).get("lending_standards_net_pct", -12.4),
                "position_pct": 65,
            },
            "commodity": {
                "phase": "Recovery",
                "crb_index": existing.get("cycles", {}).get("commodity", {}).get("crb_index", 281.4),
                "crb_change_ytd": existing.get("cycles", {}).get("commodity", {}).get("crb_change_ytd", 4.2),
                "position_pct": 42,
            },
            "real_estate": existing.get("cycles", {}).get("real_estate", {"last_trough": 2012, "cycle_length_avg": 18, "current_year_in_cycle": 14, "phase": "Late Cycle", "position_pct": 77, "case_shiller_yoy": 3.8}),
            "earnings": existing.get("cycles", {}).get("earnings", {"phase": "Expansion", "sp500_eps_growth_fwd": 11.4, "revision_ratio": 1.12, "position_pct": 62}),
            "seasonal": {
                "current_month": now.month,
                "current_month_name": current_month_name,
                "sp500_avg_return_current_month": monthly_avgs.get(current_month_name, 0.0),
                "monthly_avg_returns": monthly_avgs,
            },
            "decennial": {
                "current_year_digit": now.year % 10,
                "sp500_avg_return_year6": 8.1,
                "historical_data": {"0": -2.1, "1": 14.8, "2": -5.1, "3": 24.1, "4": 12.4, "5": 28.6, "6": 8.1, "7": 16.2, "8": 3.8, "9": 15.4},
            },
            "fed_rate": {
                "phase": "Cutting" if existing.get("regions", {}).get("US", {}).get("indicators", {}).get("lagging", [{}])[0].get("value", 4.25) < 4.5 else "Hiking",
                "current_rate": next((i["value"] for i in existing.get("regions", {}).get("US", {}).get("indicators", {}).get("lagging", []) if i["id"] == "FEDFUNDS"), 4.25),
                "cycle_peak": 5.50,
                "cycle_trough_est": 3.00,
                "cuts_delivered": 5,
                "cuts_remaining_est": 5,
                "position_pct": 50,
            },
        },
    }

    new_fingerprint = fingerprint(payload)

    if new_fingerprint == old_fingerprint:
        log.info("\n✅ No changes detected — indicators.json not updated.")
        return

    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    log.info(f"\n✅ indicators.json updated → {DATA_PATH}")


if __name__ == "__main__":
    main()
