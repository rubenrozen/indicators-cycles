"""
fetch_indicators.py
====================
Fetches economic indicators from:
  - FRED  → requires FRED_API_KEY env var
  - World Bank API → no key
  - Eurostat API   → no key
  - Yahoo Finance  → no key

KEY DESIGN: "merge-with-existing" strategy.
For every region/category, if the fresh fetch returns fewer items than
what is already stored, we keep the stored (cached) values.
A partial API failure never wipes out working indicators.
"""

import os, json, time, hashlib, logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DATA_PATH    = Path(__file__).parent.parent / "data" / "indicators.json"
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
WB_BASE      = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&mrv=2"
TIMEOUT      = 20

# ── FRED Series ───────────────────────────────────────────────────────────────
FRED_SERIES = {
    "T10Y2Y":       {"name": "Yield Curve 10Y-2Y",              "unit": "%",   "category": "leading"},
    "UMCSENT":      {"name": "Consumer Sentiment (Univ. Mich.)", "unit": "idx", "category": "leading"},
    "ICSA":         {"name": "Initial Jobless Claims",           "unit": "#",   "category": "leading"},
    "M2SL":         {"name": "M2 Money Supply",                  "unit": "B$",  "category": "leading"},
    "PERMIT":       {"name": "Building Permits",                 "unit": "K",   "category": "leading"},
    "NEWORDER":     {"name": "Mfg. New Orders",                  "unit": "B$",  "category": "leading"},
    "BAMLH0A0HYM2": {"name": "HY Credit Spread (OAS)",           "unit": "%",   "category": "leading"},
    "MICH":         {"name": "Inflation Expectations (1Y)",      "unit": "%",   "category": "leading"},
    "DTWEXBGS":     {"name": "USD Trade-Weighted Index",         "unit": "idx", "category": "leading"},
    "ACOGNO":       {"name": "New Orders Non-Defense Cap Goods", "unit": "B$",  "category": "leading"},
    "INDPRO":       {"name": "Industrial Production",            "unit": "idx", "category": "coincident"},
    "PAYEMS":       {"name": "Nonfarm Payrolls",                 "unit": "K",   "category": "coincident"},
    "RSXFS":        {"name": "Retail Sales ex-Food",             "unit": "B$",  "category": "coincident"},
    "PI":           {"name": "Personal Income",                  "unit": "B$",  "category": "coincident"},
    "TCU":          {"name": "Capacity Utilization",             "unit": "%",   "category": "coincident"},
    "CPIAUCSL":     {"name": "CPI (YoY)",                        "unit": "%",   "category": "lagging"},
    "CPILFESL":     {"name": "Core CPI (YoY)",                   "unit": "%",   "category": "lagging"},
    "UNRATE":       {"name": "Unemployment Rate",                "unit": "%",   "category": "lagging"},
    "FEDFUNDS":     {"name": "Fed Funds Rate",                   "unit": "%",   "category": "lagging"},
    "TOTCI":        {"name": "C&I Loans",                        "unit": "B$",  "category": "lagging"},
    "MPRIME":       {"name": "Prime Rate",                       "unit": "%",   "category": "lagging"},
}

# ── World Bank: only reliable annual indicators ───────────────────────────────
WB_SERIES = {
    "EU": {
        "NY.GDP.MKTP.KD.ZG": {"id": "EU_GDP",    "name": "GDP Growth (YoY)",        "unit": "%", "category": "coincident"},
        "FP.CPI.TOTL.ZG":    {"id": "EU_CPI_WB", "name": "CPI Inflation (YoY)",     "unit": "%", "category": "lagging"},
        "SL.UEM.TOTL.ZS":    {"id": "EU_UNEMP",  "name": "Unemployment Rate",        "unit": "%", "category": "lagging"},
    },
    "CN": {
        "NY.GDP.MKTP.KD.ZG": {"id": "CN_GDP",    "name": "GDP Growth (YoY)",         "unit": "%", "category": "coincident"},
        "NV.IND.TOTL.KD.ZG": {"id": "CN_IND",    "name": "Industry Value Added (YoY)","unit": "%","category": "coincident"},
        "FP.CPI.TOTL.ZG":    {"id": "CN_CPI_WB", "name": "CPI Inflation (YoY)",      "unit": "%", "category": "lagging"},
    },
    "JP": {
        "NY.GDP.MKTP.KD.ZG": {"id": "JP_GDP",    "name": "GDP Growth (YoY)",          "unit": "%", "category": "coincident"},
        "FP.CPI.TOTL.ZG":    {"id": "JP_CPI_WB", "name": "CPI Inflation (YoY)",       "unit": "%", "category": "lagging"},
        "SL.UEM.TOTL.ZS":    {"id": "JP_UNEMP",  "name": "Unemployment Rate",          "unit": "%", "category": "lagging"},
    },
    "GB": {
        "NY.GDP.MKTP.KD.ZG": {"id": "UK_GDP_WB", "name": "GDP Growth (YoY)",           "unit": "%", "category": "coincident"},
        "FP.CPI.TOTL.ZG":    {"id": "UK_CPI_WB", "name": "CPI Inflation (YoY)",        "unit": "%", "category": "lagging"},
        "SL.UEM.TOTL.ZS":    {"id": "UK_UNEMP",  "name": "Unemployment Rate",           "unit": "%", "category": "lagging"},
    },
}
WB_COUNTRY_MAP = {"EU": "EMU", "CN": "CN", "JP": "JP", "UK": "GB"}

# ── Eurostat: only the 3 most reliable endpoints ──────────────────────────────
EUROSTAT_QUERIES = [
    ("EU_CPI",    "HICP Inflation (YoY)",       "prc_hicp_manr", "coicop=CP00&geo=EA&unit=RCH_A",              "%", "lagging"),
    ("EU_UNEMP2", "Unemployment Rate (EA)",      "une_rt_m",      "geo=EA&sex=T&age=TOTAL&s_adj=SA&unit=PC_ACT","%", "lagging"),
    ("EU_IP",     "Industrial Production (YoY)", "sts_inpr_m",    "geo=EA&s_adj=SCA&nace_r2=B-D&unit=PCH_SM",  "%", "coincident"),
]

# ── Utilities ─────────────────────────────────────────────────────────────────

def safe_get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning(f"  attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None

def load_existing():
    if DATA_PATH.exists():
        with open(DATA_PATH) as f:
            return json.load(f)
    return {}

def fingerprint(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

def merge_indicators(new_list, existing_list):
    """Update existing by id; add new ids; never remove existing entries."""
    by_id = {i["id"]: i for i in existing_list}
    for ind in new_list:
        by_id[ind["id"]] = ind
    return list(by_id.values())

def merge_all_cats(new_dict, existing_dict):
    result = {}
    for cat in ("leading", "coincident", "lagging"):
        result[cat] = merge_indicators(
            new_dict.get(cat, []),
            existing_dict.get(cat, [])
        )
    return result

# ── FRED ─────────────────────────────────────────────────────────────────────

def fetch_fred(series_id):
    if not FRED_API_KEY:
        return None, None, None
    r = safe_get(FRED_BASE, params={
        "series_id": series_id, "api_key": FRED_API_KEY,
        "file_type": "json", "sort_order": "desc", "limit": 10,
        "observation_start": (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
    })
    if not r:
        return None, None, None
    obs = [o for o in r.json().get("observations", []) if o["value"] != "."]
    if not obs:
        return None, None, None
    return float(obs[0]["value"]), float(obs[1]["value"]) if len(obs) >= 2 else float(obs[0]["value"]), obs[0]["date"]

def build_us_indicators(existing_region):
    fresh = {"leading": [], "coincident": [], "lagging": []}
    for sid, meta in FRED_SERIES.items():
        log.info(f"  FRED {sid}")
        val, prev, date = fetch_fred(sid)
        if val is None:
            continue
        fresh[meta["category"]].append({
            "id": sid, "name": meta["name"],
            "value": round(val, 4), "previous": round(prev, 4),
            "date": date, "unit": meta["unit"],
            "source": "FRED", "category": meta["category"],
        })
        time.sleep(0.1)
    return merge_all_cats(fresh, existing_region.get("indicators", {}))

# ── World Bank ────────────────────────────────────────────────────────────────

def fetch_worldbank(country_code, wb_indicator):
    r = safe_get(WB_BASE.format(country=country_code, indicator=wb_indicator))
    if not r:
        return None, None, None
    try:
        valid = [rec for rec in r.json()[1] if rec.get("value") is not None]
        if not valid:
            return None, None, None
        return (round(float(valid[0]["value"]), 4),
                round(float(valid[1]["value"]), 4) if len(valid) >= 2 else round(float(valid[0]["value"]), 4),
                str(valid[0].get("date", "N/A")))
    except Exception as e:
        log.warning(f"  WB parse error: {e}")
        return None, None, None

def build_wb_indicators(region, existing_ind):
    fresh   = {"leading": [], "coincident": [], "lagging": []}
    country = WB_COUNTRY_MAP.get(region, region)
    for wb_ind, meta in WB_SERIES.get(region, {}).items():
        log.info(f"  WorldBank {country}/{wb_ind}")
        val, prev, date = fetch_worldbank(country, wb_ind)
        if val is None:
            continue
        fresh[meta["category"]].append({
            "id": meta["id"], "name": meta["name"],
            "value": val, "previous": prev, "date": date,
            "unit": meta["unit"], "source": "WorldBank", "category": meta["category"],
        })
        time.sleep(0.2)
    return merge_all_cats(fresh, existing_ind)

# ── Eurostat ──────────────────────────────────────────────────────────────────

def fetch_eurostat(dataset, filter_str):
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}?{filter_str}&format=JSON&lang=EN"
    r   = safe_get(url)
    if not r:
        return None, None, None
    try:
        data   = r.json()
        values = list(data["value"].values())
        times  = list(data["dimension"]["time"]["category"]["label"].values())
        if not values:
            return None, None, None
        return (round(float(values[-1]), 4),
                round(float(values[-2]), 4) if len(values) >= 2 else round(float(values[-1]), 4),
                times[-1] if times else "N/A")
    except Exception as e:
        log.warning(f"  Eurostat parse error {dataset}: {e}")
        return None, None, None

def build_eu_indicators(existing_ind):
    fresh = {"leading": [], "coincident": [], "lagging": []}
    for ind_id, name, dataset, filter_str, unit, category in EUROSTAT_QUERIES:
        log.info(f"  Eurostat {dataset}")
        val, prev, date = fetch_eurostat(dataset, filter_str)
        if val is None:
            continue
        fresh[category].append({
            "id": ind_id, "name": name, "value": val, "previous": prev,
            "date": date, "unit": unit, "source": "Eurostat", "category": category,
        })
        time.sleep(0.3)
    # Add WB data for EU
    wb = build_wb_indicators("EU", {})
    for cat in ("leading", "coincident", "lagging"):
        fresh[cat].extend(wb.get(cat, []))
    return merge_all_cats(fresh, existing_ind)

# ── yFinance ──────────────────────────────────────────────────────────────────

def fetch_yfinance_indicators():
    tickers = {
        "^VIX":     {"id": "VIX",     "name": "VIX Volatility Index",  "unit": "pts", "category": "leading"},
        "^GSPC":    {"id": "SP500",   "name": "S&P 500",               "unit": "pts", "category": "coincident"},
        "^TNX":     {"id": "US10Y",   "name": "US 10Y Treasury Yield", "unit": "%",   "category": "lagging"},
        "DX-Y.NYB": {"id": "DXY",    "name": "US Dollar Index (DXY)",  "unit": "idx", "category": "leading"},
        "GC=F":     {"id": "GOLD",   "name": "Gold (USD/oz)",           "unit": "$",   "category": "leading"},
        "CL=F":     {"id": "OIL_WTI","name": "WTI Crude Oil",          "unit": "$",   "category": "coincident"},
    }
    results = {}
    for ticker, meta in tickers.items():
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if hist.empty:
                continue
            val  = round(float(hist["Close"].iloc[-1]), 2)
            prev = round(float(hist["Close"].iloc[-2]), 2) if len(hist) >= 2 else val
            results[meta["id"]] = {
                "id": meta["id"], "name": meta["name"],
                "value": val, "previous": prev,
                "date": hist.index[-1].strftime("%Y-%m-%d"),
                "unit": meta["unit"], "source": "yFinance", "category": meta["category"],
            }
            log.info(f"  yFinance {ticker}: {val}")
        except Exception as e:
            log.warning(f"  yFinance {ticker} failed: {e}")
    return results

# ── Helpers ───────────────────────────────────────────────────────────────────

def compute_cycle_score(indicators_dict):
    from compute_cycle_score import compute_score_from_indicators
    return compute_score_from_indicators(indicators_dict)

def compute_presidential_cycle():
    now  = datetime.now()
    term = 2025
    try:
        hist = yf.Ticker("^GSPC").history(start=f"{now.year}-01-01")
        ytd  = round((float(hist["Close"].iloc[-1]) - float(hist["Close"].iloc[0])) / float(hist["Close"].iloc[0]) * 100, 2) if not hist.empty else 0.0
    except Exception:
        ytd = 0.0
    return {"current_president": "Trump", "term_start_year": term, "term_end_year": 2028,
            "current_year_in_term": now.year - term + 1,
            "sp500_historical_avg": {"year1": 6.4, "year2": 7.0, "year3": 16.4, "year4": 6.6},
            "sp500_current_ytd": ytd}

def compute_fed_cycle(us_ind, existing):
    fedfunds = next((i["value"] for i in us_ind.get("lagging", []) if i["id"] == "FEDFUNDS"),
                    existing.get("cycles", {}).get("fed_rate", {}).get("current_rate", 4.25))
    peak      = 5.50
    trough    = 3.00
    pos       = round(min(100, max(0, (peak - fedfunds) / (peak - trough) * 100)), 1)
    phase     = "Cutting" if fedfunds < peak - 0.25 else ("Hiking" if fedfunds >= peak else "Pause")
    ex_fed    = existing.get("cycles", {}).get("fed_rate", {})
    return {"phase": phase, "current_rate": fedfunds, "cycle_peak": peak,
            "cycle_trough_est": trough, "cuts_delivered": ex_fed.get("cuts_delivered", 5),
            "cuts_remaining_est": ex_fed.get("cuts_remaining_est", 5), "position_pct": pos}

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Starting indicator fetch")
    log.info("=" * 60)

    existing   = load_existing()
    old_fp     = fingerprint(existing)
    ex_regions = existing.get("regions", {})
    ex_cycles  = existing.get("cycles", {})

    log.info("\n── US (FRED + yFinance) ──")
    us_ind  = build_us_indicators(ex_regions.get("US", {}))
    yf_data = fetch_yfinance_indicators()
    for ind_id, ind in yf_data.items():
        cat = ind["category"]
        if ind_id not in [i["id"] for i in us_ind.get(cat, [])]:
            us_ind.setdefault(cat, []).append(ind)

    log.info("\n── EU (Eurostat + WorldBank) ──")
    eu_ind = build_eu_indicators(ex_regions.get("EU", {}).get("indicators", {}))

    log.info("\n── CN / JP / UK (WorldBank) ──")
    cn_ind = build_wb_indicators("CN", ex_regions.get("CN", {}).get("indicators", {}))
    jp_ind = build_wb_indicators("JP", ex_regions.get("JP", {}).get("indicators", {}))
    uk_ind = build_wb_indicators("UK", ex_regions.get("UK", {}).get("indicators", {}))

    us_score, us_angle, us_phase = compute_cycle_score(us_ind)
    eu_score, eu_angle, eu_phase = compute_cycle_score(eu_ind)
    cn_score, cn_angle, cn_phase = compute_cycle_score(cn_ind)
    jp_score, jp_angle, jp_phase = compute_cycle_score(jp_ind)
    uk_score, uk_angle, uk_phase = compute_cycle_score(uk_ind)

    now    = datetime.now(timezone.utc)
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    mavgs  = {"Jan":1.1,"Feb":0.1,"Mar":1.2,"Apr":1.5,"May":0.2,"Jun":0.6,
              "Jul":1.8,"Aug":0.1,"Sep":-0.8,"Oct":0.9,"Nov":1.7,"Dec":1.5}

    payload = {
        "meta": {"last_updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"},
        "regions": {
            "US": {"cycle_score": us_score, "cycle_angle_deg": us_angle, "cycle_phase": us_phase, "indicators": us_ind},
            "EU": {"cycle_score": eu_score, "cycle_angle_deg": eu_angle, "cycle_phase": eu_phase, "indicators": eu_ind},
            "CN": {"cycle_score": cn_score, "cycle_angle_deg": cn_angle, "cycle_phase": cn_phase, "indicators": cn_ind},
            "JP": {"cycle_score": jp_score, "cycle_angle_deg": jp_angle, "cycle_phase": jp_phase, "indicators": jp_ind},
            "UK": {"cycle_score": uk_score, "cycle_angle_deg": uk_angle, "cycle_phase": uk_phase, "indicators": uk_ind},
        },
        "cycles": {
            "presidential": compute_presidential_cycle(),
            "kondratieff": ex_cycles.get("kondratieff", {"current_season":"Winter","season_start_year":2000,"season_end_year_est":2035,"description":"Debt deflation phase. Credit contraction, tech disruption dominant.","position_pct":72}),
            "juglar":      ex_cycles.get("juglar",      {"last_trough":2020,"cycle_length_avg":9,"current_year_in_cycle":6,"phase":"Late Expansion","position_pct":66}),
            "kitchin":     ex_cycles.get("kitchin",     {"last_trough":2023,"cycle_length_avg":4,"current_year_in_cycle":3,"phase":"Inventory Build","position_pct":75}),
            "kuznets":     ex_cycles.get("kuznets",     {"last_trough":2012,"cycle_length_avg":18,"current_year_in_cycle":14,"phase":"Late Expansion","position_pct":77}),
            "credit":      {"phase":"Tightening Plateau","hy_spread":ex_cycles.get("credit",{}).get("hy_spread",3.45),"hy_spread_avg":4.2,"lending_standards_net_pct":ex_cycles.get("credit",{}).get("lending_standards_net_pct",-12.4),"position_pct":65},
            "commodity":   {"phase":"Recovery","crb_index":ex_cycles.get("commodity",{}).get("crb_index",281.4),"crb_change_ytd":ex_cycles.get("commodity",{}).get("crb_change_ytd",4.2),"position_pct":42},
            "real_estate": ex_cycles.get("real_estate", {"last_trough":2012,"cycle_length_avg":18,"current_year_in_cycle":14,"phase":"Late Cycle","position_pct":77,"case_shiller_yoy":3.8}),
            "earnings":    ex_cycles.get("earnings",    {"phase":"Expansion","sp500_eps_growth_fwd":11.4,"revision_ratio":1.12,"position_pct":62}),
            "seasonal": {"current_month":now.month,"current_month_name":months[now.month-1],"sp500_avg_return_current_month":mavgs.get(months[now.month-1],0.0),"monthly_avg_returns":mavgs},
            "decennial": {"current_year_digit":now.year%10,"sp500_avg_return_year6":8.1,"historical_data":{"0":-2.1,"1":14.8,"2":-5.1,"3":24.1,"4":12.4,"5":28.6,"6":8.1,"7":16.2,"8":3.8,"9":15.4}},
            "fed_rate": compute_fed_cycle(us_ind, existing),
        },
    }

    if fingerprint(payload) == old_fp:
        log.info("\nNo changes — skipping commit.")
        return

    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log.info(f"\nUpdated: {DATA_PATH}")


if __name__ == "__main__":
    main()
