"""
compute_cycle_score.py
========================
Converts indicator values into a composite cycle angle [0°, 360°].

ARCHITECTURE: Two independent signals, not one.

  1. HEALTH SCORE [-1, 1]:
     How good are economic conditions RIGHT NOW?
     High CPI / high rates = BAD (negative signal).
     Feeds the composite_score and fine-tunes the angle by ±30°.

  2. MATURITY SCORE [0, 1]:
     How FAR through the cycle are we? (timing, not quality)
     High rates + high CPI + low unemployment = LATE cycle (high maturity).
     This is the PRIMARY driver of the angle: maturity * 300° → [0°, 300°].

  angle_final = maturity_angle + health_adjustment (±30°)

  Key insight: the same indicator can have OPPOSITE roles:
    - CPI = 3.2% → health: negative (inflation is bad)
                  → maturity: positive (we're late in the cycle)
    - FEDFUNDS = 4.25% → health: negative (tight policy)
                        → maturity: positive (Fed responded to a hot cycle)

  Cycle phases (angle → phase):
    0°–44°   : Trough
    45°–89°  : Early Expansion
    90°–134° : Expansion
    135°–179°: Overheating
    180°–224°: Peak
    225°–269°: Recession
    270°–314°: Late Recession
    315°–359°: Trough (approaching)
"""

import math
from typing import Tuple, List, Dict, Any

# ── Phase Map ────────────────────────────────────────────────────────────────
PHASES = [
    (0,   45,  "trough"),
    (45,  90,  "early_expansion"),
    (90,  135, "expansion"),
    (135, 180, "overheating"),
    (180, 225, "peak"),
    (225, 270, "recession"),
    (270, 315, "late_recession"),
    (315, 360, "trough"),
]

# ── Health Normalization Rules ────────────────────────────────────────────────
# (series_id, direction, min, max, weight)
# direction: +1 = higher is better (expansion), -1 = higher is worse (recession)
HEALTH_RULES: List[Tuple[str, int, float, float, float]] = [
    # ── US Leading ──
    ("T10Y2Y",       +1,  -1.0,   2.5,  2.0),  # positive curve = healthy
    ("UMCSENT",      +1,  50.0, 100.0,  1.5),
    ("ICSA",         -1, 150e3, 700e3,  1.5),   # lower claims = better
    ("M2SL",         +1, 18000, 24000,  0.8),
    ("PERMIT",       +1,   800,  1900,  1.2),
    ("NEWORDER",     +1,   450,   680,  1.0),
    ("BAMLH0A0HYM2", -1,   1.5,   8.0,  1.5),   # tight spreads = better
    ("MICH",         -1,   2.0,   6.0,  0.8),   # lower inflation expectations = better
    ("DTWEXBGS",     -1, 100.0, 135.0,  0.5),
    ("ACOGNO",       +1,  55.0,  95.0,  1.0),
    ("VIX",          -1,  10.0,  50.0,  1.5),   # lower VIX = better
    # ── US Coincident ──
    ("INDPRO",       +1,  90.0, 115.0,  2.0),
    ("PAYEMS",       +1, 130e3, 165e3,  2.0),
    ("RSXFS",        +1,   380,   620,  1.5),
    ("PI",           +1, 18000, 26000,  1.0),
    ("TCU",          +1,  70.0,  85.0,  1.5),
    ("SP500",        +1,  3000,  7000,  1.0),
    # ── US Lagging (health: high CPI/rates = BAD) ──
    ("CPIAUCSL",     -1,   0.0,   7.0,  1.0),
    ("CPILFESL",     -1,   0.0,   6.0,  1.0),
    ("UNRATE",       -1,   3.0,  10.0,  1.5),
    ("FEDFUNDS",     -1,   0.0,   6.0,  1.0),
    ("TOTCI",        +1, 2000,   3500,  0.5),
    ("MPRIME",       -1,   3.0,   9.0,  0.5),
    # ── EU ──
    ("EU_PMI_MFG",   +1,  40.0,  60.0,  1.5),
    ("EU_PMI_SVC",   +1,  40.0,  60.0,  1.5),
    ("EU_CONS_CONF", +1, -30.0,   5.0,  1.0),
    ("EU_ESI",       +1,  85.0, 115.0,  1.5),
    ("EU_IP",        +1,  -5.0,  10.0,  1.5),
    ("EU_CPI",       -1,   0.0,   8.0,  1.0),
    ("EU_CPI_WB",    -1,   0.0,   8.0,  1.0),
    ("EU_UNEMP",     -1,   5.0,  15.0,  1.5),
    ("EU_UNEMP2",    -1,   5.0,  15.0,  1.5),
    ("EU_GDP",       +1,  -2.0,   5.0,  2.0),
    # ── CN ──
    ("CN_PMI_MFG",   +1,  45.0,  55.0,  2.0),
    ("CN_PMI_SVC",   +1,  45.0,  60.0,  1.5),
    ("CN_M2",        +1,   4.0,  15.0,  0.8),
    ("CN_IP",        +1,   2.0,  15.0,  2.0),
    ("CN_CPI",       -1,  -1.0,   5.0,  0.8),
    ("CN_CPI_WB",    -1,  -1.0,   5.0,  0.8),
    ("CN_GDP",       +1,   2.0,   9.0,  2.0),
    ("CN_IND",       +1,   2.0,  12.0,  1.5),
    # ── JP ──
    ("JP_PMI_MFG",   +1,  42.0,  55.0,  2.0),
    ("JP_GDP",       +1,  -2.0,   4.0,  2.0),
    ("JP_CPI_WB",    -1,  -1.0,   5.0,  1.5),
    ("JP_UNEMP",     -1,   2.0,   6.0,  1.0),
    # ── UK ──
    ("UK_PMI_MFG",   +1,  40.0,  60.0,  1.5),
    ("UK_PMI_SVC",   +1,  40.0,  60.0,  1.5),
    ("UK_GDP",       +1,  -1.0,   3.0,  1.5),
    ("UK_GDP_WB",    +1,  -1.0,   3.0,  1.5),
    ("UK_CPI_WB",    -1,   1.0,   7.0,  1.0),
    ("UK_UNEMP",     -1,   3.0,   9.0,  1.0),
]

HEALTH_BY_ID = {r[0]: r for r in HEALTH_RULES}


def _health_score(indicators: Dict[str, List[Dict]]) -> Tuple[float, float]:
    """Compute weighted health score [-1, 1] and total weight."""
    cat_mult = {"leading": 1.5, "coincident": 1.0, "lagging": 0.5}
    total_w, total_v = 0.0, 0.0
    for cat, ind_list in indicators.items():
        mult = cat_mult.get(cat, 1.0)
        for ind in ind_list:
            rule = HEALTH_BY_ID.get(ind.get("id", ""))
            if rule is None:
                continue
            val = ind.get("value")
            if val is None:
                continue
            _, direction, lo, hi, w = rule
            if hi == lo:
                continue
            norm = max(-1.0, min(1.0, ((float(val) - lo) / (hi - lo)) * 2 - 1)) * direction
            total_v += norm * w * mult
            total_w  += w * mult
    if total_w == 0:
        return 0.0, 0.0
    return max(-1.0, min(1.0, total_v / total_w)), total_w


def _maturity_score(indicators: Dict[str, List[Dict]]) -> float:
    """
    Compute cycle TIMING maturity [0, 1].
    0 = trough/start, ~0.6 = peak, 1 = deep trough again (full cycle).

    Uses late-cycle signals: high rates, high CPI, full employment, flat curve.
    These are OPPOSITE in sign to health_score for the same indicators.
    """
    # Flatten all indicators to id→value dict
    all_v = {}
    for ind_list in indicators.values():
        for ind in ind_list:
            k = ind.get("id")
            v = ind.get("value")
            if k and v is not None:
                all_v[k] = float(v)

    def sig(key, lo, hi, invert=False, w=1.0):
        v = all_v.get(key)
        if v is None:
            return None, None
        n = max(0.0, min(1.0, (v - lo) / (hi - lo)))
        return (1.0 - n if invert else n), w

    signals = []

    # ── US / Global timing signals ──
    # Fed Funds: 0%=early, 5.5%=peak response to overheating
    s, w = sig("FEDFUNDS", 0.0, 5.5, w=2.5)
    if s is not None: signals.append((s, w))

    # CPI above target: 0.5%=trough, 5%=overheating
    s, w = sig("CPIAUCSL", 0.5, 5.0, w=2.0)
    if s is not None: signals.append((s, w))

    # Yield curve: FLAT/NEGATIVE = late cycle. invert=True because we want low T10Y2Y = high maturity.
    # Range: -0.5 (inverted, late cycle) to 2.5 (steep, early cycle)
    s, w = sig("T10Y2Y", -0.5, 2.5, invert=True, w=2.0)
    if s is not None: signals.append((s, w))

    # Unemployment: full employment = late cycle
    s, w = sig("UNRATE", 3.5, 10.0, invert=True, w=1.0)
    if s is not None: signals.append((s, w))

    # HY Credit Spread: very tight (<2%) = complacency = peak
    s, w = sig("BAMLH0A0HYM2", 1.5, 8.0, invert=True, w=1.5)
    if s is not None: signals.append((s, w))

    # VIX: very low (<15) = complacency = late cycle signal
    s, w = sig("VIX", 10.0, 40.0, invert=True, w=0.8)
    if s is not None: signals.append((s, w))

    # ── EU timing signals ──
    s, w = sig("EU_CPI", 0.5, 5.0, w=1.5)
    if s is not None: signals.append((s, w))
    s, w = sig("EU_CPI_WB", 0.5, 5.0, w=1.5)
    if s is not None: signals.append((s, w))
    # EU NAIRU ~7%. Below 7% = late cycle. Range [5.5, 12], invert.
    s, w = sig("EU_UNEMP", 5.5, 12.0, invert=True, w=0.8)
    if s is not None: signals.append((s, w))
    s, w = sig("EU_UNEMP2", 5.5, 12.0, invert=True, w=0.8)
    if s is not None: signals.append((s, w))
    # PMI below 50 = contraction = POST-PEAK or PRE-TROUGH. 
    # Here we treat it as NEGATIVE maturity (early cycle, things haven't heated yet).
    s, w = sig("EU_PMI_MFG", 42.0, 62.0, w=1.0)
    if s is not None: signals.append((s, w))

    # ── CN timing signals ──
    # China: near deflation = early cycle, CPI rising = expanding
    s, w = sig("CN_CPI_WB", -0.5, 4.0, w=1.5)
    if s is not None: signals.append((s, w))
    # Strong GDP = active expansion = mid-cycle
    s, w = sig("CN_GDP", 2.0, 9.0, w=1.2)
    if s is not None: signals.append((s, w))
    s, w = sig("CN_PMI_MFG", 45.0, 55.0, w=1.0)
    if s is not None: signals.append((s, w))

    # ── JP timing signals ──
    # Japan CPI: first real inflation in 30y. 0%=trough, 4%=overheating (for Japan)
    s, w = sig("JP_CPI_WB", 0.0, 4.0, w=2.0)
    if s is not None: signals.append((s, w))
    s, w = sig("JP_GDP", -1.0, 4.0, w=1.0)
    if s is not None: signals.append((s, w))

    # ── UK timing signals ──
    s, w = sig("UK_CPI_WB", 1.0, 6.0, w=1.5)
    if s is not None: signals.append((s, w))
    s, w = sig("UK_GDP_WB", -1.0, 3.0, w=1.0)
    if s is not None: signals.append((s, w))

    if not signals:
        return 0.35  # default: early expansion

    total_w   = sum(w for _, w in signals)
    weighted  = sum(s * w for s, w in signals)
    return weighted / total_w


def compute_score_from_indicators(
    indicators: Dict[str, List[Dict[str, Any]]]
) -> Tuple[float, float, str]:
    """
    Returns (composite_score [-1,1], angle_deg [0,360], phase_string).

    Formula:
        maturity ∈ [0, 1]  → base angle = maturity * 300°
        health   ∈ [-1, 1] → adjustment = ±30°

    The adjustment direction depends on the cycle half:
        ascending (maturity ≤ 0.5): better health → higher angle (further along)
        descending (maturity > 0.5): better health → lower angle (not yet crashed)
    """
    health, total_w = _health_score(indicators)

    if total_w == 0:
        return 0.0, 90.0, "expansion"

    maturity = _maturity_score(indicators)

    # Linear base angle: 0=trough(0°), 0.5=overheating(150°), 0.65=peak(195°), 1.0=deepTrough(300°)
    base_angle = maturity * 300.0

    # Health fine-tunes ±30°
    normalized_health = (health + 1.0) / 2.0  # [0, 1]
    if maturity <= 0.5:
        # Ascending: high health = we're doing well = push slightly further
        adjustment = (normalized_health - 0.5) * 60.0
    else:
        # Descending: high health = things haven't crashed yet = pull back slightly
        adjustment = -(normalized_health - 0.5) * 60.0

    angle_deg = (base_angle + adjustment) % 360.0
    phase     = angle_to_phase(angle_deg)

    return round(health, 3), round(angle_deg, 1), phase


def angle_to_phase(angle_deg: float) -> str:
    a = angle_deg % 360
    for lo, hi, phase in PHASES:
        if lo <= a < hi:
            return phase
    return "trough"


def phase_label(phase: str) -> str:
    labels = {
        "trough":          "Trough",
        "early_expansion": "Early Expansion",
        "expansion":       "Expansion",
        "overheating":     "Overheating",
        "peak":            "Peak",
        "recession":       "Recession",
        "late_recession":  "Late Recession",
    }
    return labels.get(phase, phase.replace("_", " ").title())


if __name__ == "__main__":
    # ── Self-test with realistic March 2026 US data ──
    test_us = {
        "leading": [
            {"id": "T10Y2Y",        "value":  0.35},
            {"id": "UMCSENT",       "value": 72.0},
            {"id": "ICSA",          "value": 220000},
            {"id": "BAMLH0A0HYM2",  "value":  3.45},
            {"id": "VIX",           "value": 18.0},
            {"id": "PERMIT",        "value": 1450},
        ],
        "coincident": [
            {"id": "PAYEMS",  "value": 158500},
            {"id": "INDPRO",  "value": 103.0},
            {"id": "TCU",     "value": 77.8},
            {"id": "SP500",   "value": 5700},
        ],
        "lagging": [
            {"id": "CPIAUCSL",  "value": 3.2},
            {"id": "CPILFESL",  "value": 3.9},
            {"id": "UNRATE",    "value": 4.1},
            {"id": "FEDFUNDS",  "value": 4.25},
        ],
    }

    test_eu = {
        "leading": [
            {"id": "EU_PMI_MFG", "value": 47.6},
        ],
        "coincident": [
            {"id": "EU_GDP", "value": 0.7},
            {"id": "EU_IP",  "value": -0.8},
        ],
        "lagging": [
            {"id": "EU_CPI_WB", "value": 2.3},
            {"id": "EU_UNEMP",  "value": 6.0},
        ],
    }

    test_jp = {
        "coincident": [{"id": "JP_GDP",    "value": 0.1}],
        "lagging":    [{"id": "JP_CPI_WB", "value": 3.1}],
    }

    test_cn = {
        "leading":    [{"id": "CN_PMI_MFG", "value": 50.2}],
        "coincident": [{"id": "CN_GDP",     "value": 5.0}],
        "lagging":    [{"id": "CN_CPI_WB",  "value": 0.5}],
    }

    test_uk = {
        "coincident": [{"id": "UK_GDP_WB", "value": 0.1}],
        "lagging":    [{"id": "UK_CPI_WB", "value": 2.8}],
    }

    print("=== Cycle Score Self-Test (March 2026) ===\n")
    for name, data in [("US", test_us), ("EU", test_eu), ("JP", test_jp),
                        ("CN", test_cn), ("UK", test_uk)]:
        score, angle, phase = compute_score_from_indicators(data)
        mat = _maturity_score(data)
        print(f"  {name:3s}  health={score:+.3f}  maturity={mat:.3f}  "
              f"angle={angle:6.1f}°  → {phase_label(phase)}")
