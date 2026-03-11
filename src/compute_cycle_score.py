"""
compute_cycle_score.py
========================
Converts normalized indicator values into a composite cycle score,
then maps that score to an angle on the market sine wave.

Cycle phases (angle → phase):
  0°–44°   : Trough
  45°–89°  : Early Expansion
  90°–134° : Expansion
  135°–179°: Overheating
  180°–224°: Peak
  225°–269°: Recession
  270°–314°: Late Recession
  315°–359°: Back to Trough
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

# ── Normalization Rules ──────────────────────────────────────────────────────
# Each rule: (series_id, direction, typical_min, typical_max, weight)
# direction: +1 means "higher = more expansion", -1 means "higher = more recession"
NORMALIZATION_RULES: List[Tuple[str, int, float, float, float]] = [
    # Leading – US
    ("T10Y2Y",       +1,  -1.0,   2.5,   2.0),  # yield curve: positive = healthy
    ("UMCSENT",      +1,  50.0,  100.0,  1.5),
    ("ICSA",         -1, 150000, 700000, 1.5),   # higher claims = worse
    ("M2SL",         +1,  18000, 24000,  0.8),
    ("PERMIT",       +1,    800,  1900,  1.2),
    ("NEWORDER",     +1,    450,   680,  1.0),
    ("BAMLH0A0HYM2", -1,    1.5,   8.0,  1.5),  # tighter spreads = better
    ("MICH",         -1,    2.0,   6.0,  0.8),  # lower inflation expectations = better
    ("DTWEXBGS",     -1,  100.0, 135.0,  0.5),  # stronger USD can hurt globally
    ("ACOGNO",       +1,   55.0,  95.0,  1.0),
    ("VIX",          -1,   10.0,  50.0,  1.5),  # lower VIX = more expansion
    # Coincident – US
    ("INDPRO",       +1,   90.0, 115.0,  2.0),
    ("PAYEMS",       +1, 130000, 165000, 2.0),
    ("RSXFS",        +1,   380,   620,   1.5),
    ("PI",           +1, 18000,  26000,  1.0),
    ("TCU",          +1,  70.0,   85.0,  1.5),
    ("SP500",        +1, 3000,   6500,   1.0),
    # Lagging – US
    ("CPIAUCSL",     -1,   0.0,    8.0,  1.0),
    ("CPILFESL",     -1,   0.0,    7.0,  1.0),
    ("UNRATE",       -1,   3.0,   10.0,  1.5),
    ("FEDFUNDS",     -1,   0.0,    6.0,  1.0),
    # ── EU ──
    ("EU_PMI_MFG",   +1,  40.0,   60.0,  1.5),
    ("EU_PMI_SVC",   +1,  40.0,   60.0,  1.5),
    ("EU_CONS_CONF", +1, -30.0,    5.0,  1.0),
    ("EU_ESI",       +1,  85.0,  115.0,  1.5),
    ("EU_IP",        +1,  -5.0,   10.0,  1.5),
    ("EU_CPI",       -1,   0.0,    8.0,  1.0),
    ("EU_UNEMP",     -1,   5.0,   15.0,  1.5),
    # ── CN ──
    ("CN_PMI_MFG",   +1,  45.0,   55.0,  2.0),
    ("CN_PMI_SVC",   +1,  45.0,   60.0,  1.5),
    ("CN_M2",        +1,   4.0,   15.0,  0.8),
    ("CN_IP",        +1,   2.0,   15.0,  2.0),
    ("CN_CPI",       -1,  -1.0,    5.0,  0.8),
    # ── JP ──
    ("JP_PMI_MFG",   +1,  42.0,   55.0,  2.0),
    ("JP_GDP",       +1,  -2.0,    4.0,  2.0),
    # ── UK ──
    ("UK_PMI_MFG",   +1,  40.0,   60.0,  1.5),
    ("UK_PMI_SVC",   +1,  40.0,   60.0,  1.5),
    ("UK_GDP",       +1,  -1.0,    3.0,  1.5),
]

RULES_BY_ID = {r[0]: r for r in NORMALIZATION_RULES}


def normalize_indicator(series_id: str, value: float) -> Tuple[float, float]:
    """
    Normalize a single indicator to [-1, 1] range.
    Returns (normalized_value, weight).
    """
    rule = RULES_BY_ID.get(series_id)
    if rule is None:
        return 0.0, 0.0  # Unknown indicator → skip

    _, direction, min_val, max_val, weight = rule
    if max_val == min_val:
        return 0.0, weight

    # Clamp and scale to [0, 1]
    clamped = max(min_val, min(max_val, value))
    scaled = (clamped - min_val) / (max_val - min_val)  # [0, 1]

    # Apply direction: if direction = +1, high = +1; if direction = -1, high = -1
    normalized = (scaled * 2 - 1) * direction  # [-1, 1]
    return normalized, weight


def compute_score_from_indicators(
    indicators: Dict[str, List[Dict[str, Any]]]
) -> Tuple[float, float, str]:
    """
    Given a dict of {category: [indicator_objects]}, compute:
    - composite_score in [-1, 1]
    - angle_degrees in [0, 360]
    - phase string

    Category weights:
      leading    × 1.5  (forward-looking)
      coincident × 1.0
      lagging    × 0.5  (backward-looking)
    """
    category_multiplier = {
        "leading":    1.5,
        "coincident": 1.0,
        "lagging":    0.5,
    }

    total_weight = 0.0
    weighted_sum = 0.0

    for category, ind_list in indicators.items():
        cat_mult = category_multiplier.get(category, 1.0)
        for ind in ind_list:
            series_id = ind.get("id", "")
            value = ind.get("value")
            if value is None:
                continue
            norm, weight = normalize_indicator(series_id, float(value))
            effective_weight = weight * cat_mult
            weighted_sum += norm * effective_weight
            total_weight  += effective_weight

    if total_weight == 0:
        return 0.0, 90.0, "expansion"

    composite_score = weighted_sum / total_weight  # [-1, 1]
    composite_score = max(-1.0, min(1.0, composite_score))

    # Map score to angle: -1 → 270° (late recession), 0 → 90° (expansion), 1 → 180° (peak)
    # We map score [-1, 1] to angle using a shifted cosine:
    # cos(θ) = composite_score  →  θ = arccos(composite_score)  in [0, π]
    # But we want the full [0°, 360°] cycle, so we use momentum (change) to determine
    # which half of the cycle we're in. For simplicity, assume momentum > 0 = ascending half.
    # Full mapping:
    #   score = 0, ascending  → angle ≈ 90°  (expansion)
    #   score = 1             → angle = 180° (peak)
    #   score = 0, descending → angle ≈ 270° (late recession)
    #   score = -1            → angle = 0°   (trough)

    # Compute momentum: are leading indicators improving?
    leading_momentum = _compute_momentum(indicators.get("leading", []))

    if leading_momentum >= 0:
        # Ascending half: map score [-1→0→1] to angle [0°→90°→180°]
        # score → angle: angle = 90 + arcsin(score) * (180/π)  in [0, 180]
        angle_rad = math.asin(max(-1.0, min(1.0, composite_score)))
        angle_deg = 90 + math.degrees(angle_rad)  # [0, 180]
    else:
        # Descending half: map score [1→0→-1] to angle [180°→270°→360°]
        angle_rad = math.asin(max(-1.0, min(1.0, composite_score)))
        angle_deg = 270 - math.degrees(angle_rad)  # [180, 360]

    angle_deg = angle_deg % 360

    phase = angle_to_phase(angle_deg)
    return round(composite_score, 3), round(angle_deg, 1), phase


def _compute_momentum(leading_indicators: List[Dict[str, Any]]) -> float:
    """
    Compute average % change across leading indicators.
    Positive = improving conditions, negative = deteriorating.
    """
    changes = []
    for ind in leading_indicators:
        val  = ind.get("value")
        prev = ind.get("previous")
        if val is None or prev is None or prev == 0:
            continue
        direction = RULES_BY_ID.get(ind.get("id", ""), (None, 1))[1]
        pct_chg = (val - prev) / abs(prev) * 100 * direction
        changes.append(pct_chg)

    if not changes:
        return 0.0
    return sum(changes) / len(changes)


def angle_to_phase(angle_deg: float) -> str:
    """Map a cycle angle to a phase name."""
    a = angle_deg % 360
    for lo, hi, phase in PHASES:
        if lo <= a < hi:
            return phase
    return "trough"


def phase_label(phase: str) -> str:
    """Human-readable phase label."""
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
    # ── Quick self-test ──
    test_indicators = {
        "leading": [
            {"id": "T10Y2Y",   "value": 0.35, "previous": 0.20},
            {"id": "UMCSENT",  "value": 72.0, "previous": 69.0},
            {"id": "VIX",      "value": 16.0, "previous": 18.0},
        ],
        "coincident": [
            {"id": "INDPRO",   "value": 104.0, "previous": 103.0},
            {"id": "PAYEMS",   "value": 159000, "previous": 158500},
        ],
        "lagging": [
            {"id": "UNRATE",   "value": 4.1, "previous": 4.2},
            {"id": "FEDFUNDS", "value": 4.25, "previous": 4.50},
        ],
    }
    score, angle, phase = compute_score_from_indicators(test_indicators)
    print(f"Score: {score:.3f} | Angle: {angle:.1f}° | Phase: {phase_label(phase)}")
