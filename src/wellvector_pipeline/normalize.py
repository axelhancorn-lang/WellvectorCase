from __future__ import annotations

import re
from fractions import Fraction


CASING_TYPE_SYNONYMS: dict[str, str] = {
    "conductor": "Conductor",
    "surface": "Surface",
    "intermediate": "Intermediate",
    "production": "Production",
    "liner": "Liner",
    "tie-back": "Tie-back",
    "tieback": "Tie-back",
    "tailpipe": "Tailpipe",
    "tail pipe": "Tailpipe",
    # Norwegian terminology
    "leder": "Conductor",
    "overflate": "Surface",
    "mellom": "Intermediate",
    "produksjon": "Production",
    "foring": "Liner",
    "innfelling": "Tie-back",
}


def normalize_casing_type(raw: str) -> str:
    cleaned = re.sub(r"\s+", " ", raw.strip().lower())
    for key, value in CASING_TYPE_SYNONYMS.items():
        if key in cleaned:
            return value
    if cleaned:
        return cleaned.title()
    return ""


def parse_diameter_to_inches(raw: str) -> float | None:
    token = raw.strip().replace('"', "").replace("''", "")
    token = token.replace("in.", "").replace("inch", "").replace("inches", "")
    token = re.sub(r"\s+", " ", token).strip()
    if not token:
        return None

    fraction_match = re.fullmatch(r"(\d+)\s+(\d+)/(\d+)", token)
    if fraction_match:
        whole, numerator, denominator = fraction_match.groups()
        return float(int(whole) + Fraction(int(numerator), int(denominator)))

    mixed_match = re.fullmatch(r"(\d+(?:\.\d+)?)", token)
    if mixed_match:
        return float(mixed_match.group(1))

    compact_fraction_match = re.fullmatch(r"(\d+)-(\d+)/(\d+)", token)
    if compact_fraction_match:
        whole, numerator, denominator = compact_fraction_match.groups()
        return float(int(whole) + Fraction(int(numerator), int(denominator)))

    return None


def parse_depth_to_meters(raw: str) -> float | None:
    token = raw.strip().lower().replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)\s*(m|meter|meters|metre|metres)\b", token)
    if match:
        return float(match.group(1))

    feet_match = re.search(r"(\d+(?:\.\d+)?)\s*(ft|feet)\b", token)
    if feet_match:
        return round(float(feet_match.group(1)) * 0.3048, 3)

    bare_match = re.fullmatch(r"\d+(?:\.\d+)?", token)
    if bare_match:
        return float(token)

    return None


def parse_mud_weight_to_g_cm3(raw: str) -> float | None:
    token = raw.strip().lower().replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)\s*(g/cm3|g/cc|sg|ppg)\b", token)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2)
    if unit in {"g/cm3", "g/cc", "sg"}:
        return round(value, 3)
    if unit == "ppg":
        return round(value * 0.1198264273, 3)
    return None


def normalize_formation_test_type(raw: str) -> str:
    token = raw.strip().lower()
    if "lot" in token or "leak" in token:
        return "LOT"
    if "fit" in token or "integrity" in token:
        return "FIT"
    return raw.strip().upper()


def calculate_lot_fit_from_pressure(
    pressure_psi: float,
    depth_m: float,
) -> float | None:
    """
    Calculate LOT/FIT mud equivalent from pressure reading.

    Formula: mud_eqv = pressure_psi / (0.052 * depth_m)

    This derives from: pressure_gradient = mud_weight * 0.052
    Rearranged: mud_weight = pressure / (0.052 * depth)
    """
    if pressure_psi <= 0 or depth_m <= 0:
        return None

    mud_eqv = pressure_psi / (0.052 * depth_m)
    return round(mud_eqv, 3)


# Norwegian COD pressure patterns
PRESSURE_PATTERNS: list[re.Pattern] = [
    re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:psi|p\.s\.i\.?)", re.IGNORECASE),
    re.compile(r"test\s*pressure[:\s]*(\d+(?:[.,]\d+)?)", re.IGNORECASE),
    re.compile(r"leak[\s-]*off\s*pressure[:\s]*(\d+(?:[.,]\d+)?)", re.IGNORECASE),
    re.compile(r"formation\s*integrity\s*test[:\s]*(\d+(?:[.,]\d+)?)", re.IGNORECASE),
]


def extract_pressure_psi(text: str) -> list[float]:
    """Extract pressure values in PSI from text."""
    pressures: list[float] = []
    for pattern in PRESSURE_PATTERNS:
        for match in pattern.finditer(text):
            try:
                value = float(match.group(1).replace(",", "."))
                if value > 0:
                    pressures.append(value)
            except ValueError:
                continue
    return pressures
