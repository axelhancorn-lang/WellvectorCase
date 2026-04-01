from __future__ import annotations

import re
from dataclasses import replace

from wellvector_pipeline.models import DocumentText, ExtractionRecord
from wellvector_pipeline.normalize import (
    calculate_lot_fit_from_pressure,
    extract_pressure_psi,
    normalize_casing_type,
    normalize_formation_test_type,
    parse_depth_to_meters,
    parse_diameter_to_inches,
    parse_mud_weight_to_g_cm3,
)


CASING_KEYWORDS = (
    "conductor",
    "surface",
    "intermediate",
    "production",
    "liner",
    "tie-back",
    "tieback",
    # Norwegian
    "leder",
    "overflate",
    "mellom",
    "produksjon",
    "foring",
    "innfelling",
)
DIAMETER_PATTERN = re.compile(
    r'(\d+(?:\s+\d+/\d+|-\d+/\d+|\.\d+)?)\s*-?\s*(?:[\u0022\u0027\u201c\u201d]|(?<!\s)inch(?:es)?|(?<!\s)in\.?)(?=\s|$|[,;])',
    re.IGNORECASE
)
DEPTH_PATTERN = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(m|meter|meters|metre|metres|ft|feet)\b", re.IGNORECASE
)
FORMATION_PATTERN = re.compile(
    r"\b(lot|fit|leak[\s-]*off|formation integrity)\b", re.IGNORECASE
)
MUD_WEIGHT_PATTERN = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(g/cm3|g/cc|sg|ppg)\b", re.IGNORECASE
)


# Standard casing sizes in inches (Norwegian offshore)
STANDARD_CASING_SIZES = [30.0, 20.0, 13.375, 9.625, 7.0, 5.0]


def _round_to_standard_casing(diameter: float) -> float:
    """Map a parsed diameter to the nearest standard casing size.

    Handles OCR errors where e.g., 9.625 might be read as 9, 8.5, 9.8, etc.
    """
    # If it's already a standard size, return it
    if diameter in STANDARD_CASING_SIZES:
        return diameter
    # Find the nearest standard size
    nearest = min(STANDARD_CASING_SIZES, key=lambda s: abs(s - diameter))
    # Only return if within tolerance (25% of value, or max 2 inches)
    tolerance = max(2.0, diameter * 0.25)
    if abs(nearest - diameter) <= tolerance:
        return nearest
    # Outside tolerance - return original (might be hole diameter)
    return diameter


def _infer_casing_type_from_diameter(diameter_in: float, depth_m: float | None = None) -> str:
    """Infer casing type from diameter AND depth when not explicitly labeled.

    Uses depth-aware heuristics for Norwegian offshore wells:
    - Very large diameters (>=28") or very shallow depth (<250m) = Conductor
    - Large diameters (>=18") at shallow-moderate depth (<1000m) = Surface
    - Medium diameters (>=11") at moderate depth (>=1000m) = Intermediate
    - Smaller medium diameters (>=9") at intermediate depth (<2500m) = Intermediate
    - Medium diameters at deep depth (>=2500m) = Production
    - Small diameters (<9") at deep depth (>=2500m) = Production
    - Small diameters (<6") = Liner
    """
    # Very large diameter OR very shallow depth = Conductor
    if diameter_in >= 28 or (depth_m is not None and depth_m < 250):
        return "Conductor"

    # Large diameter at shallow-moderate depth = Surface
    if diameter_in >= 18:
        if depth_m is None or depth_m < 1000:
            return "Surface"
        else:
            return "Intermediate"

    # Medium-large diameter (11-18") at moderate-deep depth = Intermediate
    if diameter_in >= 11:
        if depth_m is None or depth_m < 1000:
            return "Surface"  # Large casings at shallow-moderate depth are surface
        elif depth_m < 2500:
            return "Intermediate"
        else:
            return "Production"

    # Medium diameter (9-11") at intermediate depth = Intermediate
    if diameter_in >= 9:
        if depth_m is None or depth_m < 2500:
            return "Intermediate"
        else:
            return "Production"

    # Smaller diameters at deep depth = Production
    if diameter_in >= 6:
        if depth_m is None or depth_m < 3000:
            return "Intermediate"
        else:
            return "Production"

    # Smallest diameters = Liner
    return "Liner"


def _preprocess_ocr_fractions(text: str) -> str:
    """Normalize common OCR errors in fractions and diameters."""
    # "9 S/8" or "9 §/8" -> "9-5/8" (S or § is corrupted 5)
    text = re.sub(r'(\d)\s+([A-Za-z§])\s*/\s*(\d)', r'\1-5/\3', text)
    # "12 1/k" -> "12-1/4" (k is corrupted 4)
    text = re.sub(r'(\d+)\s+(\d)\s*/\s*([A-Za-z])', r'\1-\2/4', text)
    # "13 3/8=inch" or similar -> "13-3/8-inch"
    text = re.sub(r'(\d+)\s+(\d)\s*/\s*(\d+)\s*[=]*\s*(inch|in\.?)', r'\1-\2/\3-\4', text)
    # "7=inch" or "7 =inch" -> "7-inch"
    text = re.sub(r'(\d)\s*[=]+\s*(inch)', r'\1-\2', text, flags=re.IGNORECASE)
    return text


def extract_casing_records(document: DocumentText) -> list[ExtractionRecord]:
    text = _preprocess_ocr_fractions(document.text)
    text = _normalize_text(text)
    if not text:
        return []

    candidate_lines = _candidate_lines(text)
    records: list[ExtractionRecord] = []
    for line in candidate_lines:
        record = _parse_casing_line(document, line)
        if record:
            records.append(record)

    records.extend(_parse_formation_tests(document, candidate_lines))
    return _deduplicate(records)


def _normalize_text(text: str) -> str:
    collapsed = text.replace("\r", "\n")
    collapsed = re.sub(r"[ \t]+", " ", collapsed)
    collapsed = re.sub(r"\n{3,}", "\n\n", collapsed)
    return collapsed.strip()


def _candidate_lines(text: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidates = []
    for line in lines:
        lowered = line.lower()
        if any(keyword in lowered for keyword in CASING_KEYWORDS):
            candidates.append(line)
        elif "lot" in lowered or "fit" in lowered or "leak-off" in lowered or "formation integrity" in lowered:
            candidates.append(line)
        else:
            # Also match lines with diameter + depth patterns (e.g., "30-inch set at 132 meters")
            # This catches casing program lines that don't use explicit type keywords
            has_diameter = DIAMETER_PATTERN.search(line) is not None
            has_depth = DEPTH_PATTERN.search(line) is not None
            if has_diameter and has_depth:
                candidates.append(line)
    return candidates


def _parse_casing_line(
    document: DocumentText, line: str
) -> ExtractionRecord | None:
    lowered = line.lower()

    # Check if line has explicit casing type keyword
    has_keyword = any(keyword in lowered for keyword in CASING_KEYWORDS)

    # Also check for "set at X meters" pattern which indicates casing without type label
    has_set_pattern = "set at" in lowered and DIAMETER_PATTERN.search(line)

    if not has_keyword and not has_set_pattern:
        return None

    casing_type = normalize_casing_type(line)

    # Validate casing_type is actually a known type (not garbage from .title())
    valid_types = {"Conductor", "Surface", "Intermediate", "Production", "Liner", "Tie-back"}
    if casing_type not in valid_types:
        casing_type = ""

    # Parse diameters - filter out small OCR garbage values (< 5 inches)
    all_diameters = [
        parse_diameter_to_inches(match.group(1))
        for match in DIAMETER_PATTERN.finditer(line)
    ]
    all_diameters = [v for v in all_diameters if v is not None and v >= 5]
    # Map to standard casing sizes to handle OCR errors (e.g., 9S/8 -> 9.625)
    all_diameters = [_round_to_standard_casing(d) for d in all_diameters]

    # Casing diameter is the first (usually smallest meaningful diameter)
    casing_diameter = all_diameters[0] if all_diameters else None
    # Hole diameter is typically larger - look for diameter after "hole" in text
    hole_diameter = None
    if "hole" in line.lower():
        # Find diameter near "hole" - can be before or after
        hole_idx = line.lower().find("hole")
        # Search in a window around "hole" (±10 chars)
        search_start = max(0, hole_idx - 15)
        search_end = min(len(line), hole_idx + 10)
        search_window = line[search_start:search_end]
        for match in DIAMETER_PATTERN.finditer(search_window):
            val = parse_diameter_to_inches(match.group(1))
            if val is not None and val >= 5:
                hole_diameter = val
                break
    if hole_diameter is None and len(all_diameters) > 1:
        # Fallback: second diameter that is larger than first
        # But NOT a standard casing size (those are likely casing diameters, not hole)
        for d in all_diameters[1:]:
            if d > casing_diameter and d not in STANDARD_CASING_SIZES:
                hole_diameter = d
                break
        # If no non-standard found, don't use a standard casing size as hole diameter

    # Parse depths - in "X meters (Y feet)" format, first is casing depth
    all_depths = [
        parse_depth_to_meters(" ".join(match.groups()))
        for match in DEPTH_PATTERN.finditer(line)
    ]
    all_depths = [v for v in all_depths if v is not None]
    # Filter out converted feet values that are close to the meter values
    # (e.g., 132.588m from 435ft should not be a separate depth entry)
    depths_filtered = []
    for d in all_depths:
        # Skip if it's within 5m of a previous depth (likely converted feet)
        if not any(abs(d - existing) < 5 for existing in depths_filtered):
            depths_filtered.append(d)
    depths = depths_filtered

    casing_depth = depths[0] if depths else None
    # In "set at X meters in Y-inch hole" format, hole depth = casing depth
    hole_depth = casing_depth

    # If no explicit type found, infer from diameter + depth
    if not casing_type and casing_diameter is not None:
        casing_type = _infer_casing_type_from_diameter(casing_diameter, casing_depth)

    mud_weights = [
        parse_mud_weight_to_g_cm3(" ".join(match.groups()))
        for match in MUD_WEIGHT_PATTERN.finditer(line)
    ]
    mud_weights = [value for value in mud_weights if value is not None]
    formation_match = FORMATION_PATTERN.search(line)

    record = ExtractionRecord(
        wellbore=document.metadata.wellbore,
        casing_type=casing_type,
        casing_diameter_in=casing_diameter,
        hole_diameter_in=hole_diameter,
        casing_depth_m=casing_depth,
        hole_depth_m=hole_depth,
        lot_fit_mud_eqv_g_cm3=mud_weights[0] if mud_weights else None,
        formation_test_type=normalize_formation_test_type(
            formation_match.group(1)
        ) if formation_match else "",
        source_document=document.metadata.document_name,
        source_url=document.metadata.url,
        evidence=line,
        confidence=_score_candidate(line, all_diameters, depths),
    )

    # Reject spurious records: if casing_type is set, casing_depth is required
    # (every real casing string has a depth; records without depth are garbage)
    # Also require minimum depth - real casings are at least 10m, not <1m (which would be OCR garbage)
    if record.casing_type:
        if record.casing_depth_m is None or record.casing_depth_m < 10:
            return None

    if not any(
        [
            record.casing_diameter_in,
            record.casing_depth_m,
            record.hole_diameter_in,
            record.hole_depth_m,
            record.lot_fit_mud_eqv_g_cm3,
        ]
    ):
        return None
    return record


def _parse_formation_tests(
    document: DocumentText, lines: list[str]
) -> list[ExtractionRecord]:
    results: list[ExtractionRecord] = []
    for line in lines:
        if any(keyword in line.lower() for keyword in CASING_KEYWORDS):
            continue
        formation_match = FORMATION_PATTERN.search(line)
        if not formation_match:
            continue
        mud_weights = [
            parse_mud_weight_to_g_cm3(" ".join(match.groups()))
            for match in MUD_WEIGHT_PATTERN.finditer(line)
        ]
        mud_weights = [value for value in mud_weights if value is not None]
        depths = [
            parse_depth_to_meters(" ".join(match.groups()))
            for match in DEPTH_PATTERN.finditer(line)
        ]
        depths = [value for value in depths if value is not None]

        # If no direct mud weight but has pressure data, calculate it
        if not mud_weights:
            pressures = extract_pressure_psi(line)
            if pressures and depths:
                calc_mud = calculate_lot_fit_from_pressure(pressures[0], depths[0])
                if calc_mud:
                    mud_weights = [calc_mud]

        if not mud_weights and not depths:
            continue
        results.append(
            ExtractionRecord(
                wellbore=document.metadata.wellbore,
                casing_type="",
                casing_depth_m=depths[0] if depths else None,
                lot_fit_mud_eqv_g_cm3=mud_weights[0] if mud_weights else None,
                formation_test_type=normalize_formation_test_type(
                    formation_match.group(1)
                ),
                source_document=document.metadata.document_name,
                source_url=document.metadata.url,
                evidence=line,
                confidence=0.55,
            )
        )
    return results


def _score_candidate(
    line: str, diameters: list[float], depths: list[float]
) -> float:
    score = 0.35
    if diameters:
        score += 0.25
    if depths:
        score += 0.2
    if FORMATION_PATTERN.search(line):
        score += 0.1
    if len(line.split()) >= 5:
        score += 0.1
    return min(score, 0.99)


def _deduplicate(records: list[ExtractionRecord]) -> list[ExtractionRecord]:
    """Deduplicate records, preferring more complete/higher confidence records.

    Deduplication key is: wellbore + casing_type + casing_diameter + rounded_casing_depth.
    This groups the same casing across different source documents even when depths
    are reported slightly differently.
    """
    deduped: dict[
        tuple[str, str, float | None, float | None, str], ExtractionRecord
    ] = {}
    for record in records:
        # Round depth to nearest meter for deduplication key (within 1m tolerance)
        depth_key = round(record.casing_depth_m) if record.casing_depth_m else None
        # Use "NONE" for unknown formation test type so they don't incorrectly merge
        ft_key = record.formation_test_type if record.formation_test_type else "NONE"
        key = (
            record.wellbore,
            record.casing_type,
            record.casing_diameter_in,
            depth_key,
            ft_key,
        )
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = replace(record)
        elif record.confidence > existing.confidence:
            # Higher confidence wins
            deduped[key] = replace(record)
        elif record.confidence == existing.confidence:
            # Prefer record with more non-null fields
            existing_fields = sum(1 for v in [
                existing.casing_diameter_in, existing.casing_depth_m,
                existing.hole_diameter_in, existing.hole_depth_m,
                existing.lot_fit_mud_eqv_g_cm3
            ] if v is not None)
            new_fields = sum(1 for v in [
                record.casing_diameter_in, record.casing_depth_m,
                record.hole_diameter_in, record.hole_depth_m,
                record.lot_fit_mud_eqv_g_cm3
            ] if v is not None)
            if new_fields > existing_fields:
                deduped[key] = replace(record)
    return list(deduped.values())
