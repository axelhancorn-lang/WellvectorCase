"""Claude API fallback for low-confidence extractions."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

from wellvector_pipeline.models import DocumentText, ExtractionRecord

# Configuration
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
DEFAULT_MODEL = "claude-sonnet-4-20250514"

# Confidence thresholds
CONFIDENCE_THRESHOLD_FOR_AI = 0.50  # Trigger AI fallback below this
CONFIDENCE_THRESHOLD_HIGH = 0.75    # Skip AI if above this


@dataclass
class AIFallbackConfig:
    model: str = DEFAULT_MODEL
    max_tokens: int = 1024
    temperature: float = 0.0  # Deterministic output
    confidence_trigger: float = CONFIDENCE_THRESHOLD_FOR_AI


@dataclass
class AIExtractionResult:
    records: list[ExtractionRecord]
    raw_response: str
    model: str
    input_tokens: int
    output_tokens: int


SYSTEM_PROMPT = """You are an expert at extracting casing and well completion data from Norwegian oilfield documents.

CRITICAL WELLBORE RULE:
- The wellbore name is provided in the user message
- ALWAYS use the EXACT wellbore name provided — do NOT modify, add suffixes, or invent variations
- For example: if the user says wellbore is "7/11-1", respond with "7/11-1" NOT "7/11-1X" or "7/11-1 R1"
- Never add letters, numbers, or suffixes to the wellbore name

Extract casing design data with this EXACT schema:
- Wellbore name (MUST match the provided wellbore exactly)
- Casing type (Conductor, Surface, Intermediate, Production, Liner, Tie-back)
- Casing diameter in inches
- Casing depth in meters
- Hole diameter in inches
- Hole depth in meters
- LOT/FIT mud equivalent in g/cm3 (calculate from pressure if needed)
- Formation test type (LOT or FIT)

Rules:
- If a field cannot be determined, respond with null (no quotes, no "N/A")
- Units: Prefer meters for depth, inches for diameter
- Norwegian documents may use COD (Casing and Open Hole Data) format
- LOT = Leak-Off Test, FIT = Formation Integrity Test
- For LOT/FIT: if mud equivalent not directly given but pressure is, calculate:
  mud_eqv_g_cm3 = pressure_psi / (0.052 * depth_m)
- Return ONE record per casing string found
- If multiple casings of same type, return each as separate record
- If no casing data found, return exactly: NO_CASING_DATA
"""


USER_PROMPT_TEMPLATE = """Extract casing data from this document:

Wellbore: {wellbore}
Document: {document_name}
Document type: {document_type}

--- DOCUMENT TEXT BEGIN ---
{text_content}
--- DOCUMENT TEXT END ---

IMPORTANT: Use wellbore name EXACTLY as shown above: "{wellbore}"

Respond in this JSON format only (no markdown, no explanation):
[
  {{
    "wellbore": "{wellbore}",
    "casing_type": "...",
    "casing_diameter_in": ...,
    "casing_depth_m": ...,
    "hole_diameter_in": ...,
    "hole_depth_m": ...,
    "lot_fit_mud_eqv_g_cm3": ...,
    "formation_test_type": "..."
  }}
]

Use null for any field that cannot be determined. If no casing data, respond with exactly: NO_CASING_DATA"""


def should_trigger_ai_fallback(
    records: list[ExtractionRecord],
    document: DocumentText,
    config: AIFallbackConfig | None = None,
) -> bool:
    """
    Determine if AI fallback should be triggered.

    Triggers when:
    - No records found but text exists
    - All records have confidence below threshold
    - Document type is high-priority (COD, WELL_COMPLETION, etc.)
    """
    if not ANTHROPIC_AVAILABLE or not ANTHROPIC_API_KEY:
        return False

    if config is None:
        config = AIFallbackConfig()

    # Skip if no text
    if not document.text.strip():
        return False

    # Skip if already high confidence
    if records and all(r.confidence >= CONFIDENCE_THRESHOLD_HIGH for r in records):
        return False

    # Trigger conditions
    high_priority_types = {
        "COD",
        "WELL_COMPLETION_REPORT",
        "COMPLETION_LOG",
        "COMPLETION_REPORT",
        "COMPLETION",
        "WDSS",
        "INDIVIDUAL_WELL_RECORD",
    }
    is_high_priority = document.metadata.document_type.upper() in high_priority_types

    # If no records at all AND document is OLD NPD WDSS, skip AI
    # These very old documents have extremely garbled OCR that AI can't parse reliably
    if not records and "OLD NPD WDSS" in document.metadata.document_type.upper():
        return False

    if not records:
        # No records but not an OLD WDSS - could still be worth trying AI
        # Only trigger for high-priority docs with reasonable text length
        if is_high_priority and len(document.text) > 500:
            return True
        return False

    if is_high_priority and all(r.confidence < CONFIDENCE_THRESHOLD_HIGH for r in records):
        return True  # High-priority doc with low confidence

    if all(r.confidence < config.confidence_trigger for r in records):
        return True  # All records below threshold

    return False


def extract_with_claude(
    document: DocumentText,
    config: AIFallbackConfig | None = None,
) -> AIExtractionResult | None:
    """Extract casing data using Claude API."""
    if not ANTHROPIC_AVAILABLE:
        raise RuntimeError("anthropic library not installed")

    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")

    if config is None:
        config = AIFallbackConfig()

    # Prepare text: limit to relevant pages/sections
    text_content = _prepare_text_for_api(document)

    user_prompt = USER_PROMPT_TEMPLATE.format(
        wellbore=document.metadata.wellbore,
        document_name=document.metadata.document_name,
        document_type=document.metadata.document_type,
        text_content=text_content,
    )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    response = client.messages.create(
        model=config.model,
        max_tokens=config.max_tokens,
        temperature=config.temperature,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    # Find the first text block (skip ThinkingBlock which has no .text)
    raw_text = ""
    for block in response.content:
        if hasattr(block, "text"):
            raw_text = block.text
            break
    input_tokens = response.usage.input_tokens
    output_tokens = response.usage.output_tokens

    # Parse response
    records = _parse_ai_response(raw_text, document, input_tokens, output_tokens)

    return AIExtractionResult(
        records=records,
        raw_response=raw_text,
        model=config.model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


def _prepare_text_for_api(
    document: DocumentText, max_chars: int = 15000
) -> str:
    """
    Prepare document text for API call.

    Strategy:
    1. If short enough, send entire text
    2. If longer, extract front portion (casing data often appears early)
    """
    text = document.text

    if len(text) <= max_chars:
        return text

    # For longer documents, take front portion
    # Casing data often appears early in completion reports
    front_chars = int(max_chars * 0.85)
    back_chars = int(max_chars * 0.15)

    return (
        text[:front_chars]
        + "\n\n[... content truncated ...]\n\n"
        + text[-back_chars:]
    )


def _parse_ai_response(
    raw_text: str,
    document: DocumentText,
    input_tokens: int,
    output_tokens: int,
) -> list[ExtractionRecord]:
    """Parse Claude's JSON response into ExtractionRecord objects."""

    # Clean response
    cleaned = raw_text.strip()
    if cleaned == "NO_CASING_DATA":
        return []

    # Remove markdown code blocks if present
    cleaned = re.sub(r"```json\s*", "", cleaned)
    cleaned = re.sub(r"```\s*$", "", cleaned)

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        return []

    correct_wellbore = document.metadata.wellbore
    records = []
    for item in data:
        try:
            # Validate and correct wellbore name
            extracted_wellbore = item.get("wellbore", correct_wellbore)
            # Only accept wellbores that match the document's wellbore (allow exact match)
            if extracted_wellbore != correct_wellbore:
                extracted_wellbore = correct_wellbore

            record = ExtractionRecord(
                wellbore=extracted_wellbore,
                casing_type=str(item.get("casing_type", "") or ""),
                casing_diameter_in=_parse_float(item.get("casing_diameter_in")),
                casing_depth_m=_parse_float(item.get("casing_depth_m")),
                hole_diameter_in=_parse_float(item.get("hole_diameter_in")),
                hole_depth_m=_parse_float(item.get("hole_depth_m")),
                lot_fit_mud_eqv_g_cm3=_parse_float(item.get("lot_fit_mud_eqv_g_cm3")),
                formation_test_type=str(item.get("formation_test_type", "") or "").upper(),
                source_document=document.metadata.document_name,
                source_url=document.metadata.url,
                evidence=f"AI_EXTRACTED: {raw_text[:200]}",
                confidence=0.85,  # AI extractions get fixed high confidence
            )
            records.append(record)
        except Exception:
            continue

    return records


def _parse_float(value) -> float | None:
    """Parse numeric value, handling null/N/A strings."""
    if value is None or value == "N/A" or value == "" or value == "null":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
