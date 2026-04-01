"""Document prioritization for casing data extraction."""

from __future__ import annotations

from wellvector_pipeline.models import DocumentMetadata

# Priority tiers (lower number = higher priority)
PRIORITY_TIERS: dict[str, int] = {
    # Tier 1: Most authoritative casing data sources
    "COD": 1,
    "COMPLETION_LOG": 1,
    "COMPLETION_REPORT": 1,
    "WELL_COMPLETION_REPORT": 1,
    "COMPLETION": 1,
    # Tier 2: Summary data
    "WDSS": 2,
    "OLD NPD WDSS": 2,
    "INDIVIDUAL_WELL_RECORD": 2,
    # Tier 3: Supporting documents
    "DRILLING_MUD_REPORT": 3,
    "DRILLING_MUD_RECORD": 3,
    "DRILLING_FLUID_SUMMARY": 3,
    "DRILLING_PROGRAM": 3,
    "CHANGE IN DRILLING PROGRAM": 3,
    # Tier 4: Less relevant
    "NPD PAPER": 4,
    "LOGGING_RAPPORT": 4,
    # Tier 5: Low relevance
    "GEOCHEMICAL": 5,
    "CORE_": 5,
    "CORE ANALYSIS": 5,
}

# Document name keywords indicating casing data presence
CASING_RELEVANT_KEYWORDS: tuple[str, ...] = (
    "casing",
    "conductor",
    "surface",
    "intermediate",
    "production",
    "liner",
    "hole",
    "bit",
    "reamer",
    "lot",
    "fit",
    "leak-off",
    "formation integrity",
    "pressure",
    "test",
    "completion",
    "cement",
)


def calculate_document_priority(
    document_type: str,
    document_name: str,
) -> tuple[int, float]:
    """
    Returns (priority_tier, relevance_score).

    Lower tier = process first.
    relevance_score 0.0-1.0 indicates keyword match confidence.
    """
    doc_type_upper = document_type.upper()
    doc_name_lower = document_name.lower()
    combined_lower = f"{doc_type_upper} {doc_name_lower}"

    # Find priority tier
    tier = PRIORITY_TIERS.get(doc_type_upper, 4)  # Default to tier 4

    # Check for COD-specific naming conventions
    if "cod" in doc_name_lower:
        tier = min(tier, 1)  # Boost COD documents

    # Check for well completion reports
    if "completion" in combined_lower:
        tier = min(tier, 1)

    # Check for WDSS documents
    if "wdss" in combined_lower:
        tier = min(tier, 2)

    # Calculate keyword relevance
    keyword_matches = sum(
        1 for kw in CASING_RELEVANT_KEYWORDS if kw in combined_lower
    )
    relevance = min(keyword_matches / 3.0, 1.0)  # Cap at 1.0

    return tier, relevance


def prioritize_documents(
    documents: list[DocumentMetadata],
) -> list[DocumentMetadata]:
    """Sort documents by priority tier, then by relevance score descending."""
    scored = [
        (calculate_document_priority(d.document_type, d.document_name), d)
        for d in documents
    ]
    scored.sort(key=lambda x: (x[0][0], -x[0][1]))  # Sort by tier, then -relevance
    return [d for _, d in scored]
