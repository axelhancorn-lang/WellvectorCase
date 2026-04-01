from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


OUTPUT_COLUMNS = [
    "Wellbore",
    "Casing type",
    "Casing diameter [in]",
    "Casing depth [m]",
    "Hole diameter [in]",
    "Hole depth [m]",
    "LOT/FIT mud eqv. [g/cm3]",
    "Formation test type",
]


@dataclass(slots=True)
class DocumentMetadata:
    wellbore: str
    url: str
    document_name: str = ""
    document_type: str = ""
    document_id: str = ""
    source_row: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class DocumentText:
    metadata: DocumentMetadata
    pdf_path: Path
    text: str
    extractor: str
    page_count: int = 0
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ExtractionRecord:
    wellbore: str
    casing_type: str = ""
    casing_diameter_in: float | None = None
    casing_depth_m: float | None = None
    hole_diameter_in: float | None = None
    hole_depth_m: float | None = None
    lot_fit_mud_eqv_g_cm3: float | None = None
    formation_test_type: str = ""
    source_document: str = ""
    source_url: str = ""
    evidence: str = ""
    confidence: float = 0.0

    def as_output_row(self) -> dict[str, str]:
        return {
            "Wellbore": self.wellbore,
            "Casing type": self.casing_type,
            "Casing diameter [in]": _fmt(self.casing_diameter_in),
            "Casing depth [m]": _fmt(self.casing_depth_m),
            "Hole diameter [in]": _fmt(self.hole_diameter_in),
            "Hole depth [m]": _fmt(self.hole_depth_m),
            "LOT/FIT mud eqv. [g/cm3]": _fmt(self.lot_fit_mud_eqv_g_cm3),
            "Formation test type": self.formation_test_type,
        }

    def as_audit_row(self) -> dict[str, str]:
        row = self.as_output_row()
        row.update(
            {
                "Source document": self.source_document,
                "Source URL": self.source_url,
                "Evidence": self.evidence,
                "Confidence": _fmt(self.confidence),
            }
        )
        return row


def _fmt(value: float | None) -> str:
    if value is None:
        return ""
    if value == int(value):
        return str(int(value))
    return f"{value:.3f}".rstrip("0").rstrip(".")


@dataclass
class PipelineConfig:
    """Configuration for the enhanced pipeline."""

    use_pymupdf: bool = True
    use_ai_fallback: bool = True
    ai_confidence_threshold: float = 0.50
    max_chars_for_ai: int = 15000
    max_documents_per_wellbore: int | None = None
    prioritize_documents: bool = True
