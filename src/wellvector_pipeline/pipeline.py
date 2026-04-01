from __future__ import annotations

import csv
import os
from pathlib import Path
from collections import defaultdict

# Load .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from wellvector_pipeline.download import ensure_pdf
from wellvector_pipeline.metadata import load_metadata
from wellvector_pipeline.models import (
    DocumentMetadata,
    ExtractionRecord,
    OUTPUT_COLUMNS,
    PipelineConfig,
)
from wellvector_pipeline.parser import extract_casing_records, _deduplicate
from wellvector_pipeline.pdf_extract import extract_document_text
from wellvector_pipeline.document_priorities import prioritize_documents
from wellvector_pipeline.claude_fallback import (
    should_trigger_ai_fallback,
    extract_with_claude,
    AIFallbackConfig,
)


def run_pipeline(
    metadata_csv: Path,
    output_csv: Path,
    cache_dir: Path,
    audit_csv: Path | None = None,
    config: PipelineConfig | None = None,
) -> list[ExtractionRecord]:
    """Run the full extraction pipeline."""
    if config is None:
        config = PipelineConfig()

    documents = load_metadata(metadata_csv)

    # Prioritize documents
    if config.prioritize_documents:
        documents = prioritize_documents(documents)

    # Optionally limit documents per wellbore
    if config.max_documents_per_wellbore:
        documents = _limit_per_wellbore(
            documents, config.max_documents_per_wellbore
        )

    extracted: list[ExtractionRecord] = []
    total_tokens = 0

    for document in documents:
        try:
            pdf_path = ensure_pdf(document, cache_dir=cache_dir)
            document_text = extract_document_text(document, pdf_path)
            records = extract_casing_records(document_text)

            # AI fallback for low-confidence or missing data
            if config.use_ai_fallback and should_trigger_ai_fallback(
                records, document_text
            ):
                ai_result = extract_with_claude(
                    document_text,
                    AIFallbackConfig(
                        confidence_trigger=config.ai_confidence_threshold
                    ),
                )
                if ai_result:
                    records.extend(ai_result.records)
                    total_tokens += (
                        ai_result.input_tokens + ai_result.output_tokens
                    )

            extracted.extend(records)
        except Exception as exc:
            extracted.append(
                ExtractionRecord(
                    wellbore=document.wellbore,
                    source_document=document.document_name,
                    source_url=document.url,
                    evidence=f"ERROR: {type(exc).__name__}: {exc}",
                    confidence=0.0,
                )
            )

    # Global deduplication across all documents
    non_error_records = [r for r in extracted if not r.evidence.startswith("ERROR")]
    error_records = [r for r in extracted if r.evidence.startswith("ERROR")]
    deduplicated = _deduplicate(non_error_records)
    extracted = deduplicated + error_records

    _write_output(output_csv, [r.as_output_row() for r in extracted], OUTPUT_COLUMNS)
    if audit_csv is not None:
        audit_columns = (
            OUTPUT_COLUMNS
            + ["Source document", "Source URL", "Evidence", "Confidence"]
        )
        _write_output(
            audit_csv, [r.as_audit_row() for r in extracted], audit_columns
        )

    print(f"Total AI tokens used: {total_tokens}")
    return extracted


def run_single_pdf(pdf_path: Path, wellbore: str) -> list[ExtractionRecord]:
    """Run extraction on a single local PDF."""
    metadata = DocumentMetadata(
        wellbore=wellbore, url=str(pdf_path), document_name=pdf_path.name
    )
    document_text = extract_document_text(metadata, pdf_path)
    return extract_casing_records(document_text)


def _limit_per_wellbore(
    documents: list[DocumentMetadata], max_per_wellbore: int
) -> list[DocumentMetadata]:
    """Limit documents per wellbore to max_per_wellbore."""
    by_wellbore: dict[str, list[DocumentMetadata]] = defaultdict(list)
    for doc in documents:
        by_wellbore[doc.wellbore].append(doc)

    limited: list[DocumentMetadata] = []
    for wellbore, docs in by_wellbore.items():
        limited.extend(docs[:max_per_wellbore])
    return limited


def _write_output(
    path: Path, rows: list[dict[str, str]], fieldnames: list[str]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
