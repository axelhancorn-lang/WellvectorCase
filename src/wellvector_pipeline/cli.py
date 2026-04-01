from __future__ import annotations

import argparse
from pathlib import Path

from wellvector_pipeline.pipeline import run_pipeline, run_single_pdf
from wellvector_pipeline.models import PipelineConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract casing design data from SODIR PDFs."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the pipeline on a metadata CSV.")
    run_parser.add_argument("metadata_csv", type=Path, help="Path to the input metadata CSV.")
    run_parser.add_argument("--output", type=Path, default=Path("output/casing_output.csv"))
    run_parser.add_argument("--audit", type=Path, default=Path("output/casing_audit.csv"))
    run_parser.add_argument("--cache-dir", type=Path, default=Path("cache/pdfs"))
    run_parser.add_argument(
        "--no-ai-fallback",
        action="store_true",
        help="Disable Claude API fallback",
    )
    run_parser.add_argument(
        "--ai-confidence-threshold",
        type=float,
        default=0.50,
        help="Confidence threshold for AI fallback (default: 0.50)",
    )
    run_parser.add_argument(
        "--max-docs-per-wellbore",
        type=int,
        default=None,
        help="Max documents to process per wellbore (default: all)",
    )
    run_parser.add_argument(
        "--no-prioritize",
        action="store_true",
        help="Disable document prioritization",
    )

    single_parser = subparsers.add_parser(
        "single-pdf", help="Run the extractor on one local PDF."
    )
    single_parser.add_argument("pdf_path", type=Path)
    single_parser.add_argument("--wellbore", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        config = PipelineConfig(
            use_ai_fallback=not args.no_ai_fallback,
            ai_confidence_threshold=args.ai_confidence_threshold,
            max_documents_per_wellbore=args.max_docs_per_wellbore,
            prioritize_documents=not args.no_prioritize,
        )
        run_pipeline(
            metadata_csv=args.metadata_csv,
            output_csv=args.output,
            cache_dir=args.cache_dir,
            audit_csv=args.audit,
            config=config,
        )
        print(f"Wrote output to {args.output}")
        print(f"Wrote audit trail to {args.audit}")
        return 0

    if args.command == "single-pdf":
        records = run_single_pdf(args.pdf_path, args.wellbore)
        if not records:
            print("No casing records found.")
            return 0
        for record in records:
            print(record.as_audit_row())
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
