# Wellvetor - SODIR Casing Extraction Pipeline

Extracts casing design data from Norwegian Petroleum Directorate (SODIR) PDF documents using OCR and regex parsing.

## Overview

This pipeline processes scanned PDF documents from the SODIR FactPages to extract:
- Casing type (Conductor, Surface, Intermediate, Production, Liner)
- Casing diameter (inches)
- Casing depth (meters)
- Hole diameter (inches)
- Hole depth (meters)
- LOT/FIT mud equivalent (g/cm³)

## Quick Start

```bash
# Run on full dataset (118 documents)
PYTHONPATH=src python3 -m wellvector_pipeline.cli run data/wellbore_document_7_11.csv --cache-dir cache --output output/casing_output.csv --audit output/casing_audit.csv

# Run on limited documents per wellbore (for testing)
PYTHONPATH=src python3 -m wellvector_pipeline.cli run data/wellbore_document_7_11.csv --max-docs-per-wellbore 5 --cache-dir cache --output output/casing_output.csv

# Run on a single local PDF
PYTHONPATH=src python3 -m wellvector_pipeline.cli single-pdf /path/to/pdf.pdf --wellbore "7/11-2"
```

## Output

- **casing_output.csv** - Final deduplicated casing records
- **casing_audit.csv** - Full audit trail with source document, URL, evidence text, and confidence scores

## Architecture

```
src/wellvector_pipeline/
├── cli.py              # Command-line interface
├── pipeline.py         # Main extraction pipeline
├── parser.py           # Regex-based casing record parser
├── normalize.py        # Data normalization utilities
├── pdf_extract.py      # PDF text extraction (PyMuPDF + Tesseract OCR)
├── download.py         # PDF download from SODIR
├── metadata.py         # CSV metadata handling
├── document_priorities.py  # Document prioritization
├── claude_fallback.py  # Optional Claude API fallback (disabled by default)
└── models.py           # Data models
```

## Live Demo Usage

To extract casing data from an unseen SODIR PDF:

```bash
# Download PDF from SODIR
curl -L "SODIR_PDF_URL" -o temp.pdf

# Extract casing data
PYTHONPATH=src python3 -m wellvector_pipeline.cli single-pdf temp.pdf --wellbore "WELLNAME-X"
```

The pipeline:
1. Converts PDF pages to images
2. Runs Tesseract OCR to extract text
3. Parses casing records using regex patterns
4. Deduplicates across multiple source documents

## Dataset

- **Field**: COD (Central Østland)
- **Wellbores**: 7/11-1, 7/11-2, 7/11-3, 7/11-7
- **Documents**: 118 PDFs from SODIR FactPages

## Dependencies

- Python 3.10+
- PyMuPDF (PDF processing)
- pytesseract + Tesseract OCR (text extraction)
- pandas (CSV handling)

Install: `pip install -e .`
