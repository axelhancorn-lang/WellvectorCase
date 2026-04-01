# Wellvetor Presentation - Step by Step

## SLIDE 1: Title
**Wellvetor - SODIR Casing Extraction Pipeline**

Brief intro: Automated extraction of well casing data from Norwegian Petroleum Directorate PDFs using OCR and regex parsing.

---

## SLIDE 2: The Problem
- SODIR FactPages contain scanned PDF documents
- These are image scans - no searchable text
- We need structured casing design data (casing type, diameter, depth)
- 118 documents across 4 wellbores in the COD field

---

## SLIDE 3: The Challenge
- OCR produces garbled text from scanned images
- Example OCR errors:
  - "9 S/8" should be "9-5/8"
  - "13-3/8-inch set at 1945 meters" → "13-3/8-inch set at 1g45 meters"
- Documents are inconsistent in format and naming
- Some casing types not explicitly labeled

---

## SLIDE 4: The Solution - Architecture
```
SODIR PDFs → Download → PDF Processing (PyMuPDF)
                              ↓
                        Render pages as images
                              ↓
                        Tesseract OCR → Text
                              ↓
                        Regex Parser → Casing records
                              ↓
                        Deduplication
                              ↓
                        CSV Output + Audit Trail
```

---

## SLIDE 5: Key Technical Solutions
1. **OCR Preprocessing** - Render at 3x resolution for better OCR quality
2. **Fraction Normalization** - Fix OCR errors like "9 S/8" → "9-5/8"
3. **Depth-Aware Inference** - When casing type not labeled, infer from diameter + depth
4. **Hole Diameter Extraction** - Find diameter near "hole" keyword
5. **Global Deduplication** - Same casing in multiple docs → one record

---

## SLIDE 6: Results
- **14 unique casing records** extracted from 118 documents
- **4 wellbores**: 7/11-1, 7/11-2, 7/11-3, 7/11-7
- Full casing strings (Conductor → Production) for 7/11-2 and 7/11-3
- 7/11-7 has formation test (LOT) data only

Show: `output/casing_output.csv`

---

## SLIDE 7: Audit Trail
Each extraction includes:
- Source document name
- Source URL (SODIR)
- Evidence text (the OCR text that matched)
- Confidence score (0.55 - 0.90)

Show: `output/casing_audit.csv`

---

## LIVE DEMO: Step by Step

### Step 1: Get a SODIR PDF URL
- Go to https://factpages.sodir.no
- Navigate to a wellbore (e.g., Wellbore Explorer)
- Find a document (e.g., Completion Report)
- Copy the PDF URL

Example URL format:
```
https://factpages.sodir.no/pbl/wellbore_documents/156_7_11_2_WELL_COMPLETION_REPORT.PDF
```

### Step 2: Download the PDF
```bash
curl -L "PASTE_URL_HERE" -o demo.pdf
```

### Step 3: Run the Pipeline
```bash
cd "/Users/axelhancorn/Python VScode/Wellvetor case"
PYTHONPATH=src python3 -m wellvector_pipeline.cli single-pdf demo.pdf --wellbore "7/11-2"
```

### Step 4: View Results
The pipeline will output extracted casing records directly to the terminal.

Example output:
```
Wellbore,Casing type,Casing diameter [in],Casing depth [m],Hole diameter [in],Hole depth [m],LOT/FIT mud eqv. [g/cm3],Formation test type
7/11-2,Conductor,30,132,36,132,,,
7/11-2,Surface,20,484,26,484,,,
...
```

---

## Commands Reference

```bash
# Navigate to project
cd "/Users/axelhancorn/Python VScode/Wellvetor case"

# Download a PDF from SODIR
curl -L "URL_HERE" -o temp.pdf

# Extract casing from single PDF
PYTHONPATH=src python3 -m wellvector_pipeline.cli single-pdf temp.pdf --wellbore "WELLNAME-X"

# Run full dataset (takes 30-60 min)
PYTHONPATH=src python3 -m wellvector_pipeline.cli run data/wellbore_document_7_11.csv --cache-dir cache --output output/casing_output.csv --audit output/casing_audit.csv
```

---

## What to Say During Demo

1. "Here's a SODIR PDF URL"
2. "I'll download it with curl"
3. "Now I'll run the extraction pipeline"
4. "The pipeline found X casing records"
5. "You can see the casing type, diameter, depth, etc."

---

## Files to Show During Presentation

| File | What to Point Out |
|------|-------------------|
| `output/casing_output.csv` | Final clean output - casing records |
| `output/casing_audit.csv` | Audit trail with source URLs and evidence |
| `src/wellvector_pipeline/parser.py` | The regex parsing logic |
| `src/wellvector_pipeline/pdf_extract.py` | OCR extraction code |
| `README.md` | How to run the pipeline |

---

## Potential Questions

**Q: Why not use AI/LLM for extraction?**
A: We tried Claude API fallback but it hallucinated on garbled OCR. The regex approach is deterministic and reliable.

**Q: How do you handle different document formats?**
A: Multiple regex patterns, Norwegian terminology synonyms, and deduplication across all documents.

**Q: What if OCR is too garbled?**
A: Record is discarded or marked low-confidence. We prefer no record over wrong record.

**Q: Can this work for other fields?**
A: Yes - just change the metadata CSV with SODIR URLs for different wellbores.
