from __future__ import annotations

import sys
from pathlib import Path

from wellvector_pipeline.models import DocumentMetadata, DocumentText

EXTRACTOR_PYMUPDF = "pymupdf"
EXTRACTOR_PYPDF = "pypdf"

# Track which extractors are available
_pymupdf_available = False
_pypdf_available = False

try:
    import fitz  # PyMuPDF
    _pymupdf_available = True
except ImportError:
    pass

try:
    from pypdf import PdfReader
    _pypdf_available = True
except ImportError:
    pass


# Maximum file size to attempt with PyMuPDF (in bytes)
# Large PDFs can cause memory/decompression issues
MAX_PYMUPDF_SIZE = 10 * 1024 * 1024  # 10 MB - be conservative with decompression issues


def extract_document_text(
    metadata: DocumentMetadata, pdf_path: Path
) -> DocumentText:
    """Extract text from PDF using available extractors.

    Tries PyMuPDF first (better for image-based PDFs), falls back to pypdf.
    For large PDFs, uses pypdf directly to avoid decompression issues.
    """
    warnings: list[str] = []

    # Check file size - use pypdf for very large files
    file_size = pdf_path.stat().st_size

    # Try PyMuPDF first (unless file is too large)
    if _pymupdf_available and file_size < MAX_PYMUPDF_SIZE:
        try:
            text, page_count, pymupdf_warnings = _extract_with_pymupdf(pdf_path)
            warnings.extend(pymupdf_warnings)
            extractor = EXTRACTOR_PYMUPDF

            # If PyMuPDF yields empty text, try pypdf
            if not text.strip() and _pypdf_available:
                text, page_count, pypdf_warnings = _extract_with_pypdf(pdf_path)
                warnings.extend(pypdf_warnings)
                warnings.append("PyMuPDF returned empty text; fell back to pypdf.")
                extractor = EXTRACTOR_PYPDF
        except Exception as e:
            warnings.append(f"PyMuPDF failed: {e}, falling back to pypdf")
            if _pypdf_available:
                text, page_count, pypdf_warnings = _extract_with_pypdf(pdf_path)
                warnings.extend(pypdf_warnings)
                extractor = EXTRACTOR_PYPDF
            else:
                raise RuntimeError(f"PDF extraction failed: {e}")
    elif _pypdf_available:
        try:
            text, page_count, warnings = _extract_with_pypdf(pdf_path)
            extractor = EXTRACTOR_PYPDF
        except Exception as e:
            warnings.append(f"pypdf failed: {e}")
            text = ""
            page_count = 0
            extractor = EXTRACTOR_PYPDF
    else:
        raise RuntimeError("No PDF extraction library available (tried PyMuPDF, pypdf)")

    # If both fail, mark as non-extractable
    if not text.strip():
        warnings.append("PDF appears image-based or non-extractable.")

    return DocumentText(
        metadata=metadata,
        pdf_path=pdf_path,
        text=text,
        extractor=extractor,
        page_count=page_count,
        warnings=warnings,
    )


def _extract_with_pymupdf(
    pdf_path: Path,
) -> tuple[str, int, list[str]]:
    """Extract text using PyMuPDF with OCR fallback for image-based PDFs."""
    import fitz

    warnings: list[str] = []
    doc = fitz.open(str(pdf_path))
    page_text: list[str] = []

    for idx, page in enumerate(doc, start=1):
        # Try normal text extraction first
        text = ""
        page_exception = None
        try:
            text = page.get_text() or ""
        except Exception:
            # ThinkingBlock or other unsupported element - try OCR below
            page_exception = sys.exc_info()[1]
            text = ""

        # If get_text() raised an exception (e.g. ThinkingBlock), OR returned empty
        # text, try OCR via pytesseract
        if not text.strip() or page_exception is not None:
            try:
                ocr_text = _extract_with_ocr(page)
                if ocr_text.strip():
                    text = ocr_text
                    if page_exception is not None:
                        warnings.append(
                            f"Page {idx}: {type(page_exception).__name__} from get_text(), recovered via OCR."
                        )
                    else:
                        warnings.append(f"Page {idx} extracted via OCR.")
                elif page_exception is not None:
                    warnings.append(f"Page {idx}: {type(page_exception).__name__} from get_text(), OCR returned no text.")
            except Exception:
                if page_exception is not None:
                    warnings.append(f"Page {idx}: {type(page_exception).__name__} from get_text(), OCR also failed.")

        if not text.strip():
            warnings.append(f"Page {idx} returned no text.")

        page_text.append(text)

    page_count = len(doc)
    doc.close()
    return "\n\n".join(page_text), page_count, warnings


def _extract_with_ocr(page) -> str:
    """Extract text from a PyMuPDF page using Tesseract OCR with preprocessing."""
    try:
        import pytesseract
        from PIL import Image, ImageOps, ImageEnhance
        import io

        # Render the page to an image at higher resolution (3x for better OCR)
        mat = fitz.Matrix(3, 3)
        pix = page.get_pixmap(matrix=mat)

        # Convert to PIL Image
        img_data = pix.tobytes("png")
        img = Image.open(io.BytesIO(img_data))

        # Preprocess image for better OCR
        img = _preprocess_for_ocr(img)

        # Run OCR with PSM 6 (single block of text) and Norwegian lang
        text = pytesseract.image_to_string(
            img,
            config="--psm 6 -l nor+eng",
        )
        return text
    except ImportError:
        return ""
    except Exception:
        return ""


def _preprocess_for_ocr(img: Image.Image) -> Image.Image:
    """Apply image preprocessing to improve OCR accuracy."""
    try:
        from PIL import ImageOps, ImageEnhance

        # Convert to grayscale
        img = img.convert("L")

        # Apply autocontrast to improve contrast (handles dark/light variation)
        img = ImageOps.autocontrast(img, cutoff=2)

        # Slight sharpening to make text edges crisper
        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(1.5)

        return img
    except Exception:
        # If preprocessing fails for any reason, return original
        return img


def _extract_with_pypdf(
    pdf_path: Path,
) -> tuple[str, int, list[str]]:
    """Extract text using pypdf."""
    from pypdf import PdfReader

    warnings: list[str] = []
    reader = PdfReader(str(pdf_path))
    page_text: list[str] = []

    for idx, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        if not text.strip():
            warnings.append(f"Page {idx} returned no text.")
        page_text.append(text)

    return "\n\n".join(page_text), len(reader.pages), warnings
