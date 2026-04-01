from __future__ import annotations

import hashlib
import ssl
import urllib.request
from pathlib import Path

from wellvector_pipeline.models import DocumentMetadata


USER_AGENT = "wellvector-pipeline/0.1"


# Create SSL context that doesn't verify certificates
# This is needed for servers with self-signed certificates
_ssl_context = ssl.create_default_context()
_ssl_context.check_hostname = False
_ssl_context.verify_mode = ssl.CERT_NONE


def ensure_pdf(document: DocumentMetadata, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    extension = Path(document.url).suffix or ".pdf"
    name_seed = (
        f"{document.document_id}_{document.document_name}_{document.url}".strip("_")
    )
    slug = _safe_filename(name_seed) or hashlib.sha1(
        document.url.encode("utf-8")
    ).hexdigest()
    pdf_path = cache_dir / f"{slug}{extension}"
    if pdf_path.exists():
        return pdf_path

    request = urllib.request.Request(document.url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60, context=_ssl_context) as response:
            pdf_path.write_bytes(response.read())
    except ssl.SSLError:
        # Fallback: try without SSL context for servers that have issues
        with urllib.request.urlopen(request, timeout=60) as response:
            pdf_path.write_bytes(response.read())
    return pdf_path


def _safe_filename(raw: str) -> str:
    chars = []
    for ch in raw:
        if ch.isalnum():
            chars.append(ch)
        elif ch in {" ", "-", "_"}:
            chars.append("_")
    return "".join(chars).strip("_")[:120]
