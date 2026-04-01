from __future__ import annotations

import csv
from pathlib import Path

from wellvector_pipeline.models import DocumentMetadata


WELLBORE_CANDIDATES = (
    "wellbore",
    "wellbore_name",
    "wlbName",
    "wlbname",
    "well name",
)
URL_CANDIDATES = (
    "url",
    "document_url",
    "document url",
    "wlbDocumentUrl",
    "wlbdocumenturl",
)
NAME_CANDIDATES = (
    "document_name",
    "document name",
    "title",
    "wlbDocumentName",
    "wlbdocumentname",
)
TYPE_CANDIDATES = (
    "document_type",
    "document type",
    "wlbDocumentType",
    "wlbdocumenttype",
)
ID_CANDIDATES = (
    "document_id",
    "document id",
    "id",
    "wlbDocumentId",
    "wlbdocumentid",
)


def load_metadata(csv_path: Path) -> list[DocumentMetadata]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"No headers found in {csv_path}")
        field_map = {field.lower(): field for field in reader.fieldnames}
        wellbore_field = _pick_field(field_map, WELLBORE_CANDIDATES)
        url_field = _pick_field(field_map, URL_CANDIDATES)
        name_field = _pick_field(field_map, NAME_CANDIDATES, required=False)
        type_field = _pick_field(field_map, TYPE_CANDIDATES, required=False)
        id_field = _pick_field(field_map, ID_CANDIDATES, required=False)

        documents: list[DocumentMetadata] = []
        for row in reader:
            wellbore = (row.get(wellbore_field, "") if wellbore_field else "").strip()
            url = (row.get(url_field, "") if url_field else "").strip()
            if not wellbore or not url:
                continue
            documents.append(
                DocumentMetadata(
                    wellbore=wellbore,
                    url=url,
                    document_name=(row.get(name_field, "") if name_field else "").strip(),
                    document_type=(row.get(type_field, "") if type_field else "").strip(),
                    document_id=(row.get(id_field, "") if id_field else "").strip(),
                    source_row={k: v for k, v in row.items() if k is not None},
                )
            )
        return documents


def _pick_field(
    field_map: dict[str, str],
    candidates: tuple[str, ...],
    required: bool = True,
) -> str | None:
    for candidate in candidates:
        if candidate.lower() in field_map:
            return field_map[candidate.lower()]
    if required:
        raise ValueError(f"Missing required column. Expected one of: {', '.join(candidates)}")
    return None
