#!/usr/bin/env python3
"""
Import GLOBALISE Digitized Indexes of the Dutch East India Company OBP (1602–1799)
from the Excel spreadsheet into the SQLAlchemy database.

This is import script #5 in the sequence. Scripts 1–4 must have been run first so
that Inventory records already exist in the database.

Column mapping
──────────────
Mapped:
  DESCRIPTION                          → Document.title
  INVENTORY NUMBER                     → FK to existing Inventory (by inventory_number)
  YEAR (EARLIEST)                      → Document.date_earliest_begin (Jan 1) /
                                          Document.date_latest_begin  (Dec 31)
  YEAR (LATEST)                        → Document.date_earliest_end  (Jan 1) /
                                          Document.date_latest_end    (Dec 31)
  DOCUMENT TYPE (TANAP)                → Document2Type rows (split on ";")
  ID                                   → ExternalID(context="OBP_INDEX")
  ID (TANAP)                           → ExternalID(context="TANAP")           [nullable]
  ID (DIGITIZED TYPOSCRIPTS)           → ExternalID(context="DIGITIZED TYPOSCRIPTS") [nullable]

Not mapped (no corresponding schema field):
  SECTION
  FOLIONUMBER (START / END OF DOCUMENT)
  FOLIONUMBERS (AS THEY APPEAR IN TYPOSCRIPT)
  YEARS (ALL)
  SETTLEMENT
  LOCATION (TANAP)
  GEOGRAPHICAL COVERAGE OF INV. NUMBER
"""

import os
import sys
import uuid
import logging
from datetime import date, datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from models import (
    Base,
    Document,
    Document2Type,
    Document2ExternalID,
    DocumentIdentificationMethod,
    ExternalID,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///globalise_documents.db")
engine = create_engine(DATABASE_URL, echo=False)
Base.metadata.create_all(engine)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SHEET_NAME = "Digitized Indexes "
METHOD_NAME = "TANAP Digitized Index"
BATCH_SIZE = 5_000


CSV_PATH = os.path.join(
    SCRIPT_DIR,
    "data",
    "globalise_digitized_indexes.csv",
)

# ── helpers ──────────────────────────────────────────────────────────────────

def year_to_start(year) -> Optional[date]:
    """Convert an integer year to Jan 1 of that year."""
    if year is None or (isinstance(year, float) and pd.isna(year)):
        return None
    try:
        return date(int(year), 1, 1)
    except (ValueError, TypeError):
        return None


def year_to_end(year) -> Optional[date]:
    """Convert an integer year to Dec 31 of that year."""
    if year is None or (isinstance(year, float) and pd.isna(year)):
        return None
    try:
        return date(int(year), 12, 31)
    except (ValueError, TypeError):
        return None


def int_or_none(value) -> Optional[str]:
    """
    Return the value as a clean integer string, or None.
    Handles the float representation pandas uses for nullable integer columns
    (e.g. 2.0 → "2").
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return str(int(value))
    except (ValueError, TypeError):
        return None


def parse_document_types(raw) -> list[str]:
    """
    Split the DOCUMENT TYPE (TANAP) compound string into individual type strings.
    Format: "RUBRIEK:<text>;ARCHIEFSTUK:<text>"
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    return [part.strip() for part in str(raw).split(";") if part.strip()]


# ── database helpers ──────────────────────────────────────────────────────────

def get_or_create_method(session: Session) -> str:
    """Return the ID of the TANAP identification method, creating it if needed."""
    existing = (
        session.query(DocumentIdentificationMethod)
        .filter(DocumentIdentificationMethod.name == METHOD_NAME)
        .first()
    )
    if existing:
        logger.info(f"Using existing identification method: {existing.id}")
        return existing.id

    method = DocumentIdentificationMethod(
        id=str(uuid.uuid4()),
        name=METHOD_NAME,
        description=(
            "Documents identified from the GLOBALISE Digitized Indexes of the "
            "Dutch East India Company OBP (1602–1799) spreadsheet. Each row "
            "represents a distinct archival document as catalogued in the TANAP "
            "typoscript indexes."
        ),
        date=datetime.now().date(),
        url="https://datasets.iisg.amsterdam/dataset.xhtml?persistentId=hdl:10622/APNBFT",
    )
    session.add(method)
    session.commit()
    logger.info(f"Created identification method: {method.id}")
    return method.id


def check_already_imported(session: Session, method_id: str) -> int:
    """Return the number of documents already imported with this method."""
    result = session.execute(
        text("SELECT COUNT(*) FROM document WHERE method_id = :mid"),
        {"mid": method_id},
    ).scalar()
    return result or 0


def preload_inventories(session: Session, inventory_numbers: set[str]) -> dict[str, str]:
    """Return {inventory_number: inventory.id} for all requested numbers."""
    result: dict[str, str] = {}
    inv_list = list(inventory_numbers)
    chunk = 900
    for i in range(0, len(inv_list), chunk):
        subset = inv_list[i : i + chunk]
        placeholders = ",".join([f":p{j}" for j in range(len(subset))])
        params = {f"p{j}": v for j, v in enumerate(subset)}
        rows = session.execute(
            text(
                f"SELECT inventory_number, id FROM inventory "
                f"WHERE inventory_number IN ({placeholders})"
            ),
            params,
        ).all()
        for inv_num, inv_id in rows:
            result[inv_num] = inv_id
    return result


# ── core import ───────────────────────────────────────────────────────────────

def load_xlsx() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        logger.error(f"CSV file not found: {CSV_PATH}")
        sys.exit(1)
    df = pd.read_csv(CSV_PATH)
    df = df.where(pd.notnull(df), None)
    logger.info(f"Loaded {len(df)} rows from CSV ({os.path.basename(CSV_PATH)})")
    return df


def bulk_insert(session: Session, table, rows: list[dict], label: str) -> int:
    """Insert rows in batches; returns total inserted."""
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        if batch:
            session.execute(table.insert(), batch)
            inserted += len(batch)
    session.commit()
    logger.info(f"Inserted {inserted:,} {label}")
    return inserted


def main():
    df = load_xlsx()
    session = Session(engine)

    try:
        method_id = get_or_create_method(session)

        # Guard against double-importing
        already = check_already_imported(session, method_id)
        if already > 0:
            logger.warning(
                f"{already:,} documents with method '{METHOD_NAME}' already exist. "
                "Aborting to avoid duplicates. Drop those rows first if you want to re-import."
            )
            return

        # Preload inventories keyed by string inventory number
        inv_numbers = {str(int(v)) for v in df["INVENTORY NUMBER"].dropna().unique()}
        logger.info(f"Preloading {len(inv_numbers):,} inventories...")
        inventories = preload_inventories(session, inv_numbers)

        missing_inventories: set[str] = set()
        doc_rows: list[dict] = []
        doc_type_rows: list[dict] = []
        ext_id_rows: list[dict] = []
        doc_ext_id_rows: list[dict] = []

        for _, row in df.iterrows():
            inv_number = str(int(row["INVENTORY NUMBER"]))
            inv_id = inventories.get(inv_number)
            if not inv_id:
                missing_inventories.add(inv_number)
                continue

            doc_id = str(uuid.uuid4())

            doc_rows.append(
                {
                    "id": doc_id,
                    "inventory_id": inv_id,
                    "title": row.get("DESCRIPTION"),
                    "date_earliest_begin": year_to_start(row.get("YEAR (EARLIEST)")),
                    "date_latest_begin": year_to_end(row.get("YEAR (EARLIEST)")),
                    "date_earliest_end": year_to_start(row.get("YEAR (LATEST)")),
                    "date_latest_end": year_to_end(row.get("YEAR (LATEST)")),
                    "date_text": None,
                    "part_of_id": None,
                    "location_id": None,
                    "method_id": method_id,
                }
            )

            # Document types  (split compound RUBRIEK/ARCHIEFSTUK string)
            for doc_type in parse_document_types(row.get("DOCUMENT TYPE (TANAP)")):
                doc_type_rows.append(
                    {
                        "id": str(uuid.uuid4()),
                        "document_id": doc_id,
                        "document_type": doc_type,
                    }
                )

            # External IDs — one per non-null identifier column
            for context, raw_value in (
                ("OBP_INDEX", row.get("ID")),
                ("TANAP", row.get("ID (TANAP)")),
                ("DIGITIZED TYPOSCRIPTS", row.get("ID (DIGITIZED TYPOSCRIPTS)")),
            ):
                identifier = int_or_none(raw_value)
                if identifier is None:
                    continue
                ext_id = str(uuid.uuid4())
                ext_id_rows.append(
                    {
                        "id": ext_id,
                        "identifier": identifier,
                        "context": context,
                        "URL": None,
                    }
                )
                doc_ext_id_rows.append(
                    {
                        "id": str(uuid.uuid4()),
                        "document_id": doc_id,
                        "external_id": ext_id,
                    }
                )

        if missing_inventories:
            logger.warning(
                f"Skipped {len(missing_inventories)} row(s) — "
                f"inventory numbers not found in DB: {sorted(missing_inventories)}"
            )

        logger.info(
            f"Prepared {len(doc_rows):,} documents, "
            f"{len(doc_type_rows):,} document types, "
            f"{len(ext_id_rows):,} external IDs"
        )

        bulk_insert(session, Document.__table__, doc_rows, "documents")
        bulk_insert(session, Document2Type.__table__, doc_type_rows, "document types")
        bulk_insert(session, ExternalID.__table__, ext_id_rows, "external IDs")
        bulk_insert(
            session,
            Document2ExternalID.__table__,
            doc_ext_id_rows,
            "document ↔ external ID links",
        )

        logger.info("OBP index import completed successfully.")

    except Exception as e:
        logger.exception(f"Error during import: {e}")
        session.rollback()
        raise
    finally:
        session.close()


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":

    print("=" * 60)
    print("GLOBALISE OBP Index Import  (script 5 of 5)")
    print("=" * 60)
    print(f"Source : {os.path.basename(CSV_PATH)}")
    print(f"Sheet  : {SHEET_NAME!r}")
    print(f"DB     : {DATABASE_URL}")
    print(
        "\nThis script requires scripts 1–4 to have been run first "
        "(inventories, pages, hierarchy, baseline documents)."
    )
    print("=" * 60)

    response = input("\nProceed with import? (yes/no): ")
    if response.lower() != "yes":
        print("Import cancelled.")
        sys.exit(0)

    main()
