#!/usr/bin/env python3
"""
Match pages to OBP documents by folio range.
Step 8 in the import sequence.
"""

import os
import uuid
import logging
import argparse
from typing import Optional, List, Dict, Set, Tuple, Any

from sqlalchemy import create_engine, text, insert
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

# Ensure these models match your actual models.py
from models import Base, Page2Document

# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///globalise_documents.db")
DEFAULT_CONFIDENCE = 0.8
SOURCE = "FOLIO_RANGE"
BATCH_SIZE = 5_000 # Slightly smaller batches are often more stable

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def parse_folio_numbers(raw: Optional[str]) -> List[int]:
    """Parses '695, 696' into [695, 696]."""
    if not raw:
        return []
    results = []
    for part in raw.split(","):
        try:
            results.append(int(part.strip()))
        except ValueError:
            continue
    return results

def match_folios(database_url: str, confidence: float) -> Dict[str, int]:
    engine = create_engine(database_url, echo=False)
    Base.metadata.create_all(engine)
    
    stats = {
        "inventories_processed": 0,
        "rows_inserted": 0,
        "already_linked_skipped": 0,
    }

    with Session(engine) as session:
        try:
            # 1. Identify inventories with folio data
            inv_rows = session.execute(
                text("SELECT DISTINCT inventory_id FROM document WHERE folio_start IS NOT NULL")
            ).all()

            if not inv_rows:
                logger.warning("No documents with folio_start found. Run script 7 first.")
                return stats

            inventory_ids = [r[0] for r in inv_rows]
            logger.info(f"Processing {len(inventory_ids)} inventories...")

            for inv_id in inventory_ids:
                # 2. Load pages for this inventory
                page_rows = session.execute(
                    text("SELECT id, page_or_folio_number FROM page WHERE inventory_id = :inv_id AND page_or_folio_number IS NOT NULL"),
                    {"inv_id": inv_id},
                ).all()

                if not page_rows:
                    continue

                # Index pages: folio_number -> [list of page_ids]
                folio_to_pages: Dict[int, List[str]] = {}
                for p_id, p_str in page_rows:
                    for num in parse_folio_numbers(p_str):
                        folio_to_pages.setdefault(num, []).append(p_id)

                # 3. Load documents and existing links
                doc_rows = session.execute(
                    text("SELECT id, folio_start, folio_end FROM document WHERE inventory_id = :inv_id AND folio_start IS NOT NULL"),
                    {"inv_id": inv_id},
                ).all()

                existing_links: Set[Tuple[str, str]] = set(
                    session.execute(
                        text(
                            # Intentionally loads ALL page2document rows for this
                            # inventory regardless of method_id, so we never create
                            # a duplicate (page_id, document_id) pair even when
                            # multiple identification methods share an inventory.
                            "SELECT page_id, document_id FROM page2document "
                            "JOIN document ON document.id = page2document.document_id "
                            "WHERE document.inventory_id = :inv_id"
                        ),
                        {"inv_id": inv_id},
                    ).all()
                )

                # 4. Generate New Rows
                inv_new_rows: List[Dict[str, Any]] = []
                for doc_id, f_start, f_end in doc_rows:
                    # Handle documents that might only have a start folio
                    actual_end = f_end if (f_end is not None and f_end >= f_start) else f_start
                    
                    for folio_num in range(f_start, actual_end + 1):
                        for page_id in folio_to_pages.get(folio_num, []):
                            if (page_id, doc_id) in existing_links:
                                stats["already_linked_skipped"] += 1
                                continue
                            
                            inv_new_rows.append({
                                "id": str(uuid.uuid4()),
                                "page_id": page_id,
                                "document_id": doc_id,
                                "index": folio_num,
                                "source": SOURCE,
                                "confidence": confidence,
                            })
                            # Track within loop to prevent duplicates if multiple folios map to same page
                            existing_links.add((page_id, doc_id))

                # 5. Incremental Bulk Insert (Memory Safe)
                if inv_new_rows:
                    for i in range(0, len(inv_new_rows), BATCH_SIZE):
                        batch = inv_new_rows[i : i + BATCH_SIZE]
                        session.execute(insert(Page2Document), batch)
                    
                    session.commit() # Commit per inventory
                    stats["rows_inserted"] += len(inv_new_rows)
                    stats["inventories_processed"] += 1
                    logger.info(f"  Inventory {inv_id}: Linked {len(inv_new_rows)} pages.")

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise

    return stats

def main() -> None:
    parser = argparse.ArgumentParser(description="Match pages to documents by folio range.")
    parser.add_argument("--database", default=DATABASE_URL)
    parser.add_argument("--confidence", type=float, default=DEFAULT_CONFIDENCE)
    args = parser.parse_args()

    print("=" * 60)
    print("GLOBALISE — Folio Range Matching  (step 8 of 8)")
    print("=" * 60)

    results = match_folios(args.database, args.confidence)

    print("\n=== Summary ===")
    print(f"  Inventories processed : {results['inventories_processed']}")
    print(f"  New links created     : {results['rows_inserted']:,}")
    print(f"  Duplicates skipped    : {results['already_linked_skipped']:,}")
    print("✓ Done.")

if __name__ == "__main__":
    main()