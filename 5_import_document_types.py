"""
Import document type concepts from SKOS thesaurus into the database.

Reads data/pp_project_globalisethesaurus.ttl and extracts skos:Concept entries
from two concept schemes:

  - GLOBALISE documenttypen
    https://digitaalerfgoed.poolparty.biz/globalise/7a273a96-2e11-4307-b68d-8046b4455a4b
  - TANAP documenttypen
    https://digitaalerfgoed.poolparty.biz/globalise/321974b0-c2a1-46be-9830-ff8bc7e9cc88

For each concept it stores:
  - id            : UUID extracted from the concept URI
  - scheme        : "GLOBALISE" or "TANAP"
  - pref_label_nl : Dutch skos:prefLabel
  - pref_label_en : English skos:prefLabel

Requires the DocumentType model to be present in models.py.
"""

import os
import sys
import uuid
from pathlib import Path

from rdflib import Graph, URIRef, Literal
from rdflib.namespace import SKOS
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, DocumentType  # noqa: E402

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///globalise_documents.db")

SCHEME_URIS = {
    "GLOBALISE": URIRef(
        "https://digitaalerfgoed.poolparty.biz/globalise/"
        "7a273a96-2e11-4307-b68d-8046b4455a4b"
    ),
    "TANAP": URIRef(
        "https://digitaalerfgoed.poolparty.biz/globalise/"
        "321974b0-c2a1-46be-9830-ff8bc7e9cc88"
    ),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uuid_from_uri(uri: URIRef) -> str | None:
    """Extract the UUID that forms the last path segment of a PoolParty URI.

    E.g. https://digitaalerfgoed.poolparty.biz/globalise/1a2b3c4d-... → '1a2b3c4d-...'
    Returns None when the segment is not a valid UUID.
    """
    segment = str(uri).rstrip("/").rsplit("/", 1)[-1]
    try:
        return str(uuid.UUID(segment))
    except ValueError:
        return None


def _pref_label(graph: Graph, concept: URIRef, lang: str) -> str | None:
    """Return the skos:prefLabel for *concept* in *lang*, or None."""
    for obj in graph.objects(concept, SKOS.prefLabel):
        if isinstance(obj, Literal) and obj.language == lang:
            return str(obj)
    return None


# ---------------------------------------------------------------------------
# Core import logic
# ---------------------------------------------------------------------------


def load_thesaurus(ttl_path: str) -> Graph:
    print(f"Parsing {ttl_path} …")
    g = Graph()
    g.parse(ttl_path, format="turtle")
    print(f"  Loaded {len(g)} triples.")
    return g


def extract_concepts(graph: Graph) -> list[dict]:
    """Return a list of dicts, one per concept in the two target schemes."""
    records: list[dict] = []

    for scheme_name, scheme_uri in SCHEME_URIS.items():
        # Collect concepts via skos:inScheme *and* skos:hasTopConcept / narrower
        # Using inScheme is the most reliable predicate.
        concepts_in_scheme: set[URIRef] = set()

        for concept in graph.subjects(SKOS.inScheme, scheme_uri):
            if isinstance(concept, URIRef):
                concepts_in_scheme.add(concept)

        # Also catch concepts declared directly on the scheme via hasTopConcept
        for concept in graph.objects(scheme_uri, SKOS.hasTopConcept):
            if isinstance(concept, URIRef):
                concepts_in_scheme.add(concept)

        print(f"  {scheme_name}: {len(concepts_in_scheme)} concepts found.")

        for concept_uri in sorted(concepts_in_scheme, key=str):
            concept_id = _uuid_from_uri(concept_uri)
            if concept_id is None:
                print(f"    ⚠ Skipping non-UUID concept: {concept_uri}")
                continue

            records.append(
                {
                    "id": concept_id,
                    "scheme": scheme_name,
                    "pref_label_nl": _pref_label(graph, concept_uri, "nl"),
                    "pref_label_en": _pref_label(graph, concept_uri, "en"),
                }
            )

    return records


def import_document_types(ttl_path: str, database_url: str) -> dict:
    """Parse the thesaurus and upsert DocumentType rows into the database.

    Returns a dict with counts: created, updated, skipped.
    """
    graph = load_thesaurus(ttl_path)

    print("\nExtracting concepts …")
    records = extract_concepts(graph)
    print(f"  Total concepts to import: {len(records)}")

    if not records:
        print("  Nothing to import — check scheme URIs or TTL content.")
        return {"created": 0, "updated": 0, "skipped": 0}

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    engine = create_engine(database_url, echo=False)
    Base.metadata.create_all(engine)  # creates document_type table if absent
    Session = sessionmaker(bind=engine)
    session = Session()

    stats = {"created": 0, "updated": 0, "skipped": 0}

    try:
        # Pre-load existing rows to decide create vs update
        existing: dict[str, DocumentType] = {
            dt.id: dt for dt in session.query(DocumentType).all()
        }

        for rec in records:
            concept_id = rec["id"]

            if concept_id in existing:
                obj = existing[concept_id]
                changed = False
                for field in ("scheme", "pref_label_nl", "pref_label_en"):
                    if getattr(obj, field) != rec[field]:
                        setattr(obj, field, rec[field])
                        changed = True
                if changed:
                    stats["updated"] += 1
                else:
                    stats["skipped"] += 1
            else:
                session.add(DocumentType(**rec))
                stats["created"] += 1

        session.commit()

    except Exception as exc:
        session.rollback()
        raise
    finally:
        session.close()

    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Import SKOS document-type concepts from the GLOBALISE thesaurus."
    )
    parser.add_argument(
        "--ttl",
        default=None,
        help=(
            "Path to the TTL file. "
            "Defaults to data/pp_project_globalisethesaurus.ttl "
            "relative to this script."
        ),
    )
    parser.add_argument(
        "--database",
        default=DATABASE_URL,
        help=f"SQLAlchemy database URL (default: {DATABASE_URL})",
    )
    args = parser.parse_args()

    if args.ttl:
        ttl_path = args.ttl
    else:
        script_dir = Path(__file__).parent
        ttl_path = str(script_dir / "data" / "pp_project_globalisethesaurus.ttl")

    if not os.path.exists(ttl_path):
        print(f"✗ TTL file not found: {ttl_path}")
        sys.exit(1)

    print("=" * 60)
    print("GLOBALISE Thesaurus — Document Type Import")
    print("=" * 60)

    stats = import_document_types(ttl_path, args.database)

    print("\n=== Result ===")
    print(f"  Created : {stats['created']}")
    print(f"  Updated : {stats['updated']}")
    print(f"  Unchanged: {stats['skipped']}")
    print("✓ Done.")


if __name__ == "__main__":
    main()
