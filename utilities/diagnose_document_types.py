#!/usr/bin/env python3
"""
Diagnostic script — checks the state of document-type linkage in the database.
Run with: uv run python diagnose_document_types.py
"""
import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///globalise_documents.db")
engine = create_engine(DATABASE_URL, echo=False)

CHECKS = [
    ("document_type table exists",
     "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='document_type'"),
    ("document2documenttype table exists",
     "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='document2documenttype'"),
    ("document_type rows",
     "SELECT COUNT(*) FROM document_type"),
    ("document_type rows — GLOBALISE scheme",
     "SELECT COUNT(*) FROM document_type WHERE scheme='GLOBALISE'"),
    ("document_type rows — TANAP scheme",
     "SELECT COUNT(*) FROM document_type WHERE scheme='TANAP'"),
    ("document2documenttype rows",
     "SELECT COUNT(*) FROM document2documenttype"),
    ("documents with method 'TANAP Digitized Index'",
     "SELECT COUNT(*) FROM document d JOIN document_identification_method m ON d.method_id=m.id WHERE m.name='TANAP Digitized Index'"),
    ("documents with at least one document2documenttype link",
     "SELECT COUNT(DISTINCT document_id) FROM document2documenttype"),
]

print("=" * 60)
print("Document-type linkage diagnostics")
print(f"DB: {DATABASE_URL}")
print("=" * 60)

with engine.connect() as conn:
    for label, sql in CHECKS:
        try:
            result = conn.execute(text(sql)).scalar()
            print(f"  {label:55s} {result}")
        except Exception as e:
            print(f"  {label:55s} ERROR: {e}")

print("=" * 60)
print()
print("What to look for:")
print("  • Both tables should exist (value = 1)")
print("  • document_type should have rows (scripts 5 must have run)")
print("  • document2documenttype should have rows (script 6 must have run AFTER")
print("    Document2DocumentType was added to models.py)")
print("  • If document2documenttype = 0 but TANAP documents exist,")
print("    drop those documents and re-run script 6.")
