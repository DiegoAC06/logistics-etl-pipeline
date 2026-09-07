"""
Tests for the schema drift guard in etl.ensure_schema().

Plain asserts, no test framework -- the project has no test dependency and
this doesn't need one.

    venv\\Scripts\\python.exe tests\\test_schema_drift.py

Everything runs against temporary copies. The real database and the real
sql\\schema.sql are never touched.
"""

import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import etl  # noqa: E402

REAL_SCHEMA = (ROOT / "sql" / "schema.sql").read_text(encoding="utf-8")

passed = failed = 0


def check(label, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"   PASS  {label}")
    else:
        failed += 1
        print(f"   FAIL  {label}  {detail}")


def fresh_db(tmp, schema_text):
    """Build a database from the given schema text and return its path."""
    etl.SCHEMA_PATH = tmp / "schema.sql"
    etl.SCHEMA_PATH.write_text(schema_text, encoding="utf-8")
    etl.DB_PATH = tmp / "test.db"
    etl.DB_PATH.unlink(missing_ok=True)
    conn = etl.connect()
    etl.ensure_schema(conn)
    conn.close()
    return etl.DB_PATH


def user_version(path):
    c = sqlite3.connect(path)
    v = c.execute("PRAGMA user_version").fetchone()[0]
    c.close()
    return v


def run():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        print("\n1. a fresh database gets stamped")
        db = fresh_db(tmp, REAL_SCHEMA)
        stamped = user_version(db)
        check("user_version is non-zero after creation", stamped != 0, f"got {stamped}")
        check("stamp equals the schema fingerprint",
              stamped == etl.schema_fingerprint(), f"{stamped} vs {etl.schema_fingerprint()}")

        print("\n2. an unchanged schema passes")
        conn = etl.connect()
        try:
            etl.ensure_schema(conn)
            check("no error when schema.sql is untouched", True)
        except etl.SchemaDriftError as e:
            check("no error when schema.sql is untouched", False, str(e)[:60])
        finally:
            conn.close()

        print("\n3. a REAL schema change is detected")
        etl.SCHEMA_PATH.write_text(
            REAL_SCHEMA.replace("'customs',", "'customs', 'act_of_god',"),
            encoding="utf-8")
        conn = etl.connect()
        raised = None
        try:
            etl.ensure_schema(conn)
        except etl.SchemaDriftError as e:
            raised = e
        finally:
            conn.close()
        check("SchemaDriftError raised", raised is not None)
        if raised:
            msg = str(raised)
            check("message names schema.sql", "schema.sql" in msg)
            check("message gives the delete command",
                  "del data\\processed\\logistics.db" in msg)
            check("message says nothing was deleted for you",
                  "that call is yours" in msg)
            check("message shows both fingerprints",
                  str(user_version(db)) in msg and str(etl.schema_fingerprint()) in msg)

        print("\n4. the database survives a detected drift")
        check("database file still exists", db.exists())
        check("stamp unchanged", user_version(db) == stamped)
        c = sqlite3.connect(db)
        tables = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        c.close()
        check("tables still present", {"warehouses", "orders", "shipments"} <= tables)

        print("\n5. a comment-only edit also fires (hashing the raw file)")
        etl.SCHEMA_PATH.write_text("-- an added comment\n" + REAL_SCHEMA, encoding="utf-8")
        conn = etl.connect()
        raised = None
        try:
            etl.ensure_schema(conn)
        except etl.SchemaDriftError as e:
            raised = e
        finally:
            conn.close()
        check("comment change detected (false positive, by design)", raised is not None)

        print("\n6. restoring the file clears the drift")
        etl.SCHEMA_PATH.write_text(REAL_SCHEMA, encoding="utf-8")
        conn = etl.connect()
        try:
            etl.ensure_schema(conn)
            check("no error once schema.sql is back", True)
        except etl.SchemaDriftError as e:
            check("no error once schema.sql is back", False, str(e)[:60])
        finally:
            conn.close()

        print("\n7. a database built before this guard is caught, not assumed good")
        c = sqlite3.connect(db)
        c.execute("PRAGMA user_version = 0")     # what an older build looks like
        c.close()
        conn = etl.connect()
        raised = None
        try:
            etl.ensure_schema(conn)
        except etl.SchemaDriftError as e:
            raised = e
        finally:
            conn.close()
        check("unstamped database raises", raised is not None)
        if raised:
            check("message explains it predates the guard",
                  "before schema drift detection" in str(raised))

        print("\n8. rebuilding after a change stamps the new fingerprint")
        changed = REAL_SCHEMA.replace("'customs',", "'customs', 'act_of_god',")
        db2 = fresh_db(tmp, changed)
        check("new stamp differs from the old one",
              user_version(db2) != stamped,
              f"{user_version(db2)} vs {stamped}")
        check("new stamp matches the new file",
              user_version(db2) == etl.schema_fingerprint())


if __name__ == "__main__":
    print("=" * 62)
    print("  schema drift detection")
    print("=" * 62)
    run()
    print("\n" + "=" * 62)
    print(f"  {passed} passed, {failed} failed")
    print("=" * 62)
    raise SystemExit(1 if failed else 0)
