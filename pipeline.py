"""Run the pipeline locally on DuckDB, one batch at a time: bronze -> silver -> gold -> checks.

    python pipeline.py            # loads batch1 then batch2 into pipeline.duckdb
    python pipeline.py            # run again: nothing changes (idempotent)

as_of is the date a batch's customer changes take effect (used by SCD2).
"""
import sys
from pathlib import Path
import duckdb

ROOT = Path(__file__).parent
RAW = ROOT / "data" / "raw"
BATCHES = [("batch1", "2026-01-01"), ("batch2", "2026-11-15")]


def sql(name, **params):
    text = (ROOT / "sql" / name).read_text()
    for k, v in params.items():
        text = text.replace("{" + k + "}", str(v))
    return text


def connect(db_path=":memory:"):
    con = duckdb.connect(str(db_path))
    con.execute(sql("00_setup.sql"))
    return con


def load_batch(con, batch, as_of, raw_root=RAW):
    """Load one batch. Returns False (and does nothing) if it was already loaded."""
    if con.execute("select count(*) from bronze_batches where batch_id = ?", [batch]).fetchone()[0]:
        return False
    params = dict(batch=batch, raw=Path(raw_root) / batch, as_of=as_of)
    con.execute("BEGIN")
    try:
        con.execute(sql("01_bronze.sql", **params))
        con.execute(sql("02_silver.sql", **params))
        con.execute(sql("03_gold.sql", **params))
        con.execute("insert into bronze_batches (batch_id, as_of) values (?, ?)", [batch, as_of])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return True


def checks(con):
    return con.execute(sql("04_checks.sql")).fetchall()


if __name__ == "__main__":
    con = connect(ROOT / "pipeline.duckdb")
    for batch, as_of in BATCHES:
        print(batch, "loaded" if load_batch(con, batch, as_of) else "already loaded, skipped")
    failed = 0
    for name, bad in checks(con):
        print("PASS" if bad == 0 else "FAIL", name, bad)
        failed += bad != 0
    print(con.execute(sql("05_report.sql")).df().head(10))
    sys.exit(1 if failed else 0)
