import shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest
from pipeline import connect, load_batch, checks, BATCHES, RAW

def q(con, sql, *args):
    return con.execute(sql, list(args)).fetchall()

def build():
    con = connect()
    for b, d in BATCHES:
        load_batch(con, b, d)
    return con

@pytest.fixture()
def con():
    return build()

def test_all_quality_checks_pass(con):
    assert [c for c in checks(con) if c[1] != 0] == []

def test_rerun_changes_nothing():
    con = build()
    tables = ["silver_customers", "silver_orders", "silver_order_items", "dim_customer", "fact_sales", "bronze_orders"]
    before = {t: q(con, f"select count(*), coalesce(sum(hash(t)), 0) from {t} t")[0] for t in tables}
    for b, d in BATCHES:
        assert load_batch(con, b, d) is False
    after = {t: q(con, f"select count(*), coalesce(sum(hash(t)), 0) from {t} t")[0] for t in tables}
    assert before == after

def test_batch1_cleaning():
    con = connect()
    load_batch(con, "batch1", "2026-01-01")
    assert q(con, "select count(*) from bronze_customers")[0][0] == 201      # includes the duplicate
    assert q(con, "select count(*) from silver_customers")[0][0] == 200
    blanks = q(con, "select count(*) from bronze_order_items where unit_price is null or unit_price = ''")[0][0]
    assert blanks > 0
    assert q(con, "select count(*) from silver_order_items where unit_price is null")[0][0] == 0
    rejected = q(con, "select count(*) from silver_rejected_orders")[0][0]
    assert rejected > 0
    assert q(con, "select count(*) from silver_orders")[0][0] + rejected == 1500

def test_scd2_versions(con):
    # 8 customers moved -> 2 versions each; the rest, including customers re-sent unchanged, -> 1 version
    versions = dict(q(con, "select customer_id, count(*) from dim_customer group by 1"))
    assert sum(1 for v in versions.values() if v == 2) == 8
    assert all(v in (1, 2) for v in versions.values())
    assert len(versions) == 210
    assert q(con, "select count(*) from dim_customer where is_current")[0][0] == 210

def test_old_facts_keep_old_customer_version(con):
    """Facts are never silently re-pointed: an order placed before the move still shows the old city."""
    mover = q(con, "select customer_id from dim_customer group by 1 having count(*) = 2 order by 1 limit 1")[0][0]
    old_city, new_city = [r[0] for r in q(con, "select city from dim_customer where customer_id=? order by valid_from", mover)]
    assert old_city != new_city
    cities_by_period = q(con, """select o.order_date < DATE '2026-11-15', c.city
                                 from fact_sales f join dim_customer c using (customer_key)
                                 join silver_orders o using (order_id) where o.customer_id = ?""", mover)
    for before_move, city in cities_by_period:
        assert city == (old_city if before_move else new_city)

def test_batch2_does_not_touch_batch1_fact_keys():
    con = connect()
    load_batch(con, "batch1", "2026-01-01")
    before = q(con, "select order_id, product_key, customer_key, quantity, line_amount from fact_sales order by all")
    load_batch(con, "batch2", "2026-11-15")
    after = q(con, "select order_id, product_key, customer_key, quantity, line_amount from fact_sales where order_id <= 1500 order by all")
    assert before == after

def test_status_change_updates_status_only(con):
    changed = q(con, """select count(*) from bronze_orders b2 join bronze_orders b1 using (order_id)
                        where b1.batch_id='batch1' and b2.batch_id='batch2' and b1.status <> b2.status""")[0][0]
    assert changed == 10
    assert q(con, """select count(*) from fact_sales f join silver_orders o using (order_id) where f.status <> o.status""")[0][0] == 0

def test_failed_batch_leaves_nothing_behind(tmp_path):
    raw = tmp_path / "raw"
    shutil.copytree(RAW, raw)
    (raw / "batch2" / "orders.csv").write_text("order_id,customer_id,order_date,status\n1501,1,not-a-date,delivered\n")
    con = connect()
    load_batch(con, "batch1", "2026-01-01", raw)
    n_fact = q(con, "select count(*) from fact_sales")[0][0]
    with pytest.raises(Exception):
        load_batch(con, "batch2", "2026-11-15", raw)
    assert q(con, "select count(*) from fact_sales")[0][0] == n_fact
    assert q(con, "select count(*) from bronze_orders where batch_id='batch2'")[0][0] == 0
    assert q(con, "select count(*) from bronze_batches")[0][0] == 1
