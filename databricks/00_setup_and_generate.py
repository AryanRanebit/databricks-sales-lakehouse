# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup and generate fake data
# MAGIC Creates the schema and a volume (a folder for files) in Unity Catalog, then writes two batches of CSV files into it.
# MAGIC Safe to re-run: it overwrites the same files with the same seeded data.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sales_lakehouse")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sales_lakehouse.raw")

# COMMAND ----------

import os
os.environ["RAW_DIR"] = "/Volumes/workspace/sales_lakehouse/raw"

# COMMAND ----------

# The generator below is the same file as data/generate_data.py in the repo.
"""Create a small, fake retail dataset in two daily/weekly batches, with deliberate problems.

Everything is invented here (seeded, repeatable). No third-party dataset, so no licence issue.

batch1 = the first full load.
batch2 = a later delivery: new orders, a few orders whose status changed, some customers
         who moved city, new customers, and rows re-sent unchanged.
Planted mess: duplicate rows, messy city text, missing prices, orders for a customer that
does not exist.
"""
import csv, os, random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(os.environ.get("RAW_DIR") or Path(__file__).parent / "raw")
CITIES = ["Mumbai", "Thane", "Pune", "Kalyan", "Nashik"]
CATS = {"Stationery": (20, 300), "Electronics": (300, 5000), "Home": (100, 1500)}

def write(batch, name, header, rows):
    d = OUT / batch
    d.mkdir(parents=True, exist_ok=True)
    with open(d / name, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def messy(city):
    if random.random() < 0.1:
        return random.choice([city.lower(), city.upper(), " " + city])
    return city

# ---------- batch 1 ----------
customers = {}
for i in range(1, 201):
    city = random.choice(CITIES)
    customers[i] = [i, f"Customer {i}", city, (date(2025, 1, 1) + timedelta(days=random.randint(0, 300))).isoformat()]
b1_customers = [[c[0], c[1], messy(c[2]), c[3]] for c in customers.values()]
b1_customers.append(b1_customers[4])  # exact duplicate row

products, pid = [], 1
for cat, (lo, hi) in CATS.items():
    for _ in range(15):
        products.append([pid, f"{cat} item {pid}", cat, round(random.uniform(lo, hi), 2)])
        pid += 1

def make_orders(first_id, n, start, span, max_cust):
    orders, items = [], []
    for oid in range(first_id, first_id + n):
        cust = random.randint(1, max_cust)
        if random.random() < 0.01:
            cust = 999  # orphan: customer does not exist
        d = start + timedelta(days=random.randint(0, span))
        status = random.choice(["delivered"] * 8 + ["cancelled", "returned"])
        orders.append([oid, cust, d.isoformat(), status])
        for _ in range(random.randint(1, 4)):
            p = random.choice(products)
            price = p[3] if random.random() > 0.02 else ""  # missing price sometimes
            items.append([oid, p[0], random.randint(1, 5), price])
    return orders, items

o1, i1 = make_orders(1, 1500, date(2026, 1, 1), 300, 200)   # Jan 1 .. Oct 28
write("batch1", "customers.csv", ["customer_id", "name", "city", "signup_date"], b1_customers)
write("batch1", "products.csv", ["product_id", "product_name", "category", "list_price"], products)
write("batch1", "orders.csv", ["order_id", "customer_id", "order_date", "status"], o1)
write("batch1", "order_items.csv", ["order_id", "product_id", "quantity", "unit_price"], i1)

# ---------- batch 2 ----------
movers = random.sample(range(1, 201), 8)
b2_customers = []
for i in range(1, 201):
    if i in movers:
        new_city = random.choice([c for c in CITIES if c != customers[i][2]])
        customers[i][2] = new_city
        b2_customers.append(customers[i])      # changed row
    elif i % 10 == 0:
        b2_customers.append(customers[i])      # re-sent, unchanged: must NOT create a new version
for i in range(201, 211):                      # brand new customers
    customers[i] = [i, f"Customer {i}", random.choice(CITIES), "2026-11-01"]
    b2_customers.append(customers[i])

b2_products = [list(p) for p in products]
b2_products[0][3] = round(b2_products[0][3] * 1.1, 2)    # two price changes (overwritten, Type 1)
b2_products[20][3] = round(b2_products[20][3] * 1.1, 2)

o2, i2 = make_orders(1501, 200, date(2026, 10, 29), 52, 210)   # Oct 29 .. Dec 20
changed = random.sample(o1, 10)                # old orders whose status changed
for o in changed:
    o2.append([o[0], o[1], o[2], "returned" if o[3] != "returned" else "delivered"])

write("batch2", "customers.csv", ["customer_id", "name", "city", "signup_date"], b2_customers)
write("batch2", "products.csv", ["product_id", "product_name", "category", "list_price"], b2_products)
write("batch2", "orders.csv", ["order_id", "customer_id", "order_date", "status"], o2)
write("batch2", "order_items.csv", ["order_id", "product_id", "quantity", "unit_price"], i2)
print("batch1 orders/items:", len(o1), len(i1), "| batch2 orders/items:", len(o2), len(i2), "| movers:", sorted(movers))


# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/sales_lakehouse/raw/batch1"))
display(dbutils.fs.ls("/Volumes/workspace/sales_lakehouse/raw/batch2"))
