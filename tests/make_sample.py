"""Generate sample/orders.parquet — 3,000,000 rows for trying the app out.

    .venv/bin/python tests/make_sample.py [row_count]
"""

import os
import sys
import time

import duckdb

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 3_000_000
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "sample", "orders.parquet")
os.makedirs(os.path.dirname(OUT), exist_ok=True)

started = time.time()
duckdb.connect().execute("""
COPY (
  SELECT
    i AS id,
    ['north','south','east','west','central'][(i % 5) + 1] AS region,
    ['Widget','Gadget','Doohickey','Sprocket','Cog','Gizmo'][(i % 6) + 1] AS product,
    ['pending','shipped','delivered','returned'][(i % 4) + 1] AS status,
    CASE WHEN i % 97 = 0 THEN NULL ELSE round((random()*990+10)::DOUBLE, 2) END AS amount,
    (i % 500) + 1 AS quantity,
    DATE '2023-01-01' + INTERVAL (i % 900) DAY AS order_date,
    TIMESTAMP '2023-01-01 00:00:00' + INTERVAL (i % 900000) MINUTE AS created_at,
    (i % 7 = 0) AS is_priority,
    'CUST-' || lpad(((i * 7919) % 50000)::VARCHAR, 6, '0') AS customer_id,
    CASE WHEN i % 53 = 0 THEN NULL ELSE 'Note for order ' || i::VARCHAR END AS notes
  FROM range(?) t(i)
) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
""".format(OUT.replace("'", "''")), [ROWS])

print("wrote {} — {:,} rows, {:.1f} MB in {:.1f}s".format(
    OUT, ROWS, os.path.getsize(OUT) / 1e6, time.time() - started))
