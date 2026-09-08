"""Generate sample/assets.parquet — a small fixed-asset fixture for diff analysis.

Shaped like the SAP extract the Difference Analysis tab is built for: several
rows per asset, one per depreciation area. Every column exists to pin down one
verdict, so a test can assert the classification outright:

    asset_id          the grouping key
    depr_area         the explanatory column, '01' / '02' / '03'
    cost_center       CONSTANT               identical in every row of an asset
    optional_note     CONSTANT               present-and-identical, or wholly absent
    serial_no         SPARSE_SINGLE_VALUE    one row carries it, the others NULL
    blank_spaces      SPARSE_SINGLE_VALUE    the blank rows are whitespace, not NULL
    useful_life       TRUE_DIFF_BY_DEPR_AREA differs, but never inside one area
    last_changed_by   TRUE_DIFF_OTHER        differs inside one area as well
    reserved_field    ALL_BLANK              empty string in every row
    reserved_null     ALL_BLANK              NULL in every row

Every fifth asset gets a fourth row repeating depreciation area '01'. That is
what breaks the grain, and it is deliberately the *only* way a column can differ
within an area — which is the point the fixture has to make. The two columns
behave differently on that extra row: `useful_life` agrees with the '01' row it
duplicates and stays explained by the area, while `last_changed_by` disagrees
with it and does not. A test that could not tell those apart would not be
testing the attribution pass at all.

    .venv/bin/python tests/make_assets.py [asset_count]
"""

import os
import sys

import duckdb

ASSETS = int(sys.argv[1]) if len(sys.argv) > 1 else 500
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "sample", "assets.parquet")
os.makedirs(os.path.dirname(OUT), exist_ok=True)

# `a` is the asset ordinal, `d` the depreciation-area ordinal 0..2.
SQL = """
COPY (
  SELECT
    'A-' || lpad(a::VARCHAR, 6, '0')         AS asset_id,
    lpad((d + 1)::VARCHAR, 2, '0')           AS depr_area,
    'CC-' || lpad((a % 40)::VARCHAR, 4, '0') AS cost_center,
    CASE WHEN a % 3 = 0 THEN 'NOTE-' || lpad(a::VARCHAR, 6, '0') END AS optional_note,
    CASE WHEN d = 1 THEN 'SN-' || lpad(a::VARCHAR, 8, '0') END AS serial_no,
    CASE WHEN d = 2 THEN 'X' ELSE '   ' END  AS blank_spaces,
    (60 + d * 12)::BIGINT                    AS useful_life,
    CASE WHEN d = 0 THEN 'USER_A' ELSE 'USER_B' END AS last_changed_by,
    ''                                       AS reserved_field,
    NULL::VARCHAR                            AS reserved_null
  FROM range(?) t(a), range(3) u(d)

  UNION ALL

  -- Every fifth asset gets a second '01' row. `useful_life` matches the row it
  -- duplicates (60, as area '01' always is), so the area still explains that
  -- column; `last_changed_by` does not, so that column does not.
  SELECT
    'A-' || lpad(a::VARCHAR, 6, '0'), '01',
    'CC-' || lpad((a % 40)::VARCHAR, 4, '0'),
    CASE WHEN a % 3 = 0 THEN 'NOTE-' || lpad(a::VARCHAR, 6, '0') END,
    NULL, '   ', 60::BIGINT, 'USER_Z', '', NULL::VARCHAR
  FROM range(?) t(a) WHERE a % 5 = 0
) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""

duckdb.connect().execute(SQL.format(out=OUT.replace("'", "''")), [ASSETS, ASSETS])

rows = duckdb.connect().execute(
    "SELECT count(*) FROM read_parquet('{}')".format(OUT.replace("'", "''"))).fetchone()[0]
print("wrote {} — {:,} assets, {:,} rows ({} duplicated depreciation-area pairs)".format(
    OUT, ASSETS, rows, (ASSETS + 4) // 5))
