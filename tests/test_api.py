"""API + filter-semantics tests.

Every filter is checked against the same question asked of DuckDB directly, so
the SQL the filter builder emits has to agree with hand-written SQL.

    .venv/bin/python tests/test_api.py [base_url] [parquet_path]
"""

import json
import os
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8777"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET = os.path.abspath(sys.argv[2]) if len(sys.argv) > 2 else os.path.join(ROOT, "sample", "orders.parquet")

passed, failed = 0, 0


def call(path, body=None, method=None):
    verb = method or ("POST" if body is not None else "GET")
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        BASE + path, data=data, method=verb, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return {"HTTP_ERROR": error.code, "detail": json.loads(error.read()).get("detail")}


def check(label, got, want):
    global passed, failed
    if got == want:
        passed += 1
        print("  ok   {}".format(label))
    else:
        failed += 1
        print("  FAIL {}\n         got  {!r}\n         want {!r}".format(label, got, want))


def diff_checks(call, con):
    """Difference analysis, cross-checked against hand-written SQL on the fixture.

    The fixture is built so each column has exactly one defensible verdict, so
    every assertion here is "the classifier agrees with the SQL that defines the
    classification" rather than "the classifier agrees with itself".
    """
    import time

    assets_path = os.path.join(ROOT, "sample", "assets.parquet")
    if not os.path.exists(assets_path):
        print("\ndifference analysis\n  skipped — run: python tests/make_assets.py")
        return
    src = "read_parquet('{}')".format(assets_path.replace("'", "''"))

    print("\ndifference analysis (API vs. direct SQL)")
    dataset = call("/api/open", {"path": assets_path})
    did = dataset["id"]

    job = call("/api/diff/start", {"dataset_id": did, "key_column": "asset_id",
                                   "explain_column": "depr_area",
                                   "exclude_all_blank": False})
    check("analysis starts as a job", "id" in job, True)
    deadline = time.time() + 120
    status = job
    while status["status"] in ("queued", "running") and time.time() < deadline:
        time.sleep(0.1)
        status = call("/api/diff/status/{}".format(job["id"]))
    check("analysis completes", status["status"], "done")
    check("every column reported", status["done"], status["total"])
    check("partial results stream while it runs", len(status["results"]), status["total"])

    result = call("/api/diff/results/{}".format(job["id"]))
    verdicts = {c["column"]: c["verdict"] for c in result["columns"]}
    by_column = {c["column"]: c for c in result["columns"]}

    # The SQL that *defines* each bucket, per asset.
    def bucket(column, having):
        return con.sql(
            "SELECT count(*) FROM (SELECT asset_id FROM {src} GROUP BY 1 HAVING {having})".format(
                src=src, having=having.format(
                    v="NULLIF(TRIM(CAST({} AS VARCHAR)), '')".format(column)))).fetchone()[0]

    total_assets = con.sql("SELECT count(DISTINCT asset_id) FROM {}".format(src)).fetchone()[0]
    check("assets analysed matches the file", result["assets_analysed"], total_assets)

    check("a column identical across an asset's rows is CONSTANT",
          verdicts.get("cost_center"), "CONSTANT")
    check("  its constant count matches SQL", by_column["cost_center"]["constant"],
          bucket("cost_center", "count(DISTINCT {v}) = 1 AND count({v}) = count(*)"))

    check("a column present on one row and blank on the rest is SPARSE_SINGLE_VALUE",
          verdicts.get("serial_no"), "SPARSE_SINGLE_VALUE")
    check("  its sparse count matches SQL", by_column["serial_no"]["sparse"],
          bucket("serial_no", "count(DISTINCT {v}) = 1 AND count({v}) < count(*)"))

    # Whitespace has to count as blank, or this column looks like a true difference.
    check("whitespace counts as blank, not as a value",
          verdicts.get("blank_spaces"), "SPARSE_SINGLE_VALUE")
    check("  and its blank rows are not counted as a second value",
          by_column["blank_spaces"]["max_distinct"], 1)

    # Assets that are wholly blank contradict nothing, so they must not make a
    # column look sparse.
    check("wholly-blank assets do not make a column SPARSE",
          verdicts.get("optional_note"), "CONSTANT")
    check("  they land in the blank bucket instead", by_column["optional_note"]["blank"],
          bucket("optional_note", "count({v}) = 0"))

    check("a column varying only between areas is TRUE_DIFF_BY_DEPR_AREA",
          verdicts.get("useful_life"), "TRUE_DIFF_BY_DEPR_AREA")
    check("  no asset conflicts within one area",
          by_column["useful_life"]["conflicting_assets"], 0)
    check("  its differing count matches SQL", by_column["useful_life"]["differing"],
          bucket("useful_life", "count(DISTINCT {v}) > 1"))
    check("  max distinct matches SQL", by_column["useful_life"]["max_distinct"],
          con.sql("SELECT max(n) FROM (SELECT count(DISTINCT useful_life) AS n FROM {} "
                  "GROUP BY asset_id)".format(src)).fetchone()[0])

    check("a column varying inside one area is TRUE_DIFF_OTHER",
          verdicts.get("last_changed_by"), "TRUE_DIFF_OTHER")
    sql_conflicting = con.sql(
        "SELECT count(DISTINCT asset_id) FROM (SELECT asset_id, depr_area FROM {} "
        "GROUP BY 1, 2 HAVING count(DISTINCT NULLIF(TRIM(last_changed_by), '')) > 1)".format(
            src)).fetchone()[0]
    check("  the assets it names match SQL",
          by_column["last_changed_by"]["conflicting_assets"], sql_conflicting)

    check("an empty-string column is ALL_BLANK", verdicts.get("reserved_field"), "ALL_BLANK")
    check("a NULL column is ALL_BLANK", verdicts.get("reserved_null"), "ALL_BLANK")

    # The grain finding is reported on its own, not folded into a column.
    grain = result["duplicate_grain"]
    sql_pairs = con.sql(
        "SELECT count(*) FROM (SELECT asset_id, depr_area FROM {} GROUP BY 1, 2 "
        "HAVING count(*) > 1)".format(src)).fetchone()[0]
    check("duplicate (asset, area) pairs are reported", grain["duplicate_pairs"], sql_pairs)
    check("and the grain is flagged as not unique", grain["unique"], False)

    # Percentages are of assets analysed, not of rows.
    constant = by_column["cost_center"]
    check("percentages are of assets analysed", constant["constant_pct"],
          round(100.0 * constant["constant"] / constant["assets"], 2))

    print("\ndifference analysis options")
    blanked = call("/api/diff/start", {"dataset_id": did, "key_column": "asset_id",
                                       "explain_column": "depr_area",
                                       "exclude_all_blank": True, "exclude": ["cost_center"]})
    status = blanked
    deadline = time.time() + 120
    while status["status"] in ("queued", "running") and time.time() < deadline:
        time.sleep(0.1)
        status = call("/api/diff/status/{}".format(blanked["id"]))
    excluded = call("/api/diff/results/{}".format(blanked["id"]))
    reasons = {e["column"]: e["reason"] for e in excluded["excluded"]}
    check("the key column is excluded from its own analysis",
          reasons.get("asset_id"), "Grouping key")
    check("a requested exclusion is honoured and explained",
          reasons.get("cost_center"), "Excluded by the run configuration")
    check("all-blank columns can be excluded", reasons.get("reserved_null"),
          "Blank in every row")
    check("and they no longer appear as results",
          any(c["column"] == "reserved_null" for c in excluded["columns"]), False)

    sampled = call("/api/diff/start", {"dataset_id": did, "key_column": "asset_id",
                                       "explain_column": "depr_area", "sample_assets": 50,
                                       "exclude_all_blank": False})
    status = sampled
    deadline = time.time() + 120
    while status["status"] in ("queued", "running") and time.time() < deadline:
        time.sleep(0.1)
        status = call("/api/diff/status/{}".format(sampled["id"]))
    preview = call("/api/diff/results/{}".format(sampled["id"]))
    check("sample mode analyses only the sampled assets", preview["assets_analysed"], 50)
    check("and reaches the same verdicts",
          {c["column"]: c["verdict"] for c in preview["columns"]}, verdicts)

    print("\ndifference analysis drill-down")
    examples = call("/api/diff/examples/{}/last_changed_by?limit=2".format(job["id"]))
    check("examples come back for a differing column", len(examples["assets"]), 2)
    check("every row of the example asset is shown",
          len(examples["assets"][0]["rows"]),
          con.sql("SELECT count(*) FROM {} WHERE asset_id = '{}'".format(
              src, examples["assets"][0]["asset"])).fetchone()[0])
    check("the example is flagged as differing", examples["assets"][0]["differs"], True)
    check("more pages are offered", examples["has_more"], True)
    one = call("/api/diff/examples/{}/last_changed_by?asset_id=A-000005".format(job["id"]))
    check("jumping to one asset returns just that asset",
          [a["asset"] for a in one["assets"]], ["A-000005"])

    print("\ndifference analysis rejects bad input")
    check("unknown key column", call("/api/diff/start", {
        "dataset_id": did, "key_column": "nope"})["HTTP_ERROR"], 400)
    check("key and explanatory column the same", call("/api/diff/start", {
        "dataset_id": did, "key_column": "asset_id",
        "explain_column": "asset_id"})["HTTP_ERROR"], 400)
    check("injection in the key column", call("/api/diff/start", {
        "dataset_id": did, "key_column": 'x"; DROP TABLE t; --'})["HTTP_ERROR"], 400)
    check("results before the run finishes", call(
        "/api/diff/results/nosuchjob")["HTTP_ERROR"], 404)

    # A column that cannot be analysed must not take the other 69 with it. Asked
    # of the module directly: there is no way to make the API produce a column
    # the file does not have, which is the point -- the guard is for the odd
    # types a 70-column SAP extract turns up, not for bad input.
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from app import diffanalysis as diff_module
    from app.engine import Engine as _Engine
    local = _Engine()
    local_ds = local.open(assets_path)
    config = diff_module.DiffConfig(key_column="asset_id", explain_column="depr_area")
    plan = diff_module.prepare(local, local_ds, config)
    broken = diff_module.analyse_column(local, local_ds, config, plan, "no_such_column")
    plan.release(local)
    check("a column that cannot be analysed is marked ERROR", broken.verdict, "ERROR")
    check("and it carries the reason", bool(broken.error), True)

    print("\ndifference analysis export")
    for fmt, sheets in (("xlsx", 4), ("csv", 1)):
        export = call("/api/diff/export/{}".format(job["id"]), {"format": fmt})
        deadline = time.time() + 180
        while export["status"] in ("queued", "counting", "running") and time.time() < deadline:
            time.sleep(0.2)
            export = call("/api/export/{}".format(export["id"]))
        check("{} export completes".format(fmt), export["status"], "done")
        check("{} export is tagged as a diff".format(fmt), export["kind"], "diff")
        check("{} export writes a row per column".format(fmt), export["written"],
              len(result["columns"]))
        check("{} file is non-empty".format(fmt), export["size"] > 0, True)


def main():
    import duckdb

    if not os.path.exists(PARQUET):
        sys.exit("Sample file missing: {}\nRun: python tests/make_sample.py".format(PARQUET))

    con = duckdb.connect()
    src = "read_parquet('{}')".format(PARQUET.replace("'", "''"))

    dataset = call("/api/open", {"path": PARQUET})
    assert "id" in dataset, dataset
    did = dataset["id"]
    print("opened {} — {:,} rows, {} columns\n".format(
        dataset["name"], dataset["row_count"], len(dataset["columns"])))

    print("filter semantics (API vs. direct SQL)")
    cases = [
        ("in", [{"column": "region", "op": "in", "values": ["north", "east"]}],
         "region IN ('north','east')"),
        ("not_in keeps nulls", [{"column": "notes", "op": "not_in", "values": ["Note for order 5"]}],
         "(notes NOT IN ('Note for order 5') OR notes IS NULL)"),
        ("in including blanks", [{"column": "notes", "op": "in", "values": [None, "Note for order 5"]}],
         "(notes IN ('Note for order 5') OR notes IS NULL)"),
        ("pasted list, case-insensitive",
         [{"column": "region", "op": "in", "values": ["NORTH", "East"], "case_sensitive": False}],
         "lower(region) IN ('north','east')"),
        ("pasted list, match case", [{"column": "region", "op": "in", "values": ["NORTH", "east"],
                                      "case_sensitive": True}],
         "region IN ('NORTH','east')"),
        ("pasted numeric list", [{"column": "quantity", "op": "in", "values": ["10", "20", "30"]}],
         "quantity IN (10, 20, 30)"),
        ("numeric between", [{"column": "amount", "op": "between", "value": 100, "value2": 500}],
         "amount >= 100 AND amount <= 500"),
        ("open-ended range", [{"column": "quantity", "op": "between", "value": None, "value2": 50}],
         "quantity <= 50"),
        ("not_between excludes nulls", [{"column": "amount", "op": "not_between", "value": 100, "value2": 900}],
         "NOT (amount >= 100 AND amount <= 900) AND amount IS NOT NULL"),
        ("timestamp range to end of day",
         [{"column": "created_at", "op": "between",
           "value": "2023-03-01 00:00:00", "value2": "2023-03-31 23:59:59.999999"}],
         "created_at BETWEEN TIMESTAMP '2023-03-01 00:00:00' AND TIMESTAMP '2023-03-31 23:59:59.999999'"),
        ("contains (case-insensitive)", [{"column": "notes", "op": "contains", "value": "ORDER 12"}],
         "notes ILIKE '%ORDER 12%'"),
        ("not_contains keeps nulls", [{"column": "notes", "op": "not_contains", "value": "order 1"}],
         "(notes NOT ILIKE '%order 1%' OR notes IS NULL)"),
        ("starts_with", [{"column": "customer_id", "op": "starts_with", "value": "cust-0001"}],
         "customer_id ILIKE 'cust-0001%'"),
        ("wildcards are literal", [{"column": "notes", "op": "contains", "value": "%"}],
         "notes LIKE '%\\%%' ESCAPE '\\'"),
        ("boolean eq", [{"column": "is_priority", "op": "eq", "value": True}], "is_priority = true"),
        ("is_null", [{"column": "amount", "op": "is_null"}], "amount IS NULL"),
        ("is_not_null", [{"column": "amount", "op": "is_not_null"}], "amount IS NOT NULL"),
        ("five columns ANDed",
         [{"column": "region", "op": "in", "values": ["north", "east", "south"]},
          {"column": "status", "op": "not_in", "values": ["returned"]},
          {"column": "amount", "op": "between", "value": 50, "value2": 600},
          {"column": "quantity", "op": "gte", "value": 100},
          {"column": "notes", "op": "contains", "value": "order 9"}],
         """region IN ('north','east','south') AND (status NOT IN ('returned') OR status IS NULL)
            AND amount >= 50 AND amount <= 600 AND quantity >= 100 AND notes ILIKE '%order 9%'"""),
    ]
    for label, filters, where in cases:
        api_count = call("/api/count", {"dataset_id": did, "filters": filters}).get("count")
        sql_count = con.sql("SELECT count(*) FROM {} WHERE {}".format(src, where)).fetchone()[0]
        check("{} ({:,})".format(label, sql_count), api_count, sql_count)

    print("\npreview + metadata")
    preview = call("/api/preview", {"dataset_id": did, "filters": [], "limit": 10})
    check("preview returns 10 rows", len(preview["rows"]), 10)
    check("preview returns every column", len(preview["columns"]), len(dataset["columns"]))

    filters = [{"column": "region", "op": "in", "values": ["north"]}]
    preview = call("/api/preview", {"dataset_id": did, "filters": filters,
                                    "columns": ["id", "region"], "limit": 10})
    check("column projection", [c["name"] for c in preview["columns"]], ["id", "region"])
    check("projected rows still 10", len(preview["rows"]), 10)

    values = call("/api/values", {"dataset_id": did, "column": "region", "filters": []})
    check("distinct values", sorted(v["value"] for v in values["values"]),
          ["central", "east", "north", "south", "west"])
    check("value counts", values["values"][0]["count"], 600000)

    # A value list ignores its own column's filter (Excel autofilter behaviour).
    scoped = call("/api/values", {"dataset_id": did, "column": "region", "filters": filters})
    check("value list ignores its own filter", len(scoped["values"]), 5)
    scoped2 = call("/api/values", {"dataset_id": did, "column": "status",
                                   "filters": [{"column": "status", "op": "in", "values": ["shipped"]},
                                               {"column": "region", "op": "in", "values": ["north"]}]})
    check("value list respects other filters", len(scoped2["values"]), 4)

    stats = call("/api/stats", {"dataset_id": did, "column": "amount", "filters": []})
    sql_nulls = con.sql("SELECT count(*) - count(amount) FROM {}".format(src)).fetchone()[0]
    check("stats null count", stats["nulls"], sql_nulls)
    check("stats category", stats["category"], "numeric")

    print("\nrejects bad input")
    check("missing file", call("/api/open", {"path": "/nope/x.parquet"})["HTTP_ERROR"], 404)
    check("unknown column", call("/api/count", {"dataset_id": did, "filters": [
        {"column": 'x"; DROP TABLE t; --', "op": "eq", "value": 1}]})["HTTP_ERROR"], 400)
    check("unknown operator", call("/api/count", {"dataset_id": did, "filters": [
        {"column": "id", "op": "; DROP TABLE t", "value": 1}]})["HTTP_ERROR"], 400)
    check("empty value list", call("/api/count", {"dataset_id": did, "filters": [
        {"column": "region", "op": "in", "values": []}]})["HTTP_ERROR"], 400)
    check("stale dataset id", call("/api/preview", {"dataset_id": "nope", "filters": []})["HTTP_ERROR"], 404)
    check("injection in a value is inert", call("/api/count", {"dataset_id": did, "filters": [
        {"column": "region", "op": "eq", "value": "' OR 1=1 --"}]})["count"], 0)

    import time

    print("\npivot (API vs. direct SQL)")
    pivot = call("/api/pivot", {"dataset_id": did, "rows": ["region"], "columns": ["status"],
                                "values": [{"column": "amount", "agg": "sum"}], "filters": []})
    check("pivot leaf count (4 statuses + Total)", len(pivot["leaves"]), 5)
    check("pivot row count (5 regions + grand total)", len(pivot["rows"]), 6)
    check("pivot grand total row is last", pivot["rows"][-1]["kind"], "grand")

    sql_cell = con.sql(
        "SELECT sum(amount) FROM {} WHERE region='north' AND status='shipped'".format(src)
    ).fetchone()[0]
    north = next(r for r in pivot["rows"] if r["labels"][0] == "north")
    shipped_at = next(i for i, l in enumerate(pivot["leaves"]) if l["label"] == "shipped")
    check("cell matches direct SQL", round(north["cells"][shipped_at], 2), round(sql_cell, 2))

    sql_total = con.sql("SELECT sum(amount) FROM {} WHERE region='north'".format(src)).fetchone()[0]
    check("Total column matches direct SQL", round(north["cells"][-1], 2), round(sql_total, 2))
    sql_grand = con.sql("SELECT sum(amount) FROM {}".format(src)).fetchone()[0]
    check("grand total matches direct SQL", round(pivot["rows"][-1]["cells"][-1], 2), round(sql_grand, 2))

    # A total under Average has to come from the raw rows, not from the cells above it.
    avg = call("/api/pivot", {"dataset_id": did, "rows": ["region"], "columns": ["status"],
                              "values": [{"column": "amount", "agg": "avg"}], "filters": []})
    sql_avg = con.sql("SELECT avg(amount) FROM {}".format(src)).fetchone()[0]
    check("Average grand total is not an average of averages",
          round(avg["rows"][-1]["cells"][-1], 6), round(sql_avg, 6))

    # Subtotals for the outer field, one per group, aggregated from the raw rows.
    deep = call("/api/pivot", {"dataset_id": did, "rows": ["region", "product"], "columns": [],
                               "values": [{"agg": "count_rows"}], "filters": []})
    check("subtotal per outer group", sum(1 for r in deep["rows"] if r["kind"] == "subtotal"), 5)
    check("detail rows for every combination",
          sum(1 for r in deep["rows"] if r["kind"] == "data"), 30)
    sub = next(r for r in deep["rows"] if r["kind"] == "subtotal")
    sql_sub = con.sql("SELECT count(*) FROM {} WHERE region='central'".format(src)).fetchone()[0]
    check("subtotal matches direct SQL", sub["cells"][0], sql_sub)

    # Filters reach the pivot exactly as they reach the grid.
    filtered = call("/api/pivot", {"dataset_id": did, "rows": ["region"], "columns": [],
                                   "values": [{"agg": "count_rows"}],
                                   "filters": [{"column": "status", "op": "in", "values": ["shipped"]}]})
    sql_filtered = con.sql(
        "SELECT count(*) FROM {} WHERE status='shipped' AND region='north'".format(src)).fetchone()[0]
    north = next(r for r in filtered["rows"] if r["labels"][0] == "north")
    check("filters apply to the pivot", north["cells"][0], sql_filtered)

    # Sorting by a measure orders every level of the row hierarchy.
    sorted_pivot = call("/api/pivot", {"dataset_id": did, "rows": ["region"], "columns": [],
                                       "values": [{"column": "amount", "agg": "sum"}], "filters": [],
                                       "sort": {"by": "value", "value_index": 0, "descending": True}})
    order = [r["cells"][0] for r in sorted_pivot["rows"] if r["kind"] == "data"]
    check("largest-first sort", order, sorted(order, reverse=True))

    # A pivot far past the on-screen limit reports its real size instead of failing.
    huge = call("/api/pivot", {"dataset_id": did, "rows": ["customer_id", "product"],
                               "columns": ["status"],
                               "values": [{"column": "amount", "agg": "sum"}], "filters": []})
    sql_groups = con.sql(
        "SELECT count(*) FROM (SELECT customer_id, product FROM {} "
        "GROUP BY customer_id, product)".format(src)).fetchone()[0]
    check("an oversized pivot still builds", huge.get("HTTP_ERROR"), None)
    check("it reports the true row-group count", huge["total_row_groups"], sql_groups)
    check("it is flagged as truncated", huge["truncated"], True)
    check("it shows only the row groups that fit",
          huge["row_groups"] <= huge["row_group_limit"], True)
    # The promise the pivot makes about totals has to survive the truncation.
    sql_grand = con.sql("SELECT sum(amount) FROM {}".format(src)).fetchone()[0]
    check("its grand total still covers every matching row",
          round(huge["rows"][-1]["cells"][-1], 2), round(sql_grand, 2))
    # Truncating on a group boundary is what keeps the subtotals honest.
    last_group = [r for r in huge["rows"] if r["kind"] == "subtotal"][-1]["labels"][0]
    sql_sub = con.sql(
        "SELECT sum(amount) FROM {} WHERE customer_id = '{}'".format(
            src, last_group[:-len(" Total")])).fetchone()[0]
    check("the last subtotal shown covers its whole group",
          round(last_group and [r for r in huge["rows"] if r["kind"] == "subtotal"][-1]["cells"][-1], 2),
          round(sql_sub, 2))

    print("\npivot rejects bad input")
    check("field in both Rows and Columns", call("/api/pivot", {
        "dataset_id": did, "rows": ["region"], "columns": ["region"],
        "values": [{"agg": "count_rows"}]})["HTTP_ERROR"], 400)
    check("no value fields", call("/api/pivot", {
        "dataset_id": did, "rows": ["region"], "values": []})["HTTP_ERROR"], 400)
    check("nothing to group by", call("/api/pivot", {
        "dataset_id": did, "values": [{"agg": "count_rows"}]})["HTTP_ERROR"], 400)
    check("sum of a text column", call("/api/pivot", {
        "dataset_id": did, "rows": ["region"], "values": [{"column": "notes", "agg": "sum"}]})["HTTP_ERROR"], 400)
    check("too many column groups", call("/api/pivot", {
        "dataset_id": did, "rows": ["region"], "columns": ["customer_id"],
        "values": [{"agg": "count_rows"}]})["HTTP_ERROR"], 400)
    check("unknown aggregation", call("/api/pivot", {
        "dataset_id": did, "rows": ["region"], "values": [{"column": "amount", "agg": "hack"}]})["HTTP_ERROR"], 400)
    check("injection in a pivot field", call("/api/pivot", {
        "dataset_id": did, "rows": ['x"; DROP TABLE t; --'],
        "values": [{"agg": "count_rows"}]})["HTTP_ERROR"], 400)

    print("\nparquet export")

    def run_export(body):
        job = call("/api/export", dict({"dataset_id": did}, **body))
        deadline = time.time() + 180
        while job.get("status") in ("queued", "counting", "running") and time.time() < deadline:
            time.sleep(0.3)
            job = call("/api/export/{}".format(job["id"]))
        return job

    def download(job, into):
        target = os.path.join(into, job["filename"])
        with urllib.request.urlopen(BASE + "/api/export/{}/download".format(job["id"])) as response, \
                open(target, "wb") as handle:
            handle.write(response.read())
        return "read_parquet('{}')".format(target.replace("'", "''"))

    import tempfile
    with tempfile.TemporaryDirectory() as scratch:
        pq_filters = [{"column": "region", "op": "in", "values": ["north", "east"]},
                      {"column": "amount", "op": "gte", "value": 500}]
        job = run_export({"format": "parquet", "filters": pq_filters,
                          "columns": ["id", "region", "amount", "created_at"],
                          "order_by": "amount", "descending": True})
        check("parquet export completes", job["status"], "done")
        check("parquet file is named .parquet", job["filename"].endswith(".parquet"), True)
        out = download(job, scratch)
        want = con.sql("SELECT count(*) FROM {} WHERE region IN ('north','east') AND amount >= 500"
                       .format(src)).fetchone()[0]
        check("parquet holds every matching row ({:,})".format(want),
              con.sql("SELECT count(*) FROM {}".format(out)).fetchone()[0], want)
        check("parquet keeps only the chosen columns, in order",
              [r[0] for r in con.sql("DESCRIBE SELECT * FROM {}".format(out)).fetchall()],
              ["id", "region", "amount", "created_at"])
        check("parquet keeps the source column types",
              [r[1] for r in con.sql("DESCRIBE SELECT * FROM {}".format(out)).fetchall()],
              [r[1] for r in con.sql("DESCRIBE SELECT id, region, amount, created_at FROM {}"
                                     .format(src)).fetchall()])

        limited = run_export({"format": "parquet", "filters": pq_filters, "columns": ["id", "amount"],
                              "order_by": "amount", "descending": True, "row_limit": 25})
        out = download(limited, scratch)
        check("parquet honours the row limit",
              con.sql("SELECT count(*) FROM {}".format(out)).fetchone()[0], 25)
        check("and the limit takes the top rows of the sort",
              con.sql("SELECT min(amount) FROM {}".format(out)).fetchone()[0],
              con.sql("SELECT min(amount) FROM (SELECT amount FROM {} WHERE region IN ('north','east') "
                      "AND amount >= 500 ORDER BY amount DESC LIMIT 25)".format(src)).fetchone()[0])

        csv_job = run_export({"format": "csv", "filters": pq_filters, "row_limit": 10})
        with urllib.request.urlopen(BASE + "/api/export/{}/download".format(csv_job["id"])) as response:
            csv_lines = response.read().decode("utf-8").strip().splitlines()
        check("csv honours the row limit too", len(csv_lines) - 1, 10)

    print("\npivot export")
    job = call("/api/pivot/export", {"dataset_id": did, "rows": ["region", "product"],
                                     "columns": ["status"],
                                     "values": [{"column": "amount", "agg": "sum"},
                                                {"agg": "count_rows"}],
                                     "filters": [], "sheet_name": "Pivot"})
    deadline = time.time() + 180
    while job["status"] in ("queued", "counting", "running") and time.time() < deadline:
        time.sleep(0.3)
        job = call("/api/export/{}".format(job["id"]))
    check("pivot export completes", job["status"], "done")
    check("pivot export is tagged as a pivot", job["kind"], "pivot")
    check("pivot export writes every pivot row", job["written"], 36)
    check("pivot file is non-empty", job["size"] > 0, True)

    # Repeating the labels is a layout choice, so it must not change the numbers
    # or the row count -- only which label cells are written.
    repeated = call("/api/pivot/export", {"dataset_id": did, "rows": ["region", "product"],
                                          "columns": ["status"],
                                          "values": [{"column": "amount", "agg": "sum"},
                                                     {"agg": "count_rows"}],
                                          "filters": [], "sheet_name": "Pivot",
                                          "repeat_labels": True})
    deadline = time.time() + 180
    while repeated["status"] in ("queued", "counting", "running") and time.time() < deadline:
        time.sleep(0.3)
        repeated = call("/api/export/{}".format(repeated["id"]))
    check("repeat-labels export completes", repeated["status"], "done")
    check("repeat-labels export writes the same rows", repeated["written"], job["written"])

    # The pivot itself always carries the full labels; only the writers blank them.
    labelled = call("/api/pivot", {"dataset_id": did, "rows": ["region", "product"], "columns": [],
                                   "values": [{"agg": "count_rows"}], "filters": [],
                                   "repeat_labels": True})
    detail = [r["labels"] for r in labelled["rows"] if r["kind"] == "data"]
    check("every detail row carries its outer label",
          all(labels[0] and labels[1] for labels in detail), True)

    # Past one workbook's worth, the export splits into several files in a zip.
    split = call("/api/pivot/export", {"dataset_id": did, "rows": ["customer_id", "product"],
                                       "columns": [], "values": [{"agg": "count_rows"}],
                                       "filters": [], "sheet_name": "Pivot"})
    deadline = time.time() + 600
    while split["status"] in ("queued", "counting", "running") and time.time() < deadline:
        time.sleep(0.5)
        split = call("/api/export/{}".format(split["id"]))
    check("an oversized pivot export completes", split["status"], "done")
    check("it comes back as a zip", split["format"], "zip")
    check("it is split into more than one file", split["sheets"] > 1, True)
    check("it writes every row group", split["written"], sql_groups)
    check("the zip is non-empty", split["size"] > 0, True)

    diff_checks(call, con)

    print("\nexports")
    for fmt, expect_rows in (("xlsx", True), ("csv", True), ("parquet", True)):
        job = call("/api/export", {"dataset_id": did, "format": fmt,
                                   "filters": [{"column": "region", "op": "in", "values": ["north"]}],
                                   "columns": ["id", "region", "amount", "order_date"]})
        deadline = time.time() + 180
        while job["status"] in ("queued", "counting", "running") and time.time() < deadline:
            time.sleep(0.3)
            job = call("/api/export/{}".format(job["id"]))
        check("{} export completes".format(fmt), job["status"], "done")
        if expect_rows:
            check("{} export row count".format(fmt), job["written"], 600000)
        check("{} file is non-empty".format(fmt), job["size"] > 0, True)

    print("\n{} passed, {} failed".format(passed, failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
