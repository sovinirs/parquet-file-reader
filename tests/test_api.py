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


def extract_checks(call, con, src, did):
    """Extracting a date part: grid, filter panel, filters and exports all agree with SQL."""
    import re
    import tempfile
    import time
    import zipfile

    print("\nextracting date parts")
    shown = call("/api/preview", {"dataset_id": did, "filters": [], "limit": 5,
                                  "columns": ["id", "order_date", "created_at"],
                                  "order_by": "id", "transforms": {"order_date": "year",
                                                                   "created_at": "month"}})
    check("an extracted column keeps its name and becomes an integer",
          [(c["name"], c["type"], c["category"]) for c in shown["columns"]],
          [("id", "BIGINT", "numeric"), ("order_date", "BIGINT", "numeric"),
           ("created_at", "BIGINT", "numeric")])
    check("the grid shows the year and the month",
          shown["rows"],
          [list(r) for r in con.sql("SELECT id, year(order_date), month(created_at) FROM {} "
                                    "ORDER BY id LIMIT 5".format(src)).fetchall()])
    day = call("/api/preview", {"dataset_id": did, "filters": [], "limit": 3, "columns": ["order_date"],
                                "order_by": "order_date", "descending": True,
                                "transforms": {"order_date": "day"}})
    check("day extracts the day of the month, and sorting follows it",
          [r[0] for r in day["rows"]], [31, 31, 31])

    years = call("/api/values", {"dataset_id": did, "column": "order_date", "filters": [],
                                 "transform": "year"})
    check("the filter panel lists years, in calendar order",
          [(v["value"], v["count"]) for v in years["values"]],
          [tuple(r) for r in con.sql("SELECT year(order_date), count(*) FROM {} GROUP BY 1 ORDER BY 1"
                                     .format(src)).fetchall()])
    stats = call("/api/stats", {"dataset_id": did, "column": "created_at", "filters": [],
                                "transform": "month"})
    check("range stats describe the months", (stats["lo"], stats["hi"], stats["category"]),
          (1, 12, "numeric"))

    cases = [
        ("year is any of", {"column": "order_date", "transform": "year", "op": "in", "values": [2024]},
         "year(order_date) = 2024"),
        ("month in a range", {"column": "created_at", "transform": "month", "op": "between",
                              "value": 3, "value2": 5}, "month(created_at) BETWEEN 3 AND 5"),
        ("pasted list of days", {"column": "order_date", "transform": "day", "op": "in",
                                 "values": ["1", "15"], "list": True}, "day(order_date) IN (1, 15)"),
    ]
    for label, spec, where in cases:
        check("filter on an extracted part: " + label,
              call("/api/count", {"dataset_id": did, "filters": [spec]})["count"],
              con.sql("SELECT count(*) FROM {} WHERE {}".format(src, where)).fetchone()[0])

    # The Pivot tab groups raw values, but a year filter still means year = 2024.
    pivot = call("/api/pivot", {"dataset_id": did, "rows": ["region"], "columns": [],
                                "values": [{"agg": "count_rows"}],
                                "filters": [cases[0][1]]})
    north = next(r for r in pivot["rows"] if r["labels"][0] == "north")
    check("the pivot honours a filter on an extracted part", north["cells"][0],
          con.sql("SELECT count(*) FROM {} WHERE region = 'north' AND year(order_date) = 2024"
                  .format(src)).fetchone()[0])

    check("only dates and timestamps can be extracted",
          call("/api/preview", {"dataset_id": did, "filters": [], "transforms": {"amount": "year"}})
          .get("HTTP_ERROR"), 400)
    check("only year, month and day are offered",
          call("/api/count", {"dataset_id": did, "filters": [
              {"column": "order_date", "transform": "week", "op": "eq", "value": 3}]}).get("HTTP_ERROR"), 400)

    def export(fmt, **extra):
        job = call("/api/export", dict({"dataset_id": did, "format": fmt, "include_manifest": False,
                                        "columns": ["id", "order_date"],
                                        "filters": [cases[0][1]],
                                        "order_by": "id", "row_limit": 50,
                                        "transforms": {"order_date": "year"}}, **extra))
        deadline = time.time() + 60
        while job.get("status") in ("queued", "counting", "running") and time.time() < deadline:
            time.sleep(0.2)
            job = call("/api/export/{}".format(job["id"]))
        target = os.path.join(scratch, job["filename"])
        with urllib.request.urlopen(BASE + "/api/export/{}/download".format(job["id"])) as response, \
                open(target, "wb") as handle:
            handle.write(response.read())
        return target

    with tempfile.TemporaryDirectory() as scratch:
        out = export("parquet")
        check("a parquet export writes the year, as an integer column",
              con.sql("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{}'))"
                      .format(out)).fetchall(), [("id", "BIGINT"), ("order_date", "BIGINT")])
        check("and every value is 2024",
              con.sql("SELECT DISTINCT order_date FROM read_parquet('{}')".format(out)).fetchall(),
              [(2024,)])

        out = export("xlsx")
        with zipfile.ZipFile(out) as book:
            sheet = book.read("xl/worksheets/sheet1.xml").decode("utf-8")
        cells = re.findall(r'<c r="B(\d+)"([^>]*)>(.*?)</c>', sheet)
        header = next(body for row, _, body in cells if row == "1")
        data = [(attrs, body) for row, attrs, body in cells if row != "1"]
        check("an excel export keeps the column header", "order_date" in header, True)
        check("and writes 50 plain numbers, not dates",
              (len(data), {body for _, body in data}, any(' s="' in attrs for attrs, _ in data)),
              (50, {"<v>2024</v>"}, False))

        manifest = export("xlsx", include_manifest=True)
        with zipfile.ZipFile(manifest) as book:
            text = " ".join(book.read(name).decode("utf-8") for name in book.namelist()
                            if name.startswith("xl/worksheets/") or name == "xl/sharedStrings.xml")
        check("the export info sheet records the extraction", "order_date → year only" in text, True)


def text_date_checks(call, con, src):
    """Dates stored as text (or YYYYMMDD integers) are spotted and can be extracted."""
    import tempfile
    import time

    print("\ndates stored as text")
    with tempfile.TemporaryDirectory() as scratch:
        path = os.path.join(scratch, "text_dates.parquet")
        con.execute("""COPY (SELECT id,
                strftime(order_date, '%d/%m/%Y') AS ship_text,
                strftime(order_date, '%Y-%m-%d') AS iso_text,
                CAST(strftime(order_date, '%Y%m%d') AS INTEGER) AS ymd_int,
                strftime(order_date, '%d-%b-%Y') AS mon_text,
                -- every day is 12 or under, so DD/MM and MM/DD both fit
                strftime(DATE '2024-01-01' + INTERVAL (id % 12) DAY, '%d/%m/%Y') AS ambiguous,
                CASE WHEN id % 10 = 0 THEN 'N/A' ELSE strftime(order_date, '%d/%m/%Y') END AS with_blanks,
                CASE WHEN id % 10 = 0 THEN 'soon' ELSE strftime(order_date, '%d/%m/%Y') END AS not_dates,
                region, quantity
            FROM {} WHERE id < 200000) TO '{}'""".format(src, path))
        dataset = call("/api/open", {"path": path})
        did = dataset["id"]
        found = {c["name"]: c.get("date_formats") for c in dataset["columns"]}
        check("DD/MM/YYYY text is spotted", found["ship_text"], ["%d/%m/%Y"])
        check("ISO text is spotted", found["iso_text"], ["iso"])
        check("YYYYMMDD integers are spotted", found["ymd_int"], ["%Y%m%d"])
        check("31-Jan-2024 text is spotted", found["mon_text"], ["%d-%b-%Y"])
        check("a sample that fits both layouts offers both, day-first first",
              found["ambiguous"], ["%d/%m/%Y", "%m/%d/%Y"])
        check("N/A placeholders don't hide a date column", found["with_blanks"], ["%d/%m/%Y"])
        check("a column with real non-dates is left alone", found["not_dates"], None)
        check("ordinary text and numbers are left alone", (found["region"], found["quantity"]), (None, None))

        shown = call("/api/preview", {"dataset_id": did, "filters": [], "limit": 4, "order_by": "id",
                                      "columns": ["id", "ship_text", "ymd_int", "mon_text"],
                                      "transforms": {"ship_text": {"part": "year", "format": "%d/%m/%Y"},
                                                     "ymd_int": {"part": "month", "format": "%Y%m%d"},
                                                     "mon_text": {"part": "day", "format": "%d-%b-%Y"}}})
        check("text dates extract to year, month and day",
              shown["rows"],
              [list(r) for r in con.sql("SELECT id, year(order_date), month(order_date), day(order_date) "
                                        "FROM {} ORDER BY id LIMIT 4".format(src)).fetchall()])

        spec = {"column": "ship_text", "transform": "year", "date_format": "%d/%m/%Y",
                "op": "in", "values": [2024]}
        want = con.sql("SELECT count(*) FROM {} WHERE id < 200000 AND year(order_date) = 2024"
                       .format(src)).fetchone()[0]
        check("a filter on a text date's year", call("/api/count", {"dataset_id": did, "filters": [spec]})["count"],
              want)
        blanks = call("/api/values", {"dataset_id": did, "column": "with_blanks", "filters": [],
                                      "transform": "month", "date_format": "%d/%m/%Y"})
        check("placeholders come out blank", blanks["values"][-1]["value"], None)

        # 01/02/2024 is 1 February read day-first, 2 January month-first.
        day_first = call("/api/values", {"dataset_id": did, "column": "ambiguous", "filters": [],
                                         "transform": "month", "date_format": "%d/%m/%Y"})
        month_first = call("/api/values", {"dataset_id": did, "column": "ambiguous", "filters": [],
                                           "transform": "month", "date_format": "%m/%d/%Y"})
        check("the chosen layout decides how 01/02 reads",
              ([v["value"] for v in day_first["values"]], len(month_first["values"])), ([1], 12))

        check("a text column needs a format to extract from",
              call("/api/preview", {"dataset_id": did, "filters": [],
                                    "transforms": {"ship_text": "year"}}).get("HTTP_ERROR"), 400)
        check("only known formats are accepted",
              call("/api/count", {"dataset_id": did, "filters": [dict(spec, date_format="%d' OR 1=1 --")]})
              .get("HTTP_ERROR"), 400)

        job = call("/api/export", {"dataset_id": did, "format": "parquet", "filters": [spec],
                                   "columns": ["id", "ship_text"], "row_limit": 10,
                                   "transforms": {"ship_text": {"part": "year", "format": "%d/%m/%Y"}}})
        deadline = time.time() + 60
        while job.get("status") in ("queued", "counting", "running") and time.time() < deadline:
            time.sleep(0.2)
            job = call("/api/export/{}".format(job["id"]))
        out = os.path.join(scratch, "out.parquet")
        with urllib.request.urlopen(BASE + "/api/export/{}/download".format(job["id"])) as response, \
                open(out, "wb") as handle:
            handle.write(response.read())
        check("a text date exports as its year, an integer",
              con.sql("SELECT DISTINCT typeof(ship_text), ship_text FROM read_parquet('{}')".format(out))
              .fetchall(), [("BIGINT", 2024)])
        call("/api/dataset/{}".format(did), method="DELETE")


def union_checks(call, con, src):
    """Two halves of the sample, unioned back, must behave exactly like the whole."""
    import tempfile
    import time

    print("\nunion")
    with tempfile.TemporaryDirectory() as scratch:
        first = os.path.join(scratch, "orders_early.parquet")
        second = os.path.join(scratch, "orders_late.parquet")
        retyped = os.path.join(scratch, "orders_retyped.parquet")
        con.execute("COPY (SELECT * FROM {} WHERE id < 1000000) TO '{}'".format(src, first))
        # Same columns in a different order: matched by name, so still a union.
        con.execute("COPY (SELECT * EXCLUDE (amount), amount FROM {} WHERE id >= 1000000) TO '{}'"
                    .format(src, second))
        con.execute("COPY (SELECT * REPLACE (CAST(amount AS VARCHAR) AS amount) FROM {} LIMIT 5) TO '{}'"
                    .format(src, retyped))

        union = call("/api/union", {"paths": [first, second]})
        uid = union.get("id")
        total = con.sql("SELECT count(*) FROM {}".format(src)).fetchone()[0]
        check("union stacks every row", union.get("row_count"), total)
        check("union is named after its files", union.get("name"),
              "orders_early.parquet + orders_late.parquet")
        check("union adds a source_file column", union["columns"][-1],
              {"name": "source_file", "type": "VARCHAR", "category": "text"})
        values = call("/api/values", {"dataset_id": uid, "column": "source_file", "filters": []})
        check("source_file holds each file's name and row count",
              sorted((v["value"], v["count"]) for v in values["values"]),
              [("orders_early.parquet", 1000000), ("orders_late.parquet", total - 1000000)])

        filters = [{"column": "source_file", "op": "in", "values": ["orders_late.parquet"]},
                   {"column": "region", "op": "in", "values": ["north"]}]
        check("filters work across the union, source_file included",
              call("/api/count", {"dataset_id": uid, "filters": filters})["count"],
              con.sql("SELECT count(*) FROM {} WHERE id >= 1000000 AND region = 'north'"
                      .format(src)).fetchone()[0])

        pivot = call("/api/pivot", {"dataset_id": uid, "rows": ["source_file"], "columns": [],
                                    "values": [{"column": "amount", "agg": "sum"}], "filters": []})
        early = next(r for r in pivot["rows"] if r["labels"][0] == "orders_early.parquet")
        check("pivot groups by source_file",
              round(early["cells"][0], 2),
              round(con.sql("SELECT sum(amount) FROM {} WHERE id < 1000000".format(src)).fetchone()[0], 2))

        job = call("/api/export", {"dataset_id": uid, "format": "parquet", "filters": filters,
                                   "columns": ["id", "source_file"], "row_limit": 3})
        deadline = time.time() + 60
        while job["status"] in ("queued", "counting", "running") and time.time() < deadline:
            time.sleep(0.2)
            job = call("/api/export/{}".format(job["id"]))
        target = os.path.join(scratch, "out.parquet")
        with urllib.request.urlopen(BASE + "/api/export/{}/download".format(job["id"])) as response, \
                open(target, "wb") as handle:
            handle.write(response.read())
        check("a union exports to parquet with source_file",
              con.sql("SELECT DISTINCT source_file FROM read_parquet('{}')".format(target)).fetchall(),
              [("orders_late.parquet",)])

        plain = call("/api/union", {"paths": [first, second], "source_column": False})
        check("source_file can be left out",
              [c["name"] for c in plain["columns"]][-1] != "source_file", True)

        bad = call("/api/union", {"paths": [first, retyped]})
        check("a type mismatch is refused", bad.get("HTTP_ERROR"), 400)
        check("and the error names the column", "amount (DOUBLE vs VARCHAR)" in (bad.get("detail") or ""), True)
        check("the same file twice is refused", call("/api/union", {"paths": [first, first]}).get("HTTP_ERROR"), 400)
        check("one file is not a union", call("/api/union", {"paths": [first]}).get("HTTP_ERROR"), 400)
        check("a missing file is reported",
              call("/api/union", {"paths": [first, first + ".nope"]}).get("HTTP_ERROR"), 404)

        for dataset_id in (uid, plain.get("id")):
            call("/api/dataset/{}".format(dataset_id), method="DELETE")


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

    pasted = call("/api/values", {"dataset_id": did, "column": "region", "filters": [],
                                  "exact": ["NORTH", "east", "atlantis"]})
    check("a pasted list looks values up exactly, ignoring case",
          sorted(v["value"] for v in pasted["values"]), ["east", "north"])
    pasted_num = call("/api/values", {"dataset_id": did, "column": "quantity", "filters": [],
                                      "exact": ["10", "20"]})
    check("a pasted list works on a numeric column",
          sorted(v["value"] for v in pasted_num["values"]), [10, 20])

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

    extract_checks(call, con, src, did)
    text_date_checks(call, con, src)
    union_checks(call, con, src)

    print("\n{} passed, {} failed".format(passed, failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
