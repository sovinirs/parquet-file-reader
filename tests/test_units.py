"""Unit tests: every function in app/, called directly -- no server.

Each test builds its expectation independently, usually by asking DuckDB the
same question in hand-written SQL over the same fixture, so a passing test
means the code agrees with the definition rather than with itself.

    .venv/bin/python -m unittest tests.test_units -v
"""

import asyncio
import datetime as _dt
import decimal
import io
import json
import os
import re
import shutil
import tempfile
import time
import unittest
import uuid
import zipfile
from unittest import mock

import duckdb
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app import exporter as exporter_module
from app import filters as F
from app import main as M
from app import pivot as P
from app.engine import Column, Dataset, Engine, categorise, to_jsonable
from app.exporter import ExportJob, ExportManager

ROWS = 200
TMP = None       # scratch folder for the whole module
FIXTURE = None   # path of the main fixture file
con = None       # an independent DuckDB connection: the oracle


def setUpModule():
    """One fixture file whose columns hit every type and edge the app handles."""
    global TMP, FIXTURE, con
    TMP = tempfile.mkdtemp(prefix="pqs-units-")
    FIXTURE = os.path.join(TMP, "fixture.parquet")
    con = duckdb.connect()
    con.execute("""
        COPY (SELECT
            i AS id,
            CASE WHEN i % 10 = 9 THEN NULL ELSE ['north', 'south', 'east', 'west'][i % 4 + 1] END AS region,
            CASE WHEN i % 7 = 0 THEN '' ELSE 'Item ' || i END AS label,
            CASE WHEN i % 11 = 0 THEN NULL ELSE i * 1.5 END::DOUBLE AS amount,
            (i % 9)::INTEGER AS qty,
            (i * 3)::DECIMAL(10, 2) AS price,
            i % 2 = 0 AS flag,
            (DATE '2023-01-01' + INTERVAL (i * 5) DAY)::DATE AS day_date,
            TIMESTAMP '2023-01-01 06:30:00' + INTERVAL (i * 37) HOUR AS ts,
            TIME '08:00:00' + INTERVAL (i) MINUTE AS clock,
            strftime(DATE '2023-01-01' + INTERVAL (i * 5) DAY, '%d/%m/%Y') AS dmy_text,
            CAST(strftime(DATE '2023-01-01' + INTERVAL (i * 5) DAY, '%Y%m%d') AS INTEGER) AS ymd_int,
            [i, i + 1] AS tags,
            {{'a': i}} AS info,
            'x%_' || i AS weird
        FROM range({rows}) t(i)) TO '{path}'
    """.format(rows=ROWS, path=FIXTURE))


def tearDownModule():
    shutil.rmtree(TMP, ignore_errors=True)


def sql(query):
    """Run an oracle query over the fixture; `T` stands for the table."""
    return con.sql(re.sub(r"\bT\b", "read_parquet('{}')".format(FIXTURE), query, count=1)).fetchall()


def scalar(query):
    return sql(query)[0][0]


def wait(job, timeout=60):
    deadline = time.time() + timeout
    while job.status in ("queued", "counting", "running") and time.time() < deadline:
        time.sleep(0.05)
    return job


def read_xlsx(path):
    """{sheet name: {cell ref: value}} from an .xlsx, enough to check what was written."""
    out = {}
    with zipfile.ZipFile(path) as book:
        names = book.namelist()
        shared = []
        if "xl/sharedStrings.xml" in names:
            xml = book.read("xl/sharedStrings.xml").decode("utf-8")
            shared = [re.sub(r"<[^>]+>", "", si) for si in re.findall(r"<si>(.*?)</si>", xml, re.S)]
        workbook = book.read("xl/workbook.xml").decode("utf-8")
        titles = re.findall(r'<sheet name="([^"]+)"', workbook)
        for index, title in enumerate(titles, start=1):
            xml = book.read("xl/worksheets/sheet{}.xml".format(index)).decode("utf-8")
            cells = {}
            for ref, attrs, body in re.findall(r'<c r="([A-Z]+\d+)"([^>]*?)(?:/>|>(.*?)</c>)', xml, re.S):
                kind = re.search(r't="(\w+)"', attrs)
                kind = kind.group(1) if kind else None
                value = re.search(r"<v>(.*?)</v>", body or "")
                if kind == "s":
                    cells[ref] = shared[int(value.group(1))]
                elif kind == "inlineStr":
                    cells[ref] = re.sub(r"<[^>]+>", "", body)
                elif kind == "str":
                    cells[ref] = value.group(1) if value else ""
                elif value:
                    number = float(value.group(1))
                    cells[ref] = int(number) if number.is_integer() else number
                cells[ref + "@style"] = re.search(r' s="(\d+)"', attrs) is not None
            out[title.replace("&amp;", "&")] = cells
    return out


def column_values(cells, letter):
    """A sheet column's data cells, top to bottom, header excluded."""
    rows = sorted(int(ref[len(letter):]) for ref in cells
                  if ref.startswith(letter) and ref[len(letter):].isdigit())
    return [cells[letter + str(r)] for r in rows if r > 1]


class Base(unittest.TestCase):
    """A fresh engine with the fixture open."""

    def setUp(self):
        self.engine = Engine(threads=2)
        self.ds = self.engine.open(FIXTURE)

    def count(self, *filters):
        return self.engine.count(self.ds, list(filters))


# =================================================================== filters.py

class TestFilterHelpers(unittest.TestCase):

    def test_quote_ident_escapes_quotes(self):
        self.assertEqual(F.quote_ident("a"), '"a"')
        self.assertEqual(F.quote_ident('we"ird'), '"we""ird"')

    def test_can_transform_only_calendar_types(self):
        for t in ("DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "timestamp_ns"):
            self.assertTrue(F.can_transform(t), t)
        for t in ("TIME", "INTERVAL", "VARCHAR", "BIGINT"):
            self.assertFalse(F.can_transform(t), t)

    def test_format_label_covers_every_known_format(self):
        self.assertEqual(F.format_label("iso"), "YYYY-MM-DD")
        self.assertEqual(F.format_label("%d/%m/%Y"), "DD/MM/YYYY")
        self.assertEqual(F.format_label("%m/%d/%y"), "MM/DD/YY")
        self.assertEqual(F.format_label("%d/%m/%Y %H:%M:%S"), "DD/MM/YYYY hh:mm:ss")
        self.assertEqual(F.format_label("%d %B %Y"), "DD Month YYYY")
        self.assertEqual(F.format_label("%b %d, %Y"), "Mon DD, YYYY")
        for fmt in F.DATE_FORMATS:
            self.assertNotIn("%", F.format_label(fmt), fmt)

    def test_parse_date_sql_reads_each_format(self):
        samples = {"iso": "2024-01-31", "%d/%m/%Y": "31/01/2024", "%m/%d/%Y": "01/31/2024",
                   "%Y%m%d": "20240131", "%d-%b-%Y": "31-Jan-2024", "%b %d, %Y": "Jan 31, 2024"}
        for fmt, text in samples.items():
            expr = F.parse_date_sql("?", fmt)
            got = con.execute("SELECT {}".format(expr), [text]).fetchone()[0]
            self.assertEqual(got.date(), _dt.date(2024, 1, 31), fmt)
            self.assertIsNone(con.execute("SELECT {}".format(expr), ["nonsense"]).fetchone()[0], fmt)

    def test_parse_date_sql_trims_and_refuses_unknown_formats(self):
        got = con.execute("SELECT {}".format(F.parse_date_sql("?", "%d/%m/%Y")), ["  31/01/2024 "]).fetchone()[0]
        self.assertEqual(got.date(), _dt.date(2024, 1, 31))
        with self.assertRaises(F.FilterError):
            F.parse_date_sql('"c"', "%d' OR 1=1 --")

    def test_date_shapes_are_necessary_not_sufficient(self):
        shapes = F.DATE_SHAPES
        self.assertEqual(set(shapes), set(F.DATE_FORMATS))
        # Every value DuckDB can read must pass its shape, or dates would be missed.
        samples = {"iso": ["2024-01-31", "2024/1/31", "2024-01-31 10:11:12.5", "2024-01-31T10:11:12+05:30"],
                   "%d/%m/%Y": ["31/01/2024", "3/4/2024"], "%d-%b-%Y": ["31-Jan-2024", "1-sep-2024"],
                   "%b %d, %Y": ["Jan 31, 2024"], "%d %B %Y": ["31 January 2024"],
                   "%d/%m/%Y %H:%M:%S": ["31/01/2024 10:11:12"], "%Y%m%d": ["20240131"],
                   "%Y%m%d%H%M%S": ["20240131101112"], "%d.%m.%Y": ["31.01.2024"]}
        for fmt, texts in samples.items():
            for text in texts:
                self.assertTrue(shapes[fmt].match(text), (fmt, text))
                parsed = con.execute("SELECT {}".format(F.parse_date_sql("?", fmt)), [text]).fetchone()[0]
                self.assertIsNotNone(parsed, (fmt, text))
        # ...and ordinary text fails every shape at once.
        for text in ("CODE-2024-01", "1.2.3", "cfcd208495d565ef66e7dff9f98764da", "north", "12345", "2024"):
            self.assertFalse([f for f, rx in shapes.items() if rx.match(text)], text)

    def test_column_sql(self):
        self.assertEqual(F.column_sql("c", "BIGINT"), ('"c"', "BIGINT"))
        self.assertEqual(F.column_sql("d", "DATE", "year"), ('year("d")', "BIGINT"))
        expr, typ = F.column_sql("t", "VARCHAR", "month", "%d/%m/%Y")
        self.assertEqual(typ, "BIGINT")
        self.assertTrue(expr.startswith("month(try_strptime("), expr)
        with self.assertRaises(F.FilterError):
            F.column_sql("d", "DATE", "week")
        with self.assertRaises(F.FilterError):
            F.column_sql("t", "VARCHAR", "year")          # text needs a format
        with self.assertRaises(F.FilterError):
            F.column_sql("x", "DOUBLE", "year")          # not a date at all

    def test_type_helpers(self):
        self.assertTrue(F._is_text_type("VARCHAR"))
        self.assertTrue(F._is_text_type("uuid"))
        self.assertFalse(F._is_text_type("BIGINT"))
        self.assertTrue(F._castable("DOUBLE"))
        for t in ("STRUCT(a INT)", "MAP(VARCHAR, INT)", "INTEGER[]", "BLOB", "LIST"):
            self.assertFalse(F._castable(t), t)
        self.assertEqual(F._operand("VARCHAR"), "?")
        self.assertEqual(F._operand("INTEGER[]"), "?")
        self.assertEqual(F._operand("DATE"), "CAST(? AS DATE)")
        self.assertEqual(F._text_of('"c"', "VARCHAR"), '"c"')
        self.assertEqual(F._text_of('"c"', "BIGINT"), 'CAST("c" AS VARCHAR)')


class TestBuildPredicate(Base):
    """Every operator, checked by counting rows against hand-written SQL."""

    def check(self, spec, where):
        self.assertEqual(self.count(spec), scalar("SELECT count(*) FROM T WHERE " + where),
                         "{} vs {}".format(spec, where))

    def test_set_ops(self):
        self.check({"column": "region", "op": "in", "values": ["north", "east"]},
                   "region IN ('north', 'east')")
        self.check({"column": "region", "op": "in", "values": ["north", None]},
                   "region = 'north' OR region IS NULL")
        self.check({"column": "region", "op": "in", "values": [F.NULL_TOKEN]}, "region IS NULL")
        self.check({"column": "region", "op": "not_in", "values": ["north"]},
                   "region <> 'north' OR region IS NULL")
        self.check({"column": "region", "op": "not_in", "values": ["north", None]},
                   "region <> 'north' AND region IS NOT NULL")
        self.check({"column": "qty", "op": "in", "values": ["1", "2"]}, "qty IN (1, 2)")
        self.check({"column": "day_date", "op": "in", "values": ["2023-01-06"]},
                   "day_date = DATE '2023-01-06'")

    def test_set_ops_case_folding(self):
        self.check({"column": "region", "op": "in", "values": ["NORTH"], "case_sensitive": False},
                   "region = 'north'")
        self.check({"column": "region", "op": "in", "values": ["NORTH"], "case_sensitive": True},
                   "FALSE")
        self.check({"column": "region", "op": "in", "values": ["NORTH"]}, "FALSE")

    def test_comparisons(self):
        for op, sym in (("eq", "="), ("ne", "<>"), ("gt", ">"), ("gte", ">="), ("lt", "<"), ("lte", "<=")):
            self.check({"column": "amount", "op": op, "value": 150}, "amount {} 150".format(sym))
        self.check({"column": "day_date", "op": "gte", "value": "2024-01-01"}, "day_date >= DATE '2024-01-01'")
        self.check({"column": "label", "op": "eq", "value": "Item 3"}, "label = 'Item 3'")
        self.check({"column": "flag", "op": "eq", "value": True}, "flag")
        self.check({"column": "price", "op": "lt", "value": "30.5"}, "price < 30.5")

    def test_ranges(self):
        self.check({"column": "amount", "op": "between", "value": 30, "value2": 90},
                   "amount BETWEEN 30 AND 90")
        self.check({"column": "amount", "op": "between", "value": None, "value2": 90}, "amount <= 90")
        self.check({"column": "amount", "op": "between", "value": 30, "value2": ""}, "amount >= 30")
        self.check({"column": "amount", "op": "not_between", "value": 30, "value2": 90},
                   "NOT (amount BETWEEN 30 AND 90) AND amount IS NOT NULL")
        self.check({"column": "ts", "op": "between", "value": "2023-02-01 00:00:00",
                    "value2": "2023-02-28 23:59:59.999999"},
                   "ts BETWEEN TIMESTAMP '2023-02-01' AND TIMESTAMP '2023-02-28 23:59:59.999999'")

    def test_text_matching(self):
        self.check({"column": "label", "op": "contains", "value": "ITEM 1"}, "label ILIKE '%item 1%'")
        self.check({"column": "label", "op": "contains", "value": "ITEM 1", "case_sensitive": True},
                   "label LIKE '%ITEM 1%'")
        self.check({"column": "label", "op": "not_contains", "value": "1"},
                   "label NOT ILIKE '%1%' OR label IS NULL")
        self.check({"column": "label", "op": "starts_with", "value": "item 1"}, "label ILIKE 'item 1%'")
        self.check({"column": "label", "op": "ends_with", "value": "5"}, "label ILIKE '%5'")
        self.check({"column": "label", "op": "regex", "value": "^Item [0-9]$"},
                   "regexp_matches(label, '^Item [0-9]$')")
        self.check({"column": "qty", "op": "contains", "value": "3"}, "CAST(qty AS VARCHAR) LIKE '%3%'")

    def test_wildcards_are_literal(self):
        self.check({"column": "weird", "op": "starts_with", "value": "x%_1"}, "weird LIKE 'x\\%\\_1%' ESCAPE '\\'")
        self.check({"column": "label", "op": "contains", "value": "%"}, "FALSE")
        self.check({"column": "label", "op": "contains", "value": "_"}, "FALSE")

    def test_nullary(self):
        self.check({"column": "amount", "op": "is_null"}, "amount IS NULL")
        self.check({"column": "amount", "op": "is_not_null"}, "amount IS NOT NULL")
        self.check({"column": "label", "op": "is_empty"}, "label IS NULL OR label = ''")
        self.check({"column": "label", "op": "is_not_empty"}, "label IS NOT NULL AND label <> ''")
        self.check({"column": "region", "op": "is_empty"}, "region IS NULL")

    def test_default_op_is_in(self):
        self.check({"column": "region", "values": ["west"]}, "region = 'west'")

    def test_transforms_in_filters(self):
        self.check({"column": "day_date", "transform": "year", "op": "in", "values": [2024]},
                   "year(day_date) = 2024")
        self.check({"column": "ts", "transform": "month", "op": "between", "value": 3, "value2": 4},
                   "month(ts) BETWEEN 3 AND 4")
        self.check({"column": "dmy_text", "transform": "day", "date_format": "%d/%m/%Y",
                    "op": "eq", "value": 1}, "day(day_date) = 1")
        self.check({"column": "ymd_int", "transform": "year", "date_format": "%Y%m%d",
                    "op": "gte", "value": 2024}, "year(day_date) >= 2024")

    def test_errors(self):
        bad = [
            {"column": "region", "op": "sounds_like", "value": "x"},
            {"column": "region", "op": "in", "values": "north"},
            {"column": "region", "op": "in", "values": []},
            {"column": "amount", "op": "between"},
            {"column": "amount", "op": "gt"},
            {"column": "amount", "op": "gt", "value": ""},
            {"column": "amount", "transform": "year", "op": "eq", "value": 1},
        ]
        for spec in bad:
            with self.assertRaises(F.FilterError, msg=spec):
                F.build_predicate(spec, self.ds.column_types[spec["column"]])

    def test_values_are_bound_not_inlined(self):
        sql_text, params = F.build_predicate(
            {"column": "label", "op": "eq", "value": "'; DROP TABLE x; --"}, "VARCHAR")
        self.assertNotIn("DROP", sql_text)
        self.assertEqual(params, ["'; DROP TABLE x; --"])
        self.assertEqual(self.count({"column": "label", "op": "eq", "value": "'; DROP TABLE x; --"}), 0)


class TestBuildWhere(Base):

    def test_empty(self):
        self.assertEqual(F.build_where([], self.ds.column_types), ("", []))
        self.assertEqual(F.build_where(None, self.ds.column_types), ("", []))

    def test_filters_are_anded(self):
        specs = [{"column": "region", "op": "in", "values": ["north"]},
                 {"column": "amount", "op": "gt", "value": 50}]
        where, params = F.build_where(specs, self.ds.column_types)
        self.assertTrue(where.startswith("WHERE "))
        self.assertEqual(where.count(" AND "), 1)
        self.assertEqual(self.count(*specs),
                         scalar("SELECT count(*) FROM T WHERE region = 'north' AND amount > 50"))

    def test_skip_column_and_disabled(self):
        specs = [{"column": "region", "op": "in", "values": ["north"]},
                 {"column": "qty", "op": "eq", "value": 1, "enabled": False}]
        self.assertEqual(F.build_where(specs, self.ds.column_types, skip_column="region"), ("", []))
        where, _ = F.build_where(specs, self.ds.column_types)
        self.assertNotIn('"qty"', where)

    def test_unknown_column(self):
        with self.assertRaises(F.FilterError):
            F.build_where([{"column": "nope", "op": "is_null"}], self.ds.column_types)


class TestDescribe(unittest.TestCase):

    def test_every_shape(self):
        self.assertEqual(F.describe({"column": "a", "op": "is_null"}), "a is null")
        self.assertEqual(F.describe({"column": "a", "op": "in", "values": ["x", None]}),
                         "a is any of [x, (blank)]")
        self.assertEqual(F.describe({"column": "a", "op": "not_in", "values": [F.NULL_TOKEN]}),
                         "a is none of [(blank)]")
        self.assertEqual(F.describe({"column": "a", "op": "between", "value": 1, "value2": 2}),
                         "a between 1 .. 2")
        self.assertEqual(F.describe({"column": "a", "op": "starts_with", "value": "q"}), "a starts with q")
        self.assertEqual(F.describe({"column": "d", "transform": "year", "op": "eq", "value": 2024}),
                         "d (year) eq 2024")
        many = F.describe({"column": "a", "op": "in", "values": list(range(20))})
        self.assertIn("(+8 more)", many)


# ==================================================================== engine.py

class TestEngineHelpers(unittest.TestCase):

    def test_categorise(self):
        cases = {"BOOLEAN": "boolean", "STRUCT(a INT)": "complex", "MAP(VARCHAR, INT)": "complex",
                 "INTEGER[]": "complex", "BIGINT": "numeric", "DECIMAL(10,2)": "numeric",
                 "DOUBLE": "numeric", "DATE": "temporal", "TIMESTAMP": "temporal", "TIME": "temporal",
                 "INTERVAL": "temporal", "VARCHAR": "text", "UUID": "text", "BLOB": "text",
                 "GEOMETRY": "other"}
        for sql_type, category in cases.items():
            self.assertEqual(categorise(sql_type), category, sql_type)

    def test_to_jsonable(self):
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        self.assertIsNone(to_jsonable(None))
        self.assertEqual(to_jsonable("s"), "s")
        self.assertEqual(to_jsonable(3), 3)
        self.assertEqual(to_jsonable(1.5), 1.5)
        self.assertEqual(to_jsonable(float("nan")), "nan")
        self.assertEqual(to_jsonable(float("inf")), "inf")
        self.assertEqual(to_jsonable(decimal.Decimal("2.50")), 2.5)
        self.assertEqual(to_jsonable(_dt.datetime(2024, 1, 2, 3, 4)), "2024-01-02T03:04:00")
        self.assertEqual(to_jsonable(_dt.date(2024, 1, 2)), "2024-01-02")
        self.assertEqual(to_jsonable(_dt.time(3, 4)), "03:04:00")
        self.assertEqual(to_jsonable(_dt.timedelta(hours=1)), "1:00:00")
        self.assertEqual(to_jsonable(uid), str(uid))
        self.assertEqual(to_jsonable(b"\x01\xff"), "0x01ff")
        self.assertEqual(to_jsonable([1, decimal.Decimal("1.5")]), [1, 1.5])
        self.assertEqual(to_jsonable({1: _dt.date(2024, 1, 1)}), {"1": "2024-01-01"})
        self.assertEqual(to_jsonable(object.__new__(type("X", (), {"__str__": lambda s: "x"}))), "x")
        json.dumps([to_jsonable(v) for v in (float("nan"), decimal.Decimal(1), uid)])

    def test_column_and_dataset(self):
        col = Column("a", "VARCHAR", "text")
        self.assertEqual(col.as_dict(), {"name": "a", "type": "VARCHAR", "category": "text"})
        col.date_formats = ["iso"]
        self.assertEqual(col.as_dict()["date_formats"], ["iso"])
        ds = Dataset(id="x", path="/a.parquet", display_name="a", columns=[col], row_count=1,
                     file_size=1, file_count=1)
        self.assertEqual(ds.column_types, {"a": "VARCHAR"})
        self.assertEqual(ds.sources, ["/a.parquet"])
        self.assertEqual(ds.path_label, "/a.parquet")
        ds.path = ["/a.parquet", "/b.parquet"]
        self.assertEqual(ds.sources, ["/a.parquet", "/b.parquet"])
        self.assertEqual(ds.path_label, "/a.parquet + /b.parquet")
        out = ds.as_dict()
        self.assertEqual(out["path"], "/a.parquet + /b.parquet")
        self.assertEqual(out["sources"], ["/a.parquet", "/b.parquet"])
        self.assertIsNone(out["source_column"])


class TestEngineOpen(Base):

    def test_open_reads_schema_and_count(self):
        self.assertEqual(self.ds.row_count, ROWS)
        self.assertEqual(self.ds.display_name, "fixture.parquet")
        self.assertEqual(self.ds.file_count, 1)
        self.assertEqual(self.ds.file_size, os.path.getsize(FIXTURE))
        described = sql("DESCRIBE SELECT * FROM T")
        self.assertEqual([(c.name, c.sql_type) for c in self.ds.columns], [(r[0], r[1]) for r in described])
        cats = {c.name: c.category for c in self.ds.columns}
        self.assertEqual((cats["tags"], cats["info"], cats["flag"], cats["clock"]),
                         ("complex", "complex", "boolean", "temporal"))
        self.assertIs(self.engine.get(self.ds.id), self.ds)

    def test_open_detects_dates_in_text_and_integers(self):
        found = {c.name: c.date_formats for c in self.ds.columns}
        self.assertEqual(found["dmy_text"], ["%d/%m/%Y"])
        self.assertEqual(found["ymd_int"], ["%Y%m%d"])
        for name in ("region", "label", "weird", "id", "qty", "day_date"):
            self.assertEqual(found[name], [], name)

    def test_resolve(self):
        self.assertEqual(Engine._resolve(FIXTURE), (FIXTURE, 1, os.path.getsize(FIXTURE)))
        folder = os.path.join(TMP, "parts")
        os.makedirs(os.path.join(folder, "sub"), exist_ok=True)
        con.execute("COPY (SELECT 1 AS a) TO '{}'".format(os.path.join(folder, "p1.parquet")))
        con.execute("COPY (SELECT 2 AS a) TO '{}'".format(os.path.join(folder, "sub", "p2.parquet")))
        source, count, size = Engine._resolve(folder + "/")
        self.assertEqual((source, count), (os.path.join(folder, "**", "*.parquet"), 2))
        self.assertGreater(size, 0)
        self.assertEqual(self.engine.open(folder).row_count, 2)
        empty = os.path.join(TMP, "empty-dir")
        os.makedirs(empty, exist_ok=True)
        with self.assertRaises(FileNotFoundError):
            Engine._resolve(empty)
        with self.assertRaises(FileNotFoundError):
            Engine._resolve(os.path.join(TMP, "missing.parquet"))

    def test_open_refuses_non_parquet(self):
        junk = os.path.join(TMP, "junk.parquet")
        with open(junk, "w") as handle:
            handle.write("not parquet")
        with self.assertRaises(ValueError):
            self.engine.open(junk)

    def test_get_and_close(self):
        temp = os.path.join(TMP, "temp-copy.parquet")
        shutil.copy(FIXTURE, temp)
        ds = self.engine.open(temp, is_temp=True, display_name="shown")
        self.assertEqual(ds.display_name, "shown")
        self.engine.close(ds.id)
        self.assertFalse(os.path.exists(temp), "a temp upload is deleted on close")
        with self.assertRaises(KeyError):
            self.engine.get(ds.id)
        self.engine.close("never-opened")              # no error
        self.engine.close(self.ds.id)
        self.assertTrue(os.path.exists(FIXTURE), "a file opened in place is never deleted")


class TestEngineUnion(Base):

    def setUp(self):
        super().setUp()
        self.a = os.path.join(TMP, "u_a.parquet")
        self.b = os.path.join(TMP, "u_b.parquet")
        con.execute("COPY (SELECT * FROM read_parquet('{}') WHERE id < 50) TO '{}'".format(FIXTURE, self.a))
        con.execute("COPY (SELECT * EXCLUDE (qty), qty FROM read_parquet('{}') WHERE id >= 50) TO '{}'"
                    .format(FIXTURE, self.b))

    def test_union_stacks_and_tags(self):
        ds = self.engine.open_union([self.a, " " + self.b + " ", ""])
        self.assertEqual(ds.row_count, ROWS)
        self.assertEqual(ds.sources, [self.a, self.b])
        self.assertEqual(ds.display_name, "u_a.parquet + u_b.parquet")
        self.assertEqual(ds.file_count, 2)
        self.assertEqual(ds.columns[-1].name, "source_file")
        self.assertTrue(ds.source_basename)
        values = self.engine.distinct_values(ds, "source_file", [])["values"]
        self.assertEqual(sorted((v["value"], v["count"]) for v in values),
                         [("u_a.parquet", 50), ("u_b.parquet", 150)])
        self.assertEqual({c.name: c.date_formats for c in ds.columns}["dmy_text"], ["%d/%m/%Y"])

    def test_union_without_tag_and_many_names(self):
        ds = self.engine.open_union([self.a, self.b], source_column=False)
        self.assertIsNone(ds.source_column)
        self.assertNotIn("source_file", ds.column_types)
        c = os.path.join(TMP, "u_c.parquet")
        d = os.path.join(TMP, "u_d.parquet")
        shutil.copy(self.a, c)
        shutil.copy(self.a, d)
        self.assertEqual(self.engine.open_union([self.a, self.b, c, d]).display_name,
                         "u_a.parquet + 3 more")

    def test_tag_name_avoids_a_clash(self):
        x = os.path.join(TMP, "clash_x.parquet")
        y = os.path.join(TMP, "clash_y.parquet")
        con.execute("COPY (SELECT 1 AS source_file) TO '{}'".format(x))
        con.execute("COPY (SELECT 2 AS source_file) TO '{}'".format(y))
        ds = self.engine.open_union([x, y])
        self.assertEqual(ds.source_column, "source_file_2")

    def test_same_names_fall_back_to_full_paths(self):
        other = os.path.join(TMP, "elsewhere")
        os.makedirs(other, exist_ok=True)
        twin = os.path.join(other, "u_a.parquet")
        shutil.copy(self.a, twin)
        ds = self.engine.open_union([self.a, twin])
        self.assertFalse(ds.source_basename)
        values = {v["value"] for v in self.engine.distinct_values(ds, "source_file", [])["values"]}
        self.assertEqual(values, {self.a, twin})

    def test_refusals(self):
        retyped = os.path.join(TMP, "u_bad.parquet")
        con.execute("COPY (SELECT * REPLACE (CAST(amount AS VARCHAR) AS amount) FROM read_parquet('{}')) TO '{}'"
                    .format(self.a, retyped))
        extra = os.path.join(TMP, "u_extra.parquet")
        con.execute("COPY (SELECT *, 1 AS more FROM read_parquet('{}')) TO '{}'".format(self.a, extra))
        with self.assertRaisesRegex(ValueError, r"amount \(DOUBLE vs VARCHAR\)"):
            self.engine.open_union([self.a, retyped])
        with self.assertRaisesRegex(ValueError, "extra more"):
            self.engine.open_union([self.a, extra])
        with self.assertRaisesRegex(ValueError, "missing more"):
            self.engine.open_union([extra, self.a])
        with self.assertRaisesRegex(ValueError, "same as file 1"):
            self.engine.open_union([self.a, self.a])
        with self.assertRaisesRegex(ValueError, "at least two"):
            self.engine.open_union([self.a, "  "])
        with self.assertRaises(FileNotFoundError):
            self.engine.open_union([self.a, self.a + ".missing"])


class TestDetectDates(unittest.TestCase):

    def detect(self, select):
        path = os.path.join(TMP, "detect-{}.parquet".format(uuid.uuid4().hex[:6]))
        con.execute("COPY (SELECT {} FROM range(300) t(i)) TO '{}'".format(select, path))
        cols = [Column(n, t, categorise(t)) for n, t, *_ in con.sql(
            "DESCRIBE SELECT * FROM read_parquet('{}')".format(path)).fetchall()]
        Engine._detect_dates(duckdb.connect(), path, cols)
        return {c.name: c.date_formats for c in cols}

    def test_layouts(self):
        base = "DATE '2024-01-01' + INTERVAL (i) DAY"
        found = self.detect(
            "strftime({b}, '%Y-%m-%d') AS iso, strftime({b}, '%m/%d/%Y') AS mdy, "
            "strftime({b}, '%d.%m.%Y') AS dotted, strftime({b}, '%b %d, %Y') AS words, "
            "strftime({b}, '%d/%m/%Y %H:%M:%S') AS dmy_time, "
            "CAST(strftime({b}, '%Y%m%d') AS BIGINT) AS ymd, "
            "strftime(DATE '2024-01-01' + INTERVAL (i % 12) DAY, '%d/%m/%Y') AS ambiguous".format(b=base))
        self.assertEqual(found["iso"], ["iso"])
        self.assertEqual(found["mdy"], ["%m/%d/%Y"])
        self.assertEqual(found["dotted"], ["%d.%m.%Y"])
        self.assertEqual(found["words"], ["%b %d, %Y"])
        self.assertEqual(found["dmy_time"], ["%d/%m/%Y %H:%M:%S"])
        self.assertEqual(found["ymd"], ["%Y%m%d"])
        self.assertEqual(found["ambiguous"], ["%d/%m/%Y", "%m/%d/%Y"])

    def test_blanks_and_non_dates(self):
        found = self.detect(
            "CASE WHEN i % 5 = 0 THEN 'N/A' WHEN i % 7 = 0 THEN '' WHEN i % 11 = 0 THEN NULL "
            "ELSE strftime(DATE '2024-02-01' + INTERVAL (i) DAY, '%d/%m/%Y') END AS with_blanks, "
            "CASE WHEN i = 150 THEN 'later' ELSE '2024-01-01' END AS one_bad, "
            "CAST(NULL AS VARCHAR) AS all_null, 'N/A' AS only_placeholders, "
            "i AS plain_int, (20240101 + i * 1000000)::BIGINT AS bad_ymd, 1.5 AS dbl")
        self.assertEqual(found["with_blanks"], ["%d/%m/%Y"])
        for name in ("one_bad", "all_null", "only_placeholders", "plain_int", "bad_ymd", "dbl"):
            self.assertEqual(found[name], [], name)

    def test_only_date_columns_are_flagged(self):
        found = self.detect(
            "'CODE-2024-' || lpad((i % 12 + 1)::VARCHAR, 2, '0') AS code, "
            "(i % 3)::VARCHAR || '.' || (i % 7)::VARCHAR || '.' || (i % 5)::VARCHAR AS version, "
            "md5(i::VARCHAR) AS hash, lpad((i % 99999)::VARCHAR, 5, '0') AS zip, "
            "'12/34/' || (2000 + i % 20)::VARCHAR AS impossible_day, "
            "(i % 12 + 1)::VARCHAR || '/' || (i % 28 + 1)::VARCHAR AS no_year, "
            "strftime(TIMESTAMP '2024-01-01' + INTERVAL (i) HOUR, '%Y-%m-%d %H:%M:%S') AS real_ts")
        self.assertEqual({k: v for k, v in found.items() if v}, {"real_ts": ["iso"]})

    def test_wide_files_stay_fast(self):
        # 60 text columns, none of them dates: each should cost one regex test.
        cols = ", ".join("md5((i * {k})::VARCHAR) AS t{k}".format(k=k) for k in range(60))
        path = os.path.join(TMP, "wide-text.parquet")
        con.execute("COPY (SELECT {} FROM range(5000) t(i)) TO '{}'".format(cols, path))
        engine = Engine(threads=2)
        started = time.time()
        dataset = engine.open(path)
        self.assertLess(time.time() - started, 1.5)
        self.assertFalse([c.name for c in dataset.columns if c.date_formats])

    def test_progress_is_reported(self):
        steps = []
        Engine(threads=2).open(FIXTURE, progress=steps.append)
        self.assertEqual(steps, ["Finding the parquet file…", "Reading the schema…",
                                 "Checking 8 columns for dates stored as text or numbers…", "Counting rows…"])
        union = []
        a = os.path.join(TMP, "p_a.parquet")
        shutil.copy(FIXTURE, a)
        Engine(threads=2).open_union([FIXTURE, a], progress=union.append)
        self.assertEqual(union[:5], ["Finding 2 parquet files…", "Reading the schema of file 1 of 2…",
                                     "Reading the schema of file 2 of 2…", "Comparing the files' columns…",
                                     "Checking 8 columns for dates stored as text or numbers…"])
        self.assertEqual(union[-1], "Counting rows…")

    def test_nothing_to_check_and_unreadable(self):
        cols = [Column("x", "DOUBLE", "numeric")]
        Engine._detect_dates(duckdb.connect(), "/does/not/matter", cols)
        self.assertEqual(cols[0].date_formats, [])
        cols = [Column("x", "VARCHAR", "text")]
        Engine._detect_dates(duckdb.connect(), "/does/not/exist.parquet", cols)   # swallowed
        self.assertEqual(cols[0].date_formats, [])


class TestEngineQueries(Base):

    def test_from(self):
        self.assertEqual(Engine._from(), "read_parquet(?, union_by_name=true)")
        self.assertEqual(Engine._from(self.ds), "read_parquet(?, union_by_name=true)")
        tagged = Dataset(id="t", path=["a"], display_name="t", columns=[], row_count=0, file_size=0,
                         file_count=1, source_column="source_file")
        self.assertIn("parse_filename(__pqs_src)", Engine._from(tagged))
        tagged.source_basename = False
        self.assertNotIn("parse_filename", Engine._from(tagged))

    def test_transform_of(self):
        self.assertEqual(Engine._transform_of("year"), ("year", None))
        self.assertEqual(Engine._transform_of(None), (None, None))
        self.assertEqual(Engine._transform_of(""), (None, None))
        self.assertEqual(Engine._transform_of({"part": "day", "format": "iso"}), ("day", "iso"))
        self.assertEqual(Engine._transform_of({}), (None, None))

    def test_select_list(self):
        text, chosen = self.engine._select_list(self.ds, None)
        self.assertEqual(len(chosen), len(self.ds.columns))
        text, chosen = self.engine._select_list(self.ds, ["qty", "nope", "id"])
        self.assertEqual(text, '"qty", "id"')
        text, chosen = self.engine._select_list(self.ds, ["nope"])
        self.assertEqual(len(chosen), len(self.ds.columns), "nothing valid means every column")
        text, chosen = self.engine._select_list(self.ds, ["day_date", "dmy_text"], {
            "day_date": "year", "dmy_text": {"part": "month", "format": "%d/%m/%Y"}})
        self.assertIn('year("day_date") AS "day_date"', text)
        self.assertEqual([(c.name, c.sql_type, c.category) for c in chosen],
                         [("day_date", "BIGINT", "numeric"), ("dmy_text", "BIGINT", "numeric")])
        with self.assertRaises(F.FilterError):
            self.engine._select_list(self.ds, None, {"nope": "year"})
        with self.assertRaises(F.FilterError):
            self.engine._select_list(self.ds, None, {"amount": "year"})

    def test_query_sql_sorts_nulls_last(self):
        query, params, _ = self.engine.query_sql(self.ds, [], ["id", "amount"], "amount", True)
        self.assertIn("ORDER BY", query)
        rows = self.engine.cursor().execute(query, params).fetchall()
        self.assertEqual(rows[0][1], scalar("SELECT max(amount) FROM T"))
        self.assertIsNone(rows[-1][1])
        query, _, _ = self.engine.query_sql(self.ds, [], None, "not_a_column")
        self.assertNotIn("ORDER BY", query)
        query, _, _ = self.engine.query_sql(self.ds, [], ["ts"], "ts", False, {"ts": "day"})
        self.assertIn('ORDER BY day("ts")', query)

    def test_preview(self):
        page = self.engine.preview(self.ds, [{"column": "region", "op": "in", "values": ["east"]}],
                                   ["id", "region", "day_date"], limit=3, offset=2, order_by="id")
        expected = sql("SELECT id, region, day_date FROM T WHERE region = 'east' ORDER BY id LIMIT 3 OFFSET 2")
        self.assertEqual(page["rows"], [[r[0], r[1], r[2].isoformat()] for r in expected])
        self.assertEqual([c["name"] for c in page["columns"]], ["id", "region", "day_date"])
        extracted = self.engine.preview(self.ds, [], ["id", "ts", "ymd_int"], limit=2, order_by="id",
                                        transforms={"ts": "month", "ymd_int": {"part": "year", "format": "%Y%m%d"}})
        self.assertEqual(extracted["rows"], [list(r) for r in sql(
            "SELECT id, month(ts), year(day_date) FROM T ORDER BY id LIMIT 2")])
        json.dumps(self.engine.preview(self.ds, [], None, limit=ROWS))   # every type serialises

    def test_count(self):
        self.assertEqual(self.count(), ROWS)
        self.assertEqual(self.count({"column": "flag", "op": "eq", "value": False}), ROWS // 2)

    def test_distinct_values(self):
        out = self.engine.distinct_values(self.ds, "region", [])
        self.assertEqual(sorted(((v["value"] or ""), v["count"]) for v in out["values"]),
                         sorted(((r[0] or ""), r[1]) for r in sql("SELECT region, count(*) FROM T GROUP BY 1")))
        self.assertFalse(out["truncated"])
        top = self.engine.distinct_values(self.ds, "id", [], limit=5)
        self.assertEqual((len(top["values"]), top["truncated"]), (5, True))
        searched = self.engine.distinct_values(self.ds, "label", [], search="ITEM 19")
        self.assertEqual(sorted(v["value"] for v in searched["values"]),
                         sorted(r[0] for r in sql("SELECT DISTINCT label FROM T WHERE label ILIKE '%item 19%'")))
        self.assertEqual(self.engine.distinct_values(self.ds, "weird", [], search="%_1")["values"][0]["value"],
                         "x%_1")
        exact = self.engine.distinct_values(self.ds, "region", [], exact=["NORTH", "West", "mars"])
        self.assertEqual(sorted(v["value"] for v in exact["values"]), ["north", "west"])

    def test_distinct_values_ignore_own_filter_but_not_others(self):
        own = [{"column": "region", "op": "in", "values": ["north"]}]
        self.assertEqual(len(self.engine.distinct_values(self.ds, "region", own)["values"]), 5)
        other = [{"column": "flag", "op": "eq", "value": True}]
        got = {v["value"]: v["count"] for v in self.engine.distinct_values(self.ds, "region", other)["values"]}
        self.assertEqual(got, dict(sql("SELECT region, count(*) FROM T WHERE flag GROUP BY 1")))

    def test_distinct_values_transformed_in_calendar_order(self):
        years = self.engine.distinct_values(self.ds, "day_date", [], transform="year")["values"]
        self.assertEqual([(v["value"], v["count"]) for v in years],
                         [tuple(r) for r in sql("SELECT year(day_date), count(*) FROM T GROUP BY 1 ORDER BY 1")])
        months = self.engine.distinct_values(self.ds, "dmy_text", [], transform="month", date_format="%d/%m/%Y")
        self.assertEqual([v["value"] for v in months["values"]], list(range(1, 13)))

    def test_distinct_values_complex_and_unknown(self):
        tags = self.engine.distinct_values(self.ds, "tags", [], limit=3)["values"]
        self.assertTrue(all(isinstance(v["value"], str) for v in tags))
        with self.assertRaises(F.FilterError):
            self.engine.distinct_values(self.ds, "nope", [])

    def test_column_stats(self):
        stats = self.engine.column_stats(self.ds, "amount", [])
        lo, hi, mean, nulls = sql("SELECT min(amount), max(amount), avg(amount), count(*) - count(amount) FROM T")[0]
        self.assertEqual((stats["lo"], stats["hi"], stats["nulls"], stats["category"]), (lo, hi, nulls, "numeric"))
        self.assertAlmostEqual(stats["mean"], mean)
        dates = self.engine.column_stats(self.ds, "day_date", [])
        self.assertEqual((dates["lo"], dates["category"]), ("2023-01-01", "temporal"))
        self.assertNotIn("mean", dates)
        text = self.engine.column_stats(self.ds, "region", [])
        self.assertEqual((text["nulls"], "lo" in text), (scalar("SELECT count(*) FROM T WHERE region IS NULL"), False))
        self.assertIn("distinct_approx", self.engine.column_stats(self.ds, "info", []))
        months = self.engine.column_stats(self.ds, "ts", [], transform="month")
        self.assertEqual((months["lo"], months["hi"], months["category"]),
                         sql("SELECT min(month(ts)), max(month(ts)) FROM T")[0] + ("numeric",))
        filtered = self.engine.column_stats(self.ds, "amount", [{"column": "amount", "op": "gt", "value": 1e9}])
        self.assertEqual(filtered["total"], ROWS, "a column's own filter is skipped for its stats")
        with self.assertRaises(F.FilterError):
            self.engine.column_stats(self.ds, "nope", [])

    def test_iter_rows(self):
        batches = list(self.engine.iter_rows(self.ds, [], ["id"], "id", chunk_size=64))
        self.assertEqual([len(rows) for _, rows in batches], [64, 64, 64, 8])
        self.assertEqual([r[0] for _, rows in batches for r in rows], list(range(ROWS)))
        limited = list(self.engine.iter_rows(self.ds, [], ["id"], "id", True, row_limit=5))
        self.assertEqual([r[0] for _, rows in limited for r in rows], [199, 198, 197, 196, 195])
        cols, _ = next(self.engine.iter_rows(self.ds, [], ["ts"], transforms={"ts": "year"}))
        self.assertEqual(cols[0].sql_type, "BIGINT")

    def test_copy_to_every_format(self):
        spec = [{"column": "region", "op": "in", "values": ["south"]}]
        want = scalar("SELECT count(*) FROM T WHERE region = 'south'")
        for fmt in ("csv", "parquet", "json"):
            path = os.path.join(TMP, "copy.{}".format(fmt))
            self.engine.copy_to(self.ds, path, fmt, spec, ["id", "region"], "id")
            reader = {"csv": "read_csv('{}')", "parquet": "read_parquet('{}')", "json": "read_json('{}')"}[fmt]
            rows = con.sql("SELECT * FROM " + reader.format(path)).fetchall()
            self.assertEqual(len(rows), want, fmt)
            self.assertEqual({r[1] for r in rows}, {"south"}, fmt)
        path = os.path.join(TMP, "limited.parquet")
        self.engine.copy_to(self.ds, path, "parquet", [], ["id", "day_date"], "id", True, 4, {"day_date": "year"})
        self.assertEqual(con.sql("SELECT * FROM read_parquet('{}')".format(path)).fetchall(),
                         [tuple(r) for r in sql("SELECT id, year(day_date) FROM T ORDER BY id DESC LIMIT 4")])
        with self.assertRaises(KeyError):
            self.engine.copy_to(self.ds, path, "xml", [], None)


# ===================================================================== pivot.py

class TestPivotHelpers(Base):

    def test_default_label_and_value_field(self):
        self.assertEqual(P.default_label("amount", "sum"), "Sum of amount")
        self.assertEqual(P.default_label("", "count_rows"), "Count of rows")
        field = P._measure({"column": "amount", "agg": "avg", "label": "Mean"}, self.ds.column_types)
        self.assertEqual(field.as_dict(), {"column": "amount", "agg": "avg", "label": "Mean", "is_count": False})

    def test_measure_every_aggregation(self):
        types = self.ds.column_types
        for agg in P.AGGREGATIONS:
            spec = {"agg": agg} if agg == "count_rows" else {"column": "amount", "agg": agg}
            field = P._measure(spec, types)
            self.assertEqual(field.is_count, agg in P.INTEGER_AGGS, agg)
        self.assertEqual(P._measure({"column": "tags", "agg": "count"}, types).sql, 'count(CAST("tags" AS VARCHAR))')
        self.assertEqual(P._measure({"column": "region", "agg": "max"}, types).sql, 'max("region")')
        self.assertEqual(P._measure({"column": "amount"}, types).agg, "sum")

    def test_measure_refusals(self):
        types = self.ds.column_types
        for spec in ({"column": "amount", "agg": "mode"}, {"column": "nope", "agg": "sum"},
                     {"column": "region", "agg": "sum"}, {"column": "tags", "agg": "min"},
                     {"column": "flag", "agg": "avg"}):
            with self.assertRaises(P.PivotError, msg=spec):
                P._measure(spec, types)

    def test_sort_key_orders_mixed_values(self):
        values = [None, "b", _dt.date(2024, 1, 1), 2, True, decimal.Decimal("1.5"), "a"]
        ordered = sorted(values, key=P._sort_key)
        # Booleans rank as 0/1 among the numbers; then dates, then text, blanks last.
        self.assertEqual(ordered, [True, decimal.Decimal("1.5"), 2, _dt.date(2024, 1, 1), "a", "b", None])
        self.assertEqual(P._sort_key(decimal.Decimal("NaN"))[0], 0)
        self.assertLess(P._key_sort(("a", 1)), P._key_sort(("a", 2)))
        self.assertLess(P._key_sort(("a", 9)), P._key_sort((None, 1)))

    def test_label_of(self):
        self.assertEqual(P.label_of(None), "(blank)")
        self.assertEqual(P.label_of(""), "(empty)")
        self.assertEqual(P.label_of(False), "false")
        self.assertEqual(P.label_of(_dt.datetime(2024, 1, 2)), "2024-01-02")
        self.assertEqual(P.label_of(_dt.datetime(2024, 1, 2, 3, 4)), "2024-01-02 03:04:00")
        self.assertEqual(P.label_of(_dt.date(2024, 1, 2)), "2024-01-02")
        self.assertEqual(P.label_of(_dt.time(1, 2)), "01:02:00")
        self.assertEqual(P.label_of(decimal.Decimal("2.50")), "2.5")
        self.assertEqual(P.label_of(7), "7")

    def test_grouping_sets(self):
        self.assertEqual(P._grouping_sets(["a", "b"], [], True, True), [["a", "b"], ["a"], []])
        self.assertEqual(P._grouping_sets(["a", "b"], [], False, True), [["a", "b"], []])
        self.assertEqual(P._grouping_sets(["a"], ["c"], True, True), [["a", "c"], ["a"], ["c"], []])
        self.assertEqual(P._grouping_sets(["a"], ["c"], True, False), [["a", "c"], ["c"]])
        self.assertEqual(P._grouping_sets([], ["c"], True, True), [["c"], []])

    def test_key_window_sql(self):
        text = P._key_window_sql("SRC", ["a", "b"], "WHERE x", 10)
        self.assertEqual(text, 'WITH __keys AS (SELECT "a", "b" FROM SRC WHERE x GROUP BY "a", "b" '
                               'ORDER BY "a" NULLS LAST, "b" NULLS LAST LIMIT 10)')

    def test_whole_outer_groups(self):
        keys = {("a", 1), ("a", 2), ("b", 1)}
        self.assertEqual(P._whole_outer_groups(keys), {("a", 1), ("a", 2)})
        self.assertEqual(P._whole_outer_groups({("a", 1), ("a", 2)}), {("a", 1), ("a", 2)})

    def test_size_helpers(self):
        flt = [{"column": "flag", "op": "eq", "value": True}]
        self.assertEqual(P.count_row_groups(self.engine, self.ds, [], flt), 1)
        self.assertEqual(P.count_row_groups(self.engine, self.ds, ["region", "qty"], flt),
                         scalar("SELECT count(*) FROM (SELECT DISTINCT region, qty FROM T WHERE flag)"))
        sizes = P.outer_group_sizes(self.engine, self.ds, ["region", "qty"])
        self.assertEqual(sizes, [tuple(r) for r in sql(
            "SELECT region, count(*) FROM (SELECT DISTINCT region, qty FROM T) GROUP BY 1 ORDER BY 1 NULLS LAST")])
        self.assertEqual(P.outer_group_sizes(self.engine, self.ds, []), [])
        totals = P.overall_totals(self.engine, self.ds, [{"column": "amount", "agg": "sum"}, {"agg": "count_rows"}], flt)
        self.assertEqual(totals, [("Sum of amount", scalar("SELECT sum(amount) FROM T WHERE flag")),
                                  ("Count of rows", ROWS // 2)])


class TestPivotCompute(Base):

    def pivot(self, **kw):
        kw.setdefault("values", [{"column": "amount", "agg": "sum"}])
        return P.compute(self.engine, self.ds, **kw)

    def test_rows_only_with_subtotals_and_grand_total(self):
        out = self.pivot(rows=["region", "flag"], columns=[])
        kinds = [r["kind"] for r in out["rows"]]
        self.assertEqual(kinds.count("grand"), 1)
        self.assertEqual(kinds.count("subtotal"), scalar("SELECT count(DISTINCT coalesce(region, '~')) FROM T"))
        detail = {tuple(r["labels"]): r["cells"][0] for r in out["rows"] if r["kind"] == "data"}
        for region, flag, total in sql("SELECT region, flag, sum(amount) FROM T GROUP BY 1, 2"):
            key = (P.label_of(region), P.label_of(flag))
            self.assertAlmostEqual(detail[key] or 0, total or 0, msg=key)
        north = next(r for r in out["rows"] if r["kind"] == "subtotal" and r["labels"][0] == "north Total")
        self.assertAlmostEqual(north["cells"][0], scalar("SELECT sum(amount) FROM T WHERE region = 'north'"))
        self.assertAlmostEqual(out["rows"][-1]["cells"][0], scalar("SELECT sum(amount) FROM T"))
        self.assertEqual(out["rows"][-1]["labels"], ["Grand Total", ""])
        self.assertEqual((out["row_groups"], out["truncated"]),
                         (scalar("SELECT count(*) FROM (SELECT DISTINCT region, flag FROM T)"), False))

    def test_cross_tab_totals_are_real_aggregates(self):
        out = self.pivot(rows=["region"], columns=["flag"], values=[{"column": "amount", "agg": "avg"}])
        self.assertEqual([leaf["label"] for leaf in out["leaves"]], ["false", "true", "Total"])
        grand = out["rows"][-1]["cells"]
        self.assertAlmostEqual(grand[2], scalar("SELECT avg(amount) FROM T"), msg="not an average of averages")
        east = next(r for r in out["rows"] if r["labels"] == ["east"])["cells"]
        self.assertAlmostEqual(east[1], scalar("SELECT avg(amount) FROM T WHERE region = 'east' AND flag"))
        self.assertEqual(out["column_groups"], 2)

    def test_multiple_measures_and_header(self):
        out = self.pivot(rows=["region"], columns=["flag"],
                         values=[{"column": "amount", "agg": "sum"}, {"agg": "count_rows"}])
        self.assertEqual(len(out["header"]), 2)
        self.assertEqual(len(out["leaves"]), 6)
        counts = [leaf["is_count"] for leaf in out["leaves"]]
        self.assertEqual(counts, [False, True] * 3)
        self.assertEqual(out["rows"][-1]["cells"][5], ROWS)

    def test_columns_only(self):
        out = self.pivot(rows=[], columns=["flag"], values=[{"agg": "count_rows"}])
        self.assertEqual(len(out["rows"]), 1)
        self.assertEqual(out["rows"][0]["cells"], [ROWS // 2, ROWS // 2, ROWS])

    def test_toggles(self):
        out = self.pivot(rows=["region", "flag"], columns=["qty"], subtotals=False, row_totals=False,
                         column_totals=False)
        self.assertNotIn("subtotal", [r["kind"] for r in out["rows"]])
        self.assertNotIn("grand", [r["kind"] for r in out["rows"]])
        self.assertNotIn("total", [leaf["kind"] for leaf in out["leaves"]])

    def test_sort_by_value(self):
        out = self.pivot(rows=["region"], columns=[], values=[{"agg": "count_rows"}],
                         sort={"by": "value", "value_index": 0, "descending": True})
        counts = [r["cells"][0] for r in out["rows"] if r["kind"] == "data"]
        self.assertEqual(counts, sorted(counts, reverse=True))
        out = self.pivot(rows=["region"], columns=[], values=[{"agg": "count_rows"}],
                         sort={"by": "value", "value_index": 9})
        self.assertEqual(len(out["rows"]), 6, "an out-of-range measure index falls back to the first")

    def test_truncation(self):
        out = self.pivot(rows=["id"], columns=[], max_rows=7)
        self.assertEqual((out["row_groups"], out["total_row_groups"], out["truncated"]), (7, ROWS, True))
        self.assertAlmostEqual(out["rows"][-1]["cells"][0], scalar("SELECT sum(amount) FROM T"))

    def test_windowed_path_keeps_totals_whole(self):
        with mock.patch.object(P, "HARD_MAX_ROW_KEYS", 5):
            out = self.pivot(rows=["region", "qty"], columns=[], max_rows=12)
        self.assertTrue(out["truncated"])
        subtotal_rows = [r for r in out["rows"] if r["kind"] == "subtotal"]
        for row in subtotal_rows:
            region = row["labels"][0][:-len(" Total")]
            self.assertAlmostEqual(row["cells"][0], scalar(
                "SELECT sum(amount) FROM T WHERE region = '{}'".format(region)), msg=region)
        self.assertAlmostEqual(out["rows"][-1]["cells"][0], scalar("SELECT sum(amount) FROM T"))

    def test_too_many_row_keys_falls_back(self):
        real = P._run_query
        calls = []

        def flaky(*args, **kwargs):
            calls.append(kwargs.get("key_limit"))
            if len(calls) == 1:
                raise P._TooManyRowKeys()
            return real(*args, **kwargs)

        with mock.patch.object(P, "_run_query", flaky):
            out = self.pivot(rows=["region"], columns=[], max_rows=3)
        self.assertEqual(calls, [None, 3])
        self.assertTrue(out["truncated"])

    def test_too_many_columns(self):
        with self.assertRaises(P.PivotError):
            self.pivot(rows=["region"], columns=["id"], max_columns=10)

    def test_filters_apply(self):
        flt = [{"column": "day_date", "transform": "year", "op": "in", "values": [2023]}]
        out = self.pivot(rows=["region"], columns=[], values=[{"agg": "count_rows"}], filters=flt)
        self.assertEqual(out["rows"][-1]["cells"][0], scalar("SELECT count(*) FROM T WHERE year(day_date) = 2023"))

    def test_jsonable_off_keeps_native_values(self):
        out = self.pivot(rows=["region"], columns=[], values=[{"column": "day_date", "agg": "max"}], jsonable=False)
        self.assertIsInstance(out["rows"][0]["cells"][0], _dt.date)
        out = self.pivot(rows=["region"], columns=[], values=[{"column": "day_date", "agg": "max"}])
        self.assertIsInstance(out["rows"][0]["cells"][0], str)

    def test_refusals(self):
        bad = [dict(rows=["nope"]), dict(rows=["tags"]), dict(rows=["region", "region"]),
               dict(rows=["region"], columns=["region"]), dict(rows=["region"], values=[]),
               dict(rows=[], columns=[])]
        for kw in bad:
            kw.setdefault("columns", [])
            with self.assertRaises(P.PivotError, msg=kw):
                self.pivot(**kw)


# ================================================================== exporter.py

class TestExporterHelpers(unittest.TestCase):

    def test_describe_transform(self):
        self.assertEqual(exporter_module._describe_transform("d", "year"), "d → year only")
        self.assertEqual(exporter_module._describe_transform("t", {"part": "month", "format": "%d/%m/%Y"}),
                         "t → month only (read as DD/MM/YYYY)")

    def test_export_job_as_dict(self):
        job = ExportJob(id="j", dataset_id="d", fmt="csv", filename="f.csv")
        self.assertIsNone(job.as_dict()["percent"])
        job.total, job.written = 200, 50
        self.assertEqual(job.as_dict()["percent"], 25.0)
        job.written = 900
        self.assertEqual(job.as_dict()["percent"], 100.0)
        job.total, job.status = None, "done"
        self.assertEqual(job.as_dict()["percent"], 100.0)
        self.assertEqual(set(job.as_dict()), {"id", "status", "kind", "format", "filename", "total", "written",
                                              "sheets", "percent", "size", "error", "message", "elapsed",
                                              "rows_per_sec"})

    def test_excel_value(self):
        ev = exporter_module._excel_value
        self.assertIsNone(ev(None))
        self.assertEqual(ev(True), True)
        self.assertEqual(ev(float("nan")), "nan")
        self.assertEqual(len(ev("x" * 40000)), exporter_module.EXCEL_MAX_CELL_CHARS)
        self.assertEqual(ev(decimal.Decimal("1.25")), 1.25)
        aware = _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc)
        self.assertIsNone(ev(aware).tzinfo)
        self.assertEqual(ev(_dt.datetime(1800, 1, 1)), "1800-01-01T00:00:00")
        self.assertEqual(ev(_dt.date(1899, 12, 31)), "1899-12-31")
        self.assertEqual(ev(_dt.date(2024, 1, 1)), _dt.date(2024, 1, 1))
        self.assertEqual(ev(_dt.time(1, 2)), _dt.time(1, 2))
        self.assertEqual(ev(b"\x00\x01"), "0x0001")
        self.assertEqual(ev([1, 2]), "[1, 2]")

    def test_manifest_number(self):
        self.assertEqual(exporter_module._manifest_number(None), "—")
        self.assertEqual(exporter_module._manifest_number(1234.5), "1,234.50")
        self.assertEqual(exporter_module._manifest_number(1234567), "1,234,567")
        self.assertEqual(exporter_module._manifest_number(True), "True")
        self.assertEqual(exporter_module._manifest_number("x"), "x")

    def test_pack_batches(self):
        pack = ExportManager._pack_batches
        self.assertEqual(pack([("a", 3), ("b", 3), ("c", 3)], 6), [(["a", "b"], 6), (["c"], 3)])
        self.assertEqual(pack([("a", 9), ("b", 1)], 5), [(["a"], 9), (["b"], 1)])
        self.assertEqual(pack([], 5), [])


class TestExportManager(Base):

    def setUp(self):
        super().setUp()
        self.out_dir = tempfile.mkdtemp(dir=TMP)
        self.exports = ExportManager(self.engine, self.out_dir)

    def run_export(self, fmt, **kw):
        job = wait(self.exports.start(self.ds.id, fmt, kw.pop("filters", []), **kw))
        self.assertEqual(job.status, "done", job.error)
        return job

    def test_native_formats(self):
        spec = [{"column": "region", "op": "in", "values": ["west"]}]
        want = scalar("SELECT count(*) FROM T WHERE region = 'west'")
        for fmt in ("csv", "parquet", "json"):
            job = self.run_export(fmt, filters=spec, columns=["id", "region"], total_hint=want)
            self.assertTrue(job.filename.startswith("fixture-filtered-") and job.filename.endswith("." + fmt))
            self.assertEqual((job.written, job.kind), (want, "rows"))
            self.assertEqual(job.size, os.path.getsize(job.path))
        job = self.run_export("parquet", columns=["id"], order_by="id", row_limit=3)
        self.assertEqual(job.total, 3)
        self.assertEqual(con.sql("SELECT id FROM read_parquet('{}')".format(job.path)).fetchall(), [(0,), (1,), (2,)])

    def test_xlsx_content_and_manifest(self):
        job = self.run_export("xlsx", columns=["id", "label", "day_date", "ts", "dmy_text"], order_by="id",
                              row_limit=20, sheet_name="Rows",
                              transforms={"dmy_text": {"part": "year", "format": "%d/%m/%Y"}},
                              filters=[{"column": "flag", "op": "eq", "value": True}])
        book = read_xlsx(job.path)
        self.assertEqual(list(book), ["Export info", "Rows"])
        rows = book["Rows"]
        self.assertEqual([rows[c + "1"] for c in "ABCDE"], ["id", "label", "day_date", "ts", "dmy_text"])
        self.assertEqual(column_values(rows, "A"), [r[0] for r in sql("SELECT id FROM T WHERE flag ORDER BY id LIMIT 20")])
        self.assertEqual(set(column_values(rows, "E")), {2023})
        self.assertTrue(rows["C2@style"], "dates carry a date format")
        self.assertFalse(rows["E2@style"], "an extracted year is a plain number")
        info = " ".join(str(v) for v in book["Export info"].values())
        self.assertIn("flag eq True", info)
        self.assertIn("dmy_text → year only (read as DD/MM/YYYY)", info)
        self.assertIn("20", info)

    def test_xlsx_without_manifest_and_empty_result(self):
        job = self.run_export("xlsx", include_manifest=False,
                              filters=[{"column": "id", "op": "lt", "value": 0}])
        book = read_xlsx(job.path)
        self.assertEqual(list(book), ["Data"])
        self.assertEqual(book["Data"]["A1"], "id")
        self.assertEqual(job.written, 0)

    def test_xlsx_splits_sheets(self):
        with mock.patch.object(exporter_module, "EXCEL_MAX_DATA_ROWS", 60):
            job = self.run_export("xlsx", columns=["id"], include_manifest=False)
        self.assertEqual(job.sheets, 4)
        book = read_xlsx(job.path)
        self.assertEqual(list(book), ["Data", "Data (2)", "Data (3)", "Data (4)"])
        self.assertEqual(sum(len(column_values(s, "A")) for s in book.values()), ROWS)

    def test_bad_requests_fail_fast(self):
        with self.assertRaises(F.FilterError):
            self.exports.start(self.ds.id, "csv", [], transforms={"amount": "year"})
        with self.assertRaises(KeyError):
            self.exports.start(self.ds.id, "xml", [])
        with self.assertRaises(KeyError):
            self.exports.start("no-such-dataset", "csv", [])

    def test_failure_is_reported_and_cleaned_up(self):
        with mock.patch.object(exporter_module.traceback, "print_exc"):   # expected failure, keep output quiet
            job = wait(self.exports.start(self.ds.id, "csv", [{"column": "amount", "op": "gt", "value": "abc"}]))
        self.assertEqual((job.status, job.message), ("error", "Export failed"))
        self.assertIn("Conversion", job.error)
        self.assertFalse(os.path.exists(job.path))

    def test_cancel(self):
        job = ExportJob(id="c", dataset_id=self.ds.id, fmt="xlsx", filename="c.xlsx",
                        path=os.path.join(self.out_dir, "c.xlsx"))
        self.exports.jobs[job.id] = job
        job.status = "running"
        self.assertEqual(self.exports.cancel("c").message, "Cancelling…")
        self.exports._run(job, self.ds, [], [], None, False, None, False, "Data")
        self.assertEqual(job.status, "cancelled")
        self.assertFalse(os.path.exists(job.path))
        with self.assertRaises(KeyError):
            self.exports.get("nope")

    def test_cleanup_and_discard(self):
        job = self.run_export("csv")
        self.exports.cleanup(max_age_seconds=3600)
        self.assertIn(job.id, self.exports.jobs)
        job.finished_at = time.time() - 7200
        self.exports.cleanup(max_age_seconds=3600)
        self.assertNotIn(job.id, self.exports.jobs)
        self.assertFalse(os.path.exists(job.path))
        ExportManager._discard(ExportJob(id="x", dataset_id="d", fmt="csv", filename="f", path=None))

    def test_pivot_export(self):
        spec = {"rows": ["region"], "columns": ["flag"], "values": [{"column": "amount", "agg": "sum"}],
                "filters": [], "subtotals": True, "row_totals": True, "column_totals": True,
                "repeat_labels": False, "sort": None}
        job = wait(self.exports.start_pivot(self.ds.id, spec, "My pivot"))
        self.assertEqual((job.status, job.kind, job.fmt, job.sheets), ("done", "pivot", "xlsx", 1), job.error)
        self.assertEqual(job.written, 6)
        book = read_xlsx(job.path)
        self.assertIn("My pivot", book)
        grand = [v for k, v in book["My pivot"].items() if v == "Grand Total"]
        self.assertEqual(len(grand), 1)
        numbers = [v for k, v in book["My pivot"].items() if isinstance(v, float) and not k.endswith("@style")]
        self.assertTrue(any(abs(v - scalar("SELECT sum(amount) FROM T")) < 1e-6 for v in numbers))

    def test_pivot_export_splits_into_a_zip(self):
        spec = {"rows": ["region", "qty"], "columns": [], "values": [{"agg": "count_rows"}], "filters": []}
        job = wait(self.exports.start_pivot(self.ds.id, spec, max_rows=12))
        self.assertEqual((job.status, job.fmt), ("done", "zip"), job.error)
        self.assertTrue(job.filename.endswith(".zip"))
        groups = scalar("SELECT count(*) FROM (SELECT DISTINCT region, qty FROM T)")
        self.assertEqual(job.written, groups)
        with zipfile.ZipFile(job.path) as bundle:
            names = bundle.namelist()
        self.assertEqual(len(names), job.sheets)
        self.assertGreater(job.sheets, 1)
        self.assertTrue(all(re.search(r"-\d{3}-of-\d{3}\.xlsx$", n) for n in names))

    def test_pivot_export_error(self):
        with mock.patch.object(exporter_module.traceback, "print_exc"):
            job = wait(self.exports.start_pivot(self.ds.id, {"rows": ["nope"], "values": [{"agg": "count_rows"}]}))
        self.assertEqual(job.status, "error")


# ====================================================================== main.py

class TestMain(unittest.TestCase):
    """The route functions and helpers, called directly (no HTTP)."""

    def setUp(self):
        self.recents = os.path.join(tempfile.mkdtemp(dir=TMP), "recents.json")
        patcher = mock.patch.object(M, "RECENTS_FILE", self.recents)
        patcher.start()
        self.addCleanup(patcher.stop)

    def assertHttp(self, code, fn, *args, **kwargs):
        with self.assertRaises(HTTPException) as caught:
            fn(*args, **kwargs)
        self.assertEqual(caught.exception.status_code, code)
        return caught.exception.detail

    def test_recents(self):
        self.assertEqual(M._load_recents(), [])
        with open(self.recents, "w") as handle:
            handle.write("{not json")
        self.assertEqual(M._load_recents(), [])
        M._remember(FIXTURE, "fixture.parquet", 5)
        M._remember("/gone.parquet", "gone", 1)
        M._remember(FIXTURE, "fixture.parquet", 6)
        self.assertEqual(M._load_recents(), [{"path": FIXTURE, "name": "fixture.parquet", "rows": 6}],
                         "deduplicated, newest first, missing files dropped")
        self.assertEqual(M.recents(), {"recents": M._load_recents()})
        for i in range(M.MAX_RECENTS + 5):
            path = os.path.join(os.path.dirname(self.recents), "p{}.parquet".format(i))
            open(path, "w").close()
            M._remember(path, "p", i)
        with open(self.recents) as handle:
            self.assertEqual(len(json.load(handle)), M.MAX_RECENTS)
        with mock.patch.object(M, "RECENTS_FILE", os.path.join(TMP, "no", "such", "dir.json")):
            M._remember(FIXTURE, "x", 1)       # unwritable: silently skipped

    def test_guard_maps_errors(self):
        def boom(exc):
            raise exc
        self.assertEqual(M._guard(lambda: 3), 3)
        self.assertEqual(self.assertHttp(404, M._guard, boom, KeyError("gone")), "gone")
        self.assertHttp(400, M._guard, boom, F.FilterError("bad"))
        self.assertHttp(400, M._guard, boom, ValueError("bad"))
        self.assertHttp(404, M._guard, boom, FileNotFoundError("nope"))
        self.assertEqual(self.assertHttp(500, M._guard, boom, RuntimeError("x")), "RuntimeError: x")

    def test_index_stamps_assets(self):
        page = M.index().body.decode("utf-8")
        self.assertRegex(page, r"/static/app\.js\?v=\d+")
        self.assertRegex(page, r"/static/styles\.css\?v=\d+")
        self.assertNotIn("tab-diff", page)

    def test_browse(self):
        folder = tempfile.mkdtemp(dir=TMP)
        os.makedirs(os.path.join(folder, "Sub"))
        os.makedirs(os.path.join(folder, ".hidden"))
        for name in ("b.parquet", "A.PARQUET", "notes.txt"):
            open(os.path.join(folder, name), "w").close()
        out = M.browse(folder)
        self.assertEqual([e["name"] for e in out["entries"]], ["A.PARQUET", "b.parquet", "Sub"])
        self.assertEqual(out["parent"], os.path.dirname(folder))
        self.assertEqual(M.browse(os.path.join(folder, "b.parquet"))["path"], folder)
        self.assertIsNone(M.browse("/")["parent"])
        with mock.patch("os.listdir", side_effect=PermissionError("denied")):
            self.assertHttp(400, M.browse, folder)

    def test_dataset_routes(self):
        opened = M.open_dataset(M.OpenRequest(path=FIXTURE))
        did = opened["id"]
        self.assertEqual(opened["row_count"], ROWS)
        self.assertEqual(M._load_recents()[0]["path"], FIXTURE)
        self.assertEqual(M.dataset_info(did)["id"], did)
        page = M.preview(M.QueryRequest(dataset_id=did, columns=["id"], limit=5000, offset=-3, order_by="id"))
        self.assertEqual(len(page["rows"]), ROWS, "limit clamps to 1000, offset to 0")
        self.assertEqual(M.count(M.CountRequest(dataset_id=did)), {"count": ROWS, "total": ROWS})
        vals = M.values(M.ValuesRequest(dataset_id=did, column="id", limit=99999))
        self.assertEqual(len(vals["values"]), ROWS)
        self.assertEqual(M.stats(M.StatsRequest(dataset_id=did, column="qty"))["hi"], 8)
        self.assertHttp(400, M.preview, M.QueryRequest(dataset_id=did, transforms={"amount": "year"}))
        self.assertHttp(404, M.count, M.CountRequest(dataset_id="nope"))
        self.assertHttp(404, M.open_dataset, M.OpenRequest(path=FIXTURE + ".missing"))
        self.assertEqual(M.close_dataset(did), {"closed": True})
        self.assertHttp(404, M.dataset_info, did)

    def run_open(self, **body):
        job = M.start_open(M.OpenStartRequest(**body))
        deadline = time.time() + 30
        while M.open_status(job["id"])["status"] == "running" and time.time() < deadline:
            time.sleep(0.01)
        return M.open_status(job["id"])

    def test_open_job(self):
        done = self.run_open(path=FIXTURE)
        self.assertEqual((done["status"], done["label"], done["dataset"]["row_count"]), ("done", "fixture.parquet", ROWS))
        self.assertEqual([s["message"] for s in done["steps"]][-1], "Ready")
        self.assertTrue(all(s["seconds"] is not None for s in done["steps"]), done["steps"])
        self.assertEqual(M._load_recents()[0]["path"], FIXTURE)
        self.assertEqual(M.dataset_info(done["dataset"]["id"])["name"], "fixture.parquet")
        union = self.run_open(paths=[FIXTURE, FIXTURE + " "], source_column=True)
        self.assertEqual(union["status"], "error")
        self.assertIn("same as file 1", union["error"])
        missing = self.run_open(path=FIXTURE + ".nope")
        self.assertEqual(missing["status"], "error")
        self.assertIn("No such file", missing["error"])
        self.assertHttp(400, M.start_open, M.OpenStartRequest(path="  "))
        self.assertHttp(404, M.open_status, "nope")

    def test_open_job_union(self):
        a = os.path.join(TMP, "oj_a.parquet")
        shutil.copy(FIXTURE, a)
        union = self.run_open(paths=[FIXTURE, a], source_column=False)
        self.assertEqual((union["status"], union["label"], union["dataset"]["row_count"]), ("done", "2 files", ROWS * 2))
        self.assertIsNone(union["dataset"]["source_column"])

    def test_old_open_jobs_are_forgotten(self):
        with mock.patch.object(M, "MAX_OPEN_JOBS", 2):
            ids = [self.run_open(path=FIXTURE)["id"] for _ in range(4)]
        self.assertLessEqual(len(M.open_jobs), 3)
        self.assertIn(ids[-1], M.open_jobs)

    def test_union_route(self):
        a = os.path.join(TMP, "m_a.parquet")
        con.execute("COPY (SELECT * FROM read_parquet('{}') LIMIT 10) TO '{}'".format(FIXTURE, a))
        out = M.union_datasets(M.UnionRequest(paths=[FIXTURE, a]))
        self.assertEqual(out["row_count"], ROWS + 10)
        self.assertHttp(400, M.union_datasets, M.UnionRequest(paths=[FIXTURE]))

    def test_upload(self):
        with open(FIXTURE, "rb") as handle:
            data = handle.read()
        out = asyncio.run(M.upload(UploadFile(io.BytesIO(data), filename="dropped.parquet")))
        self.assertEqual((out["name"], out["row_count"]), ("dropped.parquet", ROWS))
        self.assertTrue(M.engine.get(out["id"]).is_temp)
        with self.assertRaises(HTTPException):
            asyncio.run(M.upload(UploadFile(io.BytesIO(b"x"), filename="notes.csv")))

    def test_pivot_routes(self):
        aggs = M.pivot_aggregations()
        self.assertEqual({a["id"] for a in aggs["aggregations"]}, set(P.AGGREGATIONS))
        self.assertEqual(aggs["export_rows_per_file"], exporter_module.PIVOT_EXPORT_MAX_ROWS)
        did = M.open_dataset(M.OpenRequest(path=FIXTURE))["id"]
        out = M.build_pivot(M.PivotRequest(dataset_id=did, rows=["flag"], values=[M.PivotValue(agg="count_rows")]))
        self.assertEqual(out["rows"][-1]["cells"], [ROWS])
        self.assertHttp(400, M.build_pivot, M.PivotRequest(dataset_id=did, rows=["flag"]))
        job = M.export_pivot(M.PivotExportRequest(dataset_id=did, rows=["flag"],
                                                  values=[M.PivotValue(agg="count_rows")]))
        self.assertEqual(wait(M.exports.get(job["id"])).status, "done")
        self.assertHttp(404, M.export_pivot, M.PivotExportRequest(dataset_id="nope", rows=["flag"]))

    def test_export_routes(self):
        did = M.open_dataset(M.OpenRequest(path=FIXTURE))["id"]
        self.assertHttp(400, M.start_export, M.ExportRequest(dataset_id=did, format="xml"))
        job = M.start_export(M.ExportRequest(dataset_id=did, format="parquet", columns=["id"], row_limit=5))
        self.assertIn(M.export_status(job["id"])["status"], ("queued", "counting", "running", "done"))
        wait(M.exports.get(job["id"]))
        response = M.download_export(job["id"])
        self.assertEqual((response.media_type, response.filename), ("application/octet-stream", job["filename"]))
        self.assertEqual(M.cancel_export(job["id"])["status"], "done")
        pending = ExportJob(id="pending", dataset_id=did, fmt="csv", filename="p.csv")
        M.exports.jobs["pending"] = pending
        self.assertHttp(409, M.download_export, "pending")
        self.assertHttp(404, M.export_status, "nope")

    def test_no_difference_analysis_routes_remain(self):
        paths = {getattr(route, "path", "") for route in M.app.routes}
        self.assertFalse([p for p in paths if "diff" in p], paths)
        self.assertFalse(os.path.exists(os.path.join(os.path.dirname(M.__file__), "diffanalysis.py")))


if __name__ == "__main__":
    unittest.main()
