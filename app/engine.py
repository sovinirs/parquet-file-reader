"""DuckDB-backed query engine.

Nothing is ever loaded into memory wholesale: the parquet file stays on disk and
DuckDB pushes filters and projections down into it, so a preview of ten rows out
of fifty million touches only the row groups it has to.
"""

import datetime as _dt
import decimal
import os
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import duckdb

from .filters import DATE_FORMATS, FilterError, build_where, column_sql, parse_date_sql, quote_ident

NUMERIC_PREFIXES = (
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT",
    "UINTEGER", "UBIGINT", "UHUGEINT", "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC", "INT",
)
TEMPORAL_PREFIXES = ("DATE", "TIME", "TIMESTAMP", "INTERVAL")
TEXT_PREFIXES = ("VARCHAR", "CHAR", "TEXT", "STRING", "UUID", "ENUM", "BLOB", "BIT")

# Checking for dates stored as text reads this many rows from the top of the
# file -- a row group or two, so opening stays instant however big the file is.
DATE_SAMPLE_ROWS = 2000
# Where dates hide in the wrong type: strings, and integers like 20240131.
DATE_TEXT_TYPES = ("VARCHAR", "CHAR", "TEXT", "STRING")
DATE_INT_TYPES = ("INTEGER", "BIGINT", "UINTEGER", "UBIGINT", "HUGEINT")
DATE_INT_FORMATS = ("%Y%m%d", "%Y%m%d%H%M%S")
# Placeholders that mean "no date" in exported data. They don't count against a
# format (extracting from them gives a blank, like any value that doesn't parse).
DATE_BLANKS = ("null", "none", "n/a", "na", "nan", "nat", "-", "--")


def categorise(sql_type: str) -> str:
    t = sql_type.upper()
    if t.startswith("BOOLEAN"):
        return "boolean"
    if t.startswith(("STRUCT", "MAP", "UNION")) or t.endswith("[]") or t.startswith("LIST"):
        return "complex"
    if t.startswith(NUMERIC_PREFIXES):
        return "numeric"
    if t.startswith(TEMPORAL_PREFIXES):
        return "temporal"
    if t.startswith(TEXT_PREFIXES):
        return "text"
    return "other"


def to_jsonable(value: Any) -> Any:
    """Coerce a DuckDB value into something json.dumps can handle."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        # NaN/Infinity are not valid JSON.
        return value if value == value and abs(value) != float("inf") else str(value)
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, _dt.timedelta):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "0x" + bytes(value)[:64].hex()
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    return str(value)


@dataclass
class Column:
    name: str
    sql_type: str
    category: str
    # Set when a non-date column turned out to hold dates: every format that
    # read the whole sample, best guess first.
    date_formats: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        out = {"name": self.name, "type": self.sql_type, "category": self.category}
        if self.date_formats:
            out["date_formats"] = list(self.date_formats)
        return out


@dataclass
class Dataset:
    id: str
    # One source (a file, or a glob over a folder's parts) -- or, for a union,
    # a list of them. DuckDB's read_parquet takes either as its parameter, so
    # every query in the app handles both without knowing which it has.
    path: Any
    display_name: str
    columns: List[Column]
    row_count: int
    file_size: int
    file_count: int
    is_temp: bool = False
    # Set on a union that tags each row with the file it came from.
    source_column: Optional[str] = None
    # Tag rows with the bare file name rather than the full path -- true unless
    # two of the unioned files share a name.
    source_basename: bool = True
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def column_types(self) -> Dict[str, str]:
        return {c.name: c.sql_type for c in self.columns}

    @property
    def sources(self) -> List[str]:
        return list(self.path) if isinstance(self.path, (list, tuple)) else [self.path]

    @property
    def path_label(self) -> str:
        """The source(s) as one line of text, for titles and export manifests."""
        return " + ".join(self.sources)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "path": self.path_label,
            "sources": self.sources,
            "source_column": self.source_column,
            "name": self.display_name,
            "columns": [c.as_dict() for c in self.columns],
            "row_count": self.row_count,
            "file_size": self.file_size,
            "file_count": self.file_count,
        }


class Engine:
    """Owns the DuckDB instance and the set of currently open datasets."""

    def __init__(self, threads: Optional[int] = None, memory_limit: Optional[str] = None):
        self._conn = duckdb.connect(database=":memory:")
        n_threads = threads or max(2, (os.cpu_count() or 4))
        self._conn.execute("SET threads TO {}".format(n_threads))
        if memory_limit:
            self._conn.execute("SET memory_limit = '{}'".format(memory_limit))
        # Spill to disk instead of dying on a big sort/aggregate.
        self._conn.execute("SET preserve_insertion_order = false")
        self.datasets: Dict[str, Dataset] = {}

    def cursor(self):
        """DuckDB connections are not safe to share across threads; cursors are."""
        return self._conn.cursor()

    # ---------------------------------------------------------------- opening

    @staticmethod
    def _resolve(path: str) -> Tuple[str, int, int]:
        """Return (duckdb source glob, file count, total bytes)."""
        expanded = os.path.abspath(os.path.expanduser(path.strip()))
        if os.path.isdir(expanded):
            source = os.path.join(expanded, "**", "*.parquet")
            files = []
            for root, _dirs, names in os.walk(expanded):
                files.extend(os.path.join(root, n) for n in names if n.lower().endswith(".parquet"))
            if not files:
                raise FileNotFoundError("No .parquet files found under {}".format(expanded))
            return source, len(files), sum(os.path.getsize(f) for f in files)
        if not os.path.exists(expanded):
            raise FileNotFoundError("No such file: {}".format(expanded))
        return expanded, 1, os.path.getsize(expanded)

    def open(self, path: str, is_temp: bool = False, display_name: Optional[str] = None) -> Dataset:
        source, file_count, size = self._resolve(path)
        cur = self.cursor()
        try:
            described = cur.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)", [source]
            ).fetchall()
        except duckdb.Error as exc:
            raise ValueError("Could not read parquet: {}".format(exc))

        columns = [Column(name=row[0], sql_type=row[1], category=categorise(row[1])) for row in described]
        self._detect_dates(cur, source, columns)
        if not columns:
            raise ValueError("That parquet file has no columns.")

        # Reads only the parquet footer metadata -- instant even for huge files.
        row_count = cur.execute(
            "SELECT count(*) FROM read_parquet(?, union_by_name=true)", [source]
        ).fetchone()[0]

        dataset = Dataset(
            id=uuid.uuid4().hex[:12],
            path=source,
            display_name=display_name or os.path.basename(source.rstrip(os.sep)) or source,
            columns=columns,
            row_count=int(row_count),
            file_size=int(size),
            file_count=file_count,
            is_temp=is_temp,
        )
        self.datasets[dataset.id] = dataset
        return dataset

    def open_union(self, paths: Sequence[str], source_column: bool = True) -> Dataset:
        """Open several parquet sources as one table, stacked on top of each other.

        Each source can be a file or a folder of parts. They must share a schema:
        the same column names with the same types. Column order may differ, since
        columns are matched by name.
        """
        cleaned = [p for p in (str(p).strip() for p in paths or []) if p]
        if len(cleaned) < 2:
            raise ValueError("Choose at least two parquet files to union.")

        resolved = [self._resolve(p) for p in cleaned]
        sources = [source for source, _, _ in resolved]
        seen = {}
        for index, source in enumerate(sources):
            if source in seen:
                raise ValueError("File {} is the same as file {}: {}".format(
                    index + 1, seen[source] + 1, source))
            seen[source] = index

        cur = self.cursor()
        schemas = []
        for index, source in enumerate(sources):
            try:
                described = cur.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)", [source]
                ).fetchall()
            except duckdb.Error as exc:
                raise ValueError("Could not read file {} ({}): {}".format(index + 1, source, exc))
            schemas.append([(row[0], row[1]) for row in described])

        first = dict(schemas[0])
        problems = []
        for index, schema in enumerate(schemas[1:], start=2):
            other = dict(schema)
            missing = [name for name in first if name not in other]
            extra = [name for name in other if name not in first]
            retyped = ["{} ({} vs {})".format(name, first[name], other[name])
                       for name in first if name in other and first[name] != other[name]]
            if missing or extra or retyped:
                bits = []
                if missing:
                    bits.append("missing " + ", ".join(missing))
                if extra:
                    bits.append("extra " + ", ".join(extra))
                if retyped:
                    bits.append("different types: " + ", ".join(retyped))
                problems.append("File {} ({}) does not match file 1: {}.".format(
                    index, os.path.basename(sources[index - 1].rstrip(os.sep)), "; ".join(bits)))
        if problems:
            raise ValueError("These files do not share a schema, so they cannot be unioned. "
                             + " ".join(problems))

        columns = [Column(name=n, sql_type=t, category=categorise(t)) for n, t in schemas[0]]
        self._detect_dates(cur, sources, columns)
        tag = None
        if source_column:
            taken = {c.name for c in columns}
            tag = "source_file"
            suffix = 1
            while tag in taken:
                suffix += 1
                tag = "source_file_{}".format(suffix)
            columns.append(Column(name=tag, sql_type="VARCHAR", category="text"))

        row_count = cur.execute(
            "SELECT count(*) FROM read_parquet(?, union_by_name=true)", [sources]
        ).fetchone()[0]

        names = [os.path.basename(s.rstrip(os.sep)) or s for s in sources]
        dataset = Dataset(
            id=uuid.uuid4().hex[:12],
            path=sources,
            display_name=" + ".join(names) if len(names) <= 3
            else "{} + {} more".format(names[0], len(names) - 1),
            columns=columns,
            row_count=int(row_count),
            file_size=sum(size for _, _, size in resolved),
            file_count=sum(count for _, count, _ in resolved),
            source_column=tag,
            # Folders expand to many files, so there the file name alone can repeat.
            source_basename=all(count == 1 for _, count, _ in resolved) and len(set(names)) == len(names),
        )
        self.datasets[dataset.id] = dataset
        return dataset

    @staticmethod
    def _detect_dates(cur, source: Any, columns: List[Column]) -> None:
        """Spot text (and YYYYMMDD integer) columns that really hold dates.

        One query over the first DATE_SAMPLE_ROWS rows counts, per column and
        format, the non-blank values that fail to parse. A format that reads
        every one of them is a match. Nothing is decided on a column with no
        values in the sample.
        """
        checks = []
        for column in columns:
            upper = column.sql_type.upper()
            if upper.startswith(DATE_TEXT_TYPES):
                formats = DATE_FORMATS
            elif upper.startswith(DATE_INT_TYPES):
                formats = DATE_INT_FORMATS
            else:
                continue
            checks.append((column, formats))
        if not checks:
            return

        parts, index = [], 0
        for position, (column, formats) in enumerate(checks):
            col = 's.c{}'.format(position)
            present = "lower(trim(CAST({} AS VARCHAR))) NOT IN ('', {})".format(
                col, ", ".join("'{}'".format(b) for b in DATE_BLANKS))
            parts.append("count(*) FILTER (WHERE {})".format(present))
            for fmt in formats:
                parts.append("count(*) FILTER (WHERE {} AND {} IS NULL)".format(
                    present, parse_date_sql(col, fmt)))
        sample = ", ".join("{} AS c{}".format(quote_ident(column.name), position)
                           for position, (column, _) in enumerate(checks))
        sql = "SELECT {} FROM (SELECT {} FROM read_parquet(?, union_by_name=true) LIMIT {}) AS s".format(
            ", ".join(parts), sample, DATE_SAMPLE_ROWS)
        try:
            counts = cur.execute(sql, [source]).fetchone()
        except duckdb.Error:
            return  # a best-effort hint, never a reason to refuse the file

        for column, formats in checks:
            present = counts[index]
            failures = counts[index + 1:index + 1 + len(formats)]
            index += 1 + len(formats)
            if present:
                column.date_formats = [fmt for fmt, bad in zip(formats, failures) if bad == 0]

    def get(self, dataset_id: str) -> Dataset:
        dataset = self.datasets.get(dataset_id)
        if dataset is None:
            raise KeyError("This file is no longer open. Re-open it to continue.")
        return dataset

    def close(self, dataset_id: str) -> None:
        dataset = self.datasets.pop(dataset_id, None)
        if dataset and dataset.is_temp and isinstance(dataset.path, str) and os.path.isfile(dataset.path):
            try:
                os.remove(dataset.path)
            except OSError:
                pass

    # ---------------------------------------------------------------- queries

    @staticmethod
    def _from(dataset: Optional[Dataset] = None) -> str:
        """The table expression every query reads from; its one parameter is dataset.path."""
        if dataset is not None and dataset.source_column:
            # The names here are generated by open_union, never user input.
            # DuckDB pushes filters and projections through this subquery, so
            # it reads no more of the files than the bare read_parquet would.
            tag = "parse_filename(__pqs_src)" if dataset.source_basename else "__pqs_src"
            return ("(SELECT * EXCLUDE (__pqs_src), {} AS {} "
                    "FROM read_parquet(?, union_by_name=true, filename='__pqs_src'))").format(
                        tag, quote_ident(dataset.source_column))
        return "read_parquet(?, union_by_name=true)"

    @staticmethod
    def _transform_of(value: Any) -> Tuple[Optional[str], Optional[str]]:
        """A transforms entry as (part, date format). "year" is shorthand for a
        real date column; a text date column sends {"part", "format"}."""
        if isinstance(value, dict):
            return value.get("part") or None, value.get("format") or None
        return value or None, None

    def _select_list(
        self,
        dataset: Dataset,
        columns: Optional[Sequence[str]],
        transforms: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, List[Column]]:
        """SELECT list for the grid/export. An extracted column keeps its name but
        holds just the part -- `year("order_date") AS "order_date"` -- and is
        reported with the part's type, so Excel writes it as a number."""
        transforms = transforms or {}
        for name in transforms:
            if name not in dataset.column_types:
                raise FilterError("Unknown column: {!r}".format(name))
        known = {c.name: c for c in dataset.columns}
        chosen = [known[c] for c in columns if c in known] if columns else list(dataset.columns)
        if not chosen:
            chosen = list(dataset.columns)
        parts, shaped = [], []
        for column in chosen:
            part, date_format = self._transform_of(transforms.get(column.name))
            if part:
                expr, sql_type = column_sql(column.name, column.sql_type, part, date_format)
                parts.append("{} AS {}".format(expr, quote_ident(column.name)))
                shaped.append(Column(name=column.name, sql_type=sql_type, category=categorise(sql_type)))
            else:
                parts.append(quote_ident(column.name))
                shaped.append(column)
        return ", ".join(parts), shaped

    def query_sql(
        self,
        dataset: Dataset,
        filters: Sequence[Dict[str, Any]],
        columns: Optional[Sequence[str]] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
        transforms: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, List[Any], List[Column]]:
        select_list, chosen = self._select_list(dataset, columns, transforms)
        where, params = build_where(filters, dataset.column_types)
        sql = "SELECT {} FROM {} {}".format(select_list, self._from(dataset), where)
        if order_by and order_by in dataset.column_types:
            # Spelled out rather than by name: an extracted column's alias
            # shadows the raw one, and the sort should follow what is shown.
            key, _ = column_sql(order_by, dataset.column_types[order_by],
                                *self._transform_of((transforms or {}).get(order_by)))
            sql += " ORDER BY {} {} NULLS LAST".format(key, "DESC" if descending else "ASC")
        return sql, [dataset.path] + params, chosen

    def preview(
        self,
        dataset: Dataset,
        filters: Sequence[Dict[str, Any]],
        columns: Optional[Sequence[str]] = None,
        limit: int = 10,
        offset: int = 0,
        order_by: Optional[str] = None,
        descending: bool = False,
        transforms: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        sql, params, chosen = self.query_sql(dataset, filters, columns, order_by, descending, transforms)
        sql += " LIMIT {} OFFSET {}".format(int(limit), int(offset))
        cur = self.cursor()
        rows = cur.execute(sql, params).fetchall()
        return {
            "columns": [c.as_dict() for c in chosen],
            "rows": [[to_jsonable(v) for v in row] for row in rows],
        }

    def count(self, dataset: Dataset, filters: Sequence[Dict[str, Any]]) -> int:
        where, params = build_where(filters, dataset.column_types)
        sql = "SELECT count(*) FROM {} {}".format(self._from(dataset), where)
        cur = self.cursor()
        return int(cur.execute(sql, [dataset.path] + params).fetchone()[0])

    def distinct_values(
        self,
        dataset: Dataset,
        column: str,
        filters: Sequence[Dict[str, Any]],
        search: str = "",
        limit: int = 500,
        exact: Optional[Sequence[str]] = None,
        transform: Optional[str] = None,
        date_format: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Value list for a filter panel, honouring every *other* column's filter.

        `exact` is a pasted list: return only values equal to one of them
        (ignoring case), instead of substring-matching `search`. `transform`
        lists an extracted part (the years of a date) instead of the raw values.
        """
        types = dataset.column_types
        if column not in types:
            raise FilterError("Unknown column: {!r}".format(column))
        col, sql_type = column_sql(column, types[column], transform, date_format)
        expr = col if categorise(sql_type) != "complex" else "CAST({} AS VARCHAR)".format(col)

        where, params = build_where(filters, types, skip_column=column)
        params = [dataset.path] + params
        if exact:
            clause = "lower(CAST({} AS VARCHAR)) IN ({})".format(col, ", ".join("lower(?)" for _ in exact))
            where = "{} AND {}".format(where, clause) if where else "WHERE " + clause
            params.extend(str(v) for v in exact)
        elif search:
            clause = "CAST({} AS VARCHAR) ILIKE ? ESCAPE '\\'".format(col)
            needle = "%{}%".format(str(search).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_"))
            where = "{} AND {}".format(where, clause) if where else "WHERE " + clause
            params.append(needle)

        sql = (
            "SELECT {expr} AS v, count(*) AS n FROM {src} {where} "
            "GROUP BY 1 ORDER BY {order} LIMIT {lim}"
        ).format(expr=expr, src=self._from(dataset), where=where, lim=int(limit) + 1,
                 # Years, months and days read best in calendar order.
                 order="1 NULLS LAST" if transform else "n DESC, 1")

        cur = self.cursor()
        rows = cur.execute(sql, params).fetchall()
        truncated = len(rows) > limit
        rows = rows[:limit]
        return {
            "column": column,
            "values": [{"value": to_jsonable(v), "count": int(n)} for v, n in rows],
            "truncated": truncated,
        }

    def column_stats(
        self, dataset: Dataset, column: str, filters: Sequence[Dict[str, Any]],
        transform: Optional[str] = None,
        date_format: Optional[str] = None,
    ) -> Dict[str, Any]:
        types = dataset.column_types
        if column not in types:
            raise FilterError("Unknown column: {!r}".format(column))
        col, sql_type = column_sql(column, types[column], transform, date_format)
        category = categorise(sql_type)
        where, params = build_where(filters, types, skip_column=column)
        params = [dataset.path] + params

        aggregates = [
            "count(*) AS total",
            "count({}) AS non_null".format(col),
            "approx_count_distinct({}) AS distinct_approx".format(
                col if category != "complex" else "CAST({} AS VARCHAR)".format(col)
            ),
        ]
        if category in ("numeric", "temporal"):
            aggregates += ["min({0}) AS lo".format(col), "max({0}) AS hi".format(col)]
        if category == "numeric":
            aggregates += ["avg(CAST({0} AS DOUBLE)) AS mean".format(col),
                           "median(CAST({0} AS DOUBLE)) AS med".format(col)]

        sql = "SELECT {} FROM {} {}".format(", ".join(aggregates), self._from(dataset), where)
        cur = self.cursor()
        row = cur.execute(sql, params).fetchone()
        names = [d[0] for d in cur.description]
        stats = {n: to_jsonable(v) for n, v in zip(names, row)}
        stats["column"] = column
        stats["category"] = category
        stats["nulls"] = int(stats.get("total", 0)) - int(stats.get("non_null", 0))
        return stats

    def iter_rows(
        self,
        dataset: Dataset,
        filters: Sequence[Dict[str, Any]],
        columns: Optional[Sequence[str]] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
        chunk_size: int = 50_000,
        row_limit: Optional[int] = None,
        transforms: Optional[Dict[str, str]] = None,
    ) -> Iterator[Tuple[List[Column], List[Tuple]]]:
        """Stream the filtered result in chunks. Yields (columns, rows) batches."""
        sql, params, chosen = self.query_sql(dataset, filters, columns, order_by, descending, transforms)
        if row_limit:
            sql += " LIMIT {}".format(int(row_limit))
        cur = self.cursor()
        cur.execute(sql, params)
        while True:
            batch = cur.fetchmany(chunk_size)
            if not batch:
                break
            yield chosen, batch

    def copy_to(
        self,
        dataset: Dataset,
        destination: str,
        fmt: str,
        filters: Sequence[Dict[str, Any]],
        columns: Optional[Sequence[str]] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
        row_limit: Optional[int] = None,
        transforms: Optional[Dict[str, str]] = None,
    ) -> None:
        """Native DuckDB export -- parallel and far faster than row-by-row."""
        sql, params, _ = self.query_sql(dataset, filters, columns, order_by, descending, transforms)
        if row_limit:
            sql += " LIMIT {}".format(int(row_limit))
        options = {
            "csv": "(FORMAT CSV, HEADER, DELIMITER ',')",
            "parquet": "(FORMAT PARQUET, COMPRESSION ZSTD)",
            "json": "(FORMAT JSON, ARRAY true)",
        }[fmt]
        escaped = destination.replace("'", "''")
        cur = self.cursor()
        cur.execute("COPY ({}) TO '{}' {}".format(sql, escaped, options), params)
