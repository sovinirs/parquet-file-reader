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

from .filters import FilterError, build_where, quote_ident

NUMERIC_PREFIXES = (
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT",
    "UINTEGER", "UBIGINT", "UHUGEINT", "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC", "INT",
)
TEMPORAL_PREFIXES = ("DATE", "TIME", "TIMESTAMP", "INTERVAL")
TEXT_PREFIXES = ("VARCHAR", "CHAR", "TEXT", "STRING", "UUID", "ENUM", "BLOB", "BIT")


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

    def as_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "type": self.sql_type, "category": self.category}


@dataclass
class Dataset:
    id: str
    path: str
    display_name: str
    columns: List[Column]
    row_count: int
    file_size: int
    file_count: int
    is_temp: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def column_types(self) -> Dict[str, str]:
        return {c.name: c.sql_type for c in self.columns}

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "path": self.path,
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

    def get(self, dataset_id: str) -> Dataset:
        dataset = self.datasets.get(dataset_id)
        if dataset is None:
            raise KeyError("This file is no longer open. Re-open it to continue.")
        return dataset

    def close(self, dataset_id: str) -> None:
        dataset = self.datasets.pop(dataset_id, None)
        if dataset and dataset.is_temp and os.path.isfile(dataset.path):
            try:
                os.remove(dataset.path)
            except OSError:
                pass

    # ---------------------------------------------------------------- queries

    def _from(self) -> str:
        return "read_parquet(?, union_by_name=true)"

    def _select_list(self, dataset: Dataset, columns: Optional[Sequence[str]]) -> Tuple[str, List[Column]]:
        known = {c.name: c for c in dataset.columns}
        chosen = [known[c] for c in columns if c in known] if columns else list(dataset.columns)
        if not chosen:
            chosen = list(dataset.columns)
        return ", ".join(quote_ident(c.name) for c in chosen), chosen

    def query_sql(
        self,
        dataset: Dataset,
        filters: Sequence[Dict[str, Any]],
        columns: Optional[Sequence[str]] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
    ) -> Tuple[str, List[Any], List[Column]]:
        select_list, chosen = self._select_list(dataset, columns)
        where, params = build_where(filters, dataset.column_types)
        sql = "SELECT {} FROM {} {}".format(select_list, self._from(), where)
        if order_by and order_by in dataset.column_types:
            sql += " ORDER BY {} {} NULLS LAST".format(
                quote_ident(order_by), "DESC" if descending else "ASC"
            )
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
    ) -> Dict[str, Any]:
        sql, params, chosen = self.query_sql(dataset, filters, columns, order_by, descending)
        sql += " LIMIT {} OFFSET {}".format(int(limit), int(offset))
        cur = self.cursor()
        rows = cur.execute(sql, params).fetchall()
        return {
            "columns": [c.as_dict() for c in chosen],
            "rows": [[to_jsonable(v) for v in row] for row in rows],
        }

    def count(self, dataset: Dataset, filters: Sequence[Dict[str, Any]]) -> int:
        where, params = build_where(filters, dataset.column_types)
        sql = "SELECT count(*) FROM {} {}".format(self._from(), where)
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
    ) -> Dict[str, Any]:
        """Value list for a filter panel, honouring every *other* column's filter.

        `exact` is a pasted list: return only values equal to one of them
        (ignoring case), instead of substring-matching `search`.
        """
        types = dataset.column_types
        if column not in types:
            raise FilterError("Unknown column: {!r}".format(column))
        sql_type = types[column]
        col = quote_ident(column)
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
            "GROUP BY 1 ORDER BY n DESC, 1 LIMIT {lim}"
        ).format(expr=expr, src=self._from(), where=where, lim=int(limit) + 1)

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
        self, dataset: Dataset, column: str, filters: Sequence[Dict[str, Any]]
    ) -> Dict[str, Any]:
        types = dataset.column_types
        if column not in types:
            raise FilterError("Unknown column: {!r}".format(column))
        col = quote_ident(column)
        category = categorise(types[column])
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

        sql = "SELECT {} FROM {} {}".format(", ".join(aggregates), self._from(), where)
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
    ) -> Iterator[Tuple[List[Column], List[Tuple]]]:
        """Stream the filtered result in chunks. Yields (columns, rows) batches."""
        sql, params, chosen = self.query_sql(dataset, filters, columns, order_by, descending)
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
    ) -> None:
        """Native DuckDB export -- parallel and far faster than row-by-row."""
        sql, params, _ = self.query_sql(dataset, filters, columns, order_by, descending)
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
