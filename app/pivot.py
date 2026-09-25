"""Cross-tab (pivot table) computation over a parquet dataset.

A single `GROUP BY GROUPING SETS` query returns the detail cells *and* every
subtotal in one pass. That matters twice over: the file is scanned once no
matter how deep the pivot is, and every total is aggregated from the raw rows
rather than from the cells above it -- so a "Total" column under an Average
measure is the real average, never an average of averages.

The result is assembled here into a shape that both the browser grid and the
Excel writer can walk straight through: a list of header levels, a flat list of
leaf columns, and a list of rows tagged data / subtotal / grand.
"""

import datetime as _dt
import decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .engine import Dataset, Engine, categorise, to_jsonable
from .filters import FilterError, build_where, quote_ident

# A pivot wider than this is unreadable long before it is slow.
MAX_COLUMN_KEYS = 200
# Row groups returned to the caller; beyond it the pivot is truncated.
DEFAULT_MAX_ROW_KEYS = 20_000
# The assembly below is in-memory, and a row group costs roughly a kilobyte once
# its cells are held. Past this many the pivot stops building the whole thing and
# works over a window instead -- unless the caller explicitly asks for more rows
# than this, which the Excel export does so that a file is never short a group.
HARD_MAX_ROW_KEYS = 50_000

NUMERIC_ONLY = ("numeric",)
ORDERABLE = ("numeric", "temporal", "text", "boolean")

# agg -> (verb used in the label, SQL template, allowed column categories)
AGGREGATIONS: Dict[str, Tuple[str, str, Optional[Tuple[str, ...]]]] = {
    "sum": ("Sum", "sum({c})", NUMERIC_ONLY),
    "avg": ("Average", "avg(CAST({c} AS DOUBLE))", NUMERIC_ONLY),
    "min": ("Min", "min({c})", ORDERABLE),
    "max": ("Max", "max({c})", ORDERABLE),
    "count": ("Count", "count({c})", None),
    "count_distinct": ("Distinct count", "count(DISTINCT {c})", None),
    "median": ("Median", "median(CAST({c} AS DOUBLE))", NUMERIC_ONLY),
    "stddev": ("Std dev", "stddev_samp(CAST({c} AS DOUBLE))", NUMERIC_ONLY),
    "count_rows": ("Count of rows", "count(*)", None),
}

# Aggregations whose result is a count, so it is always a whole number.
INTEGER_AGGS = {"count", "count_distinct", "count_rows"}


class PivotError(FilterError):
    """The pivot as specified cannot be built."""


class ValueField:
    """One measure: a column, an aggregation, and the SQL that computes it."""

    __slots__ = ("column", "agg", "label", "sql", "is_count")

    def __init__(self, column: str, agg: str, label: str, sql: str, is_count: bool):
        self.column = column
        self.agg = agg
        self.label = label
        self.sql = sql
        self.is_count = is_count

    def as_dict(self) -> Dict[str, Any]:
        return {"column": self.column, "agg": self.agg, "label": self.label,
                "is_count": self.is_count}


def default_label(column: str, agg: str) -> str:
    verb = AGGREGATIONS[agg][0]
    return verb if agg == "count_rows" else "{} of {}".format(verb, column)


def _measure(spec: Dict[str, Any], types: Dict[str, str]) -> ValueField:
    agg = str(spec.get("agg") or "sum").lower()
    if agg not in AGGREGATIONS:
        raise PivotError("Unsupported aggregation: {!r}".format(agg))
    _verb, template, allowed = AGGREGATIONS[agg]

    if agg == "count_rows":
        label = str(spec.get("label") or default_label("", agg))
        return ValueField("", agg, label, "count(*)", True)

    column = spec.get("column")
    if column not in types:
        raise PivotError("Unknown column: {!r}".format(column))
    category = categorise(types[column])
    if allowed is not None and category not in allowed:
        raise PivotError(
            "{} cannot be applied to {!r} ({}). Try Count or Distinct count.".format(
                AGGREGATIONS[agg][0], column, types[column])
        )
    col = quote_ident(column)
    if category == "complex":
        if agg not in ("count", "count_distinct"):
            raise PivotError(
                "{!r} is a nested column -- only Count and Distinct count work on it.".format(column))
        col = "CAST({} AS VARCHAR)".format(col)

    label = str(spec.get("label") or default_label(column, agg))
    return ValueField(column, agg, label, template.format(c=col), agg in INTEGER_AGGS)


def _sort_key(value: Any) -> Tuple[int, float, str]:
    """Order mixed-type group keys deterministically, blanks last."""
    if value is None:
        return (3, 0.0, "")
    if isinstance(value, bool):
        return (0, 1.0 if value else 0.0, "")
    if isinstance(value, (int, float, decimal.Decimal)):
        try:
            return (0, float(value), "")
        except (ValueError, OverflowError):
            return (2, 0.0, str(value))
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return (1, 0.0, value.isoformat())
    return (2, 0.0, str(value))


def _key_sort(key: Sequence[Any]) -> Tuple:
    return tuple(_sort_key(v) for v in key)


def label_of(value: Any) -> str:
    """Header / row label for one group key."""
    if value is None:
        return "(blank)"
    if value == "":
        return "(empty)"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, _dt.datetime):
        text = value.isoformat(sep=" ")
        # A date column read as TIMESTAMP is all midnights; do not shout it.
        return text[:10] if text.endswith(" 00:00:00") else text
    if isinstance(value, (_dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(float(value))
    return str(value)


def compute(
    engine: Engine,
    dataset: Dataset,
    rows: Sequence[str],
    columns: Sequence[str],
    values: Sequence[Dict[str, Any]],
    filters: Sequence[Dict[str, Any]] = (),
    subtotals: bool = True,
    row_totals: bool = True,
    column_totals: bool = True,
    sort: Optional[Dict[str, Any]] = None,
    max_rows: int = DEFAULT_MAX_ROW_KEYS,
    max_columns: int = MAX_COLUMN_KEYS,
    jsonable: bool = True,
    grand_label: str = "Grand Total",
) -> Dict[str, Any]:
    """Build the pivot; see the module docstring for the shape returned.

    With `jsonable` the cells are coerced for json.dumps. The Excel writer turns
    it off so dates stay native and land in the sheet as real dates.
    """
    types = dataset.column_types
    rows = list(rows or [])
    columns = list(columns or [])

    for field in rows + columns:
        if field not in types:
            raise PivotError("Unknown column: {!r}".format(field))
        if categorise(types[field]) == "complex":
            raise PivotError(
                "{!r} is a nested column ({}) -- it cannot group a pivot. Use it in "
                "Values with Count instead.".format(field, types[field]))
    if len(set(rows)) != len(rows) or len(set(columns)) != len(columns):
        raise PivotError("The same field cannot be used twice in Rows or in Columns.")
    overlap = set(rows) & set(columns)
    if overlap:
        raise PivotError("{!r} is in both Rows and Columns -- use it in one or the other.".format(
            sorted(overlap)[0]))
    if not values:
        raise PivotError("Add at least one field to Values.")
    if not rows and not columns:
        raise PivotError("Add at least one field to Rows or Columns.")

    measures = [_measure(spec, types) for spec in values]
    max_rows = max(1, int(max_rows))
    max_columns = max(1, min(int(max_columns), 4096))
    # Hold the whole pivot only up to the memory ceiling -- or further, if that is
    # what was asked for: a caller wanting more rows than fit on screen has
    # already accepted the cost.
    overflow_at = max(HARD_MAX_ROW_KEYS, max_rows)

    # Count the groups before building any of them. It is one small GROUP BY over
    # the row fields -- a few milliseconds, and the file is in DuckDB's buffer
    # pool for the real query that follows -- and it buys two things: a row-group
    # count that is exact however large the pivot is, and the chance to not build
    # a pivot that will not fit.
    total_groups = count_row_groups(engine, dataset, rows, filters) if rows else 1
    value_sort = bool(rows and sort and sort.get("by") == "value")
    windowed = bool(rows) and total_groups > overflow_at

    if windowed:
        grouped, meta = _run_query(engine, dataset, rows, columns, measures, filters,
                                   subtotals, max_columns, key_limit=max_rows)
    else:
        try:
            grouped, meta = _run_query(engine, dataset, rows, columns, measures, filters,
                                       subtotals, max_columns, overflow_at=overflow_at)
        except _TooManyRowKeys:
            # The count said this would fit and it did not. Fall back rather than
            # fill memory: the data can change under a re-read.
            windowed = True
            total_groups = count_row_groups(engine, dataset, rows, filters)
            grouped, meta = _run_query(engine, dataset, rows, columns, measures, filters,
                                       subtotals, max_columns, key_limit=max_rows)
    cells, col_keys, row_keys = grouped

    if windowed:
        # The window narrowed the scan, so every total it produced covers the
        # window rather than the file. Restore the two promises the pivot makes:
        # end the window on an outer-group boundary so no subtotal is left half
        # counted, and re-read the grand total from the whole filtered file.
        if len(rows) > 1 and subtotals:
            row_keys = _whole_outer_groups(row_keys)
        cells.update(_grand_totals(engine, dataset, columns, measures, filters))

    ordered_cols = sorted(col_keys, key=_key_sort)
    leaves, header, lookups = _build_header(columns, ordered_cols, measures, row_totals)
    body, shown = _build_rows(rows, cells, row_keys, lookups, measures, subtotals,
                              column_totals, sort, max_rows, grand_label)
    # `shown` is what actually reached the caller -- fewer than the limit when the
    # window stopped at a group boundary -- so it is what the total is measured
    # against.
    truncated_total = total_groups if total_groups > shown else None
    if jsonable:
        for row in body:
            row["cells"] = [to_jsonable(v) for v in row["cells"]]

    return {
        "row_fields": [{"name": n, "type": types[n]} for n in rows],
        "column_fields": [{"name": n, "type": types[n]} for n in columns],
        "values": [m.as_dict() for m in measures],
        "header": header,
        "leaves": leaves,
        "rows": body,
        "row_groups": shown,
        "total_row_groups": truncated_total or shown,
        "truncated": truncated_total is not None,
        "row_group_limit": max_rows,
        # A windowed pivot only ever saw the first `max_rows` keys, so a sort by
        # value ranks those -- not the whole pivot. Say so rather than imply a
        # top-N the numbers do not support.
        "sort_limited": bool(windowed and value_sort),
        "column_groups": len(ordered_cols) if columns else 0,
        "scanned_rows": meta.get("scanned"),
    }


# ------------------------------------------------------------------ the query

def _grouping_sets(rows: List[str], columns: List[str], subtotals: bool,
                   want_row_totals: bool) -> List[List[str]]:
    """Prefix grouping sets: detail, each subtotal level, and the grand total."""
    sets: List[List[str]] = []
    seen = set()

    def add(fields: List[str]) -> None:
        key = tuple(fields)
        if key not in seen:
            seen.add(key)
            sets.append(fields)

    levels = list(range(len(rows), -1, -1)) if subtotals else sorted({len(rows), 0}, reverse=True)
    for k in levels:
        add(rows[:k] + columns)
        if columns and want_row_totals:
            add(rows[:k])
    return sets


class _TooManyRowKeys(Exception):
    """The unbounded pass produced more row groups than can be held in memory."""


def count_row_groups(engine, dataset, rows, filters=()) -> int:
    """How many row groups this pivot has, without building any of it.

    One `GROUP BY` over the row fields alone -- no measures, no grouping sets --
    so the answer is exact however far past the display limit it lands.
    """
    if not rows:
        return 1
    where, params = build_where(filters, dataset.column_types)
    keys = ", ".join(quote_ident(f) for f in rows)
    sql = ("SELECT count(*) FROM (SELECT {keys} FROM {src} "
           "{where} GROUP BY {keys})").format(src=engine._from(dataset), keys=keys, where=where)
    cur = engine.cursor()
    cur.execute(sql, [dataset.path] + params)
    return int(cur.fetchone()[0])


def outer_group_sizes(engine, dataset, rows, filters=()) -> List[Tuple[Any, int]]:
    """(value, row-group count) for each value of the first Rows field.

    What an export needs to decide where to cut: splitting on whole values of the
    outermost field keeps every group -- and so every subtotal -- inside one file.
    """
    if not rows:
        return []
    where, params = build_where(filters, dataset.column_types)
    keys = ", ".join(quote_ident(f) for f in rows)
    outer = quote_ident(rows[0])
    sql = ("SELECT {outer}, count(*) FROM (SELECT {keys} FROM {src} "
           "{where} GROUP BY {keys}) GROUP BY {outer} ORDER BY {outer} NULLS LAST").format(
               src=engine._from(dataset), outer=outer, keys=keys, where=where)
    cur = engine.cursor()
    cur.execute(sql, [dataset.path] + params)
    return [(record[0], int(record[1])) for record in cur.fetchall()]


def overall_totals(engine, dataset, values, filters=()) -> List[Tuple[str, Any]]:
    """Each measure aggregated over the whole filtered file, ungrouped.

    A split export needs this: every file's own total row covers only that file,
    so the figure for the pivot as a whole has to come from somewhere.
    """
    measures = [_measure(spec, dataset.column_types) for spec in values]
    where, params = build_where(filters, dataset.column_types)
    sql = "SELECT {sel} FROM {src} {where}".format(
        src=engine._from(dataset), sel=", ".join(m.sql for m in measures), where=where)
    cur = engine.cursor()
    cur.execute(sql, [dataset.path] + params)
    record = cur.fetchone() or [None] * len(measures)
    return [(m.label, record[i]) for i, m in enumerate(measures)]


def _key_window_sql(src: str, rows: List[str], where: str, limit: int) -> str:
    """A CTE holding just the first `limit` row keys, in row-label order.

    Blanks sort last in the grid, so they sort last here too -- the window has to
    be the same set of keys the grid would have shown had it all fit.
    """
    keys = ", ".join(quote_ident(f) for f in rows)
    order = ", ".join("{} NULLS LAST".format(quote_ident(f)) for f in rows)
    return ("WITH __keys AS (SELECT {keys} FROM {src} {where} "
            "GROUP BY {keys} ORDER BY {order} LIMIT {limit})").format(
                src=src, keys=keys, where=where, order=order, limit=int(limit))


def _run_query(engine, dataset, rows, columns, measures, filters, subtotals, max_columns,
               overflow_at=HARD_MAX_ROW_KEYS, key_limit=None):
    key_fields = rows + columns
    select_parts = [quote_ident(f) for f in key_fields]
    select_parts += ["GROUPING({}) AS {}".format(quote_ident(f), quote_ident("__g{}".format(i)))
                     for i, f in enumerate(key_fields)]
    select_parts += ["{} AS {}".format(m.sql, quote_ident("__v{}".format(i)))
                     for i, m in enumerate(measures)]

    sets = _grouping_sets(rows, columns, subtotals, True)
    rendered = ", ".join(
        "({})".format(", ".join(quote_ident(f) for f in group)) if group else "()"
        for group in sets
    )
    where, params = build_where(filters, dataset.column_types)
    if key_limit is None:
        sql = ("SELECT {sel} FROM {src} {where} "
               "GROUP BY GROUPING SETS ({sets})").format(
                   src=engine._from(dataset), sel=", ".join(select_parts), where=where, sets=rendered)
        args = [dataset.path] + params
    else:
        # Too big to hold whole: narrow the scan to the row keys that will be
        # shown before aggregating, so memory tracks the window, not the file.
        # NULL keys are real group keys here, so they have to be matched as
        # values rather than by equality.
        on = " AND ".join("__t.{c} IS NOT DISTINCT FROM __keys.{c}".format(c=quote_ident(f))
                          for f in rows)
        sql = ("{cte} SELECT {sel} FROM {src} AS __t "
               "SEMI JOIN __keys ON {on} {where} GROUP BY GROUPING SETS ({sets})").format(
                   cte=_key_window_sql(engine._from(dataset), rows, where, key_limit),
                   src=engine._from(dataset), sel=", ".join(select_parts),
                   on=on, where=where, sets=rendered)
        args = [dataset.path] + params + [dataset.path] + params

    cur = engine.cursor()
    cur.execute(sql, args)

    n_rows, n_cols = len(rows), len(columns)
    cells: Dict[Tuple[Tuple, Optional[Tuple]], List[Any]] = {}
    col_keys = set()
    row_keys = set()
    scanned = 0

    while True:
        batch = cur.fetchmany(20_000)
        if not batch:
            break
        for record in batch:
            scanned += 1
            keys = record[:n_rows + n_cols]
            flags = record[n_rows + n_cols: 2 * (n_rows + n_cols)]
            data = record[2 * (n_rows + n_cols):]

            # Grouping sets are prefixes, so the present row fields are the leading ones.
            depth = 0
            for i in range(n_rows):
                if flags[i]:
                    break
                depth += 1
            has_cols = bool(n_cols) and not flags[n_rows]

            row_key = tuple(keys[:depth])
            col_key = tuple(keys[n_rows:n_rows + n_cols]) if has_cols else None

            if col_key is not None:
                col_keys.add(col_key)
                if len(col_keys) > max_columns:
                    raise PivotError(
                        "This pivot needs more than {} column groups. Put a field with fewer "
                        "distinct values in Columns, or filter the data first.".format(max_columns))
            if depth == n_rows and n_rows:
                row_keys.add(row_key)
                if key_limit is None and len(row_keys) > overflow_at:
                    # Abandon this pass rather than fill memory with rows nobody
                    # asked for; the caller counts the groups and comes back with
                    # a window.
                    cur.close()
                    raise _TooManyRowKeys()

            cells[(row_key, col_key)] = list(data)

    if not n_rows:
        row_keys.add(())
    return (cells, col_keys, row_keys), {"scanned": scanned}


def _whole_outer_groups(row_keys):
    """Drop a trailing outer group the window cut in half.

    A subtotal has to cover its whole group or it is a lie, so the window ends
    where a group ends. Only worth doing when there are subtotals to keep whole:
    with one row field every group is a single row and nothing is ever cut. The
    last group is kept when it is the only one -- there is nothing to show
    otherwise.
    """
    outer = {key[0] for key in row_keys}
    if len(outer) < 2:
        return row_keys
    last = max(outer, key=_sort_key)
    return {key for key in row_keys if key[0] != last}


def _grand_totals(engine, dataset, columns, measures, filters):
    """The grand-total row, aggregated over the whole filtered file.

    Grouping only by the column fields, so this is one small query however many
    row groups the pivot has -- and its numbers are the ones the pivot would have
    reported had every row fitted on screen.
    """
    select_parts = [quote_ident(f) for f in columns]
    # A column field can hold NULLs of its own, so only GROUPING() can tell the
    # "total across every column group" record from a genuine (blank) group.
    select_parts += ["GROUPING({}) AS {}".format(quote_ident(f), quote_ident("__g{}".format(i)))
                     for i, f in enumerate(columns)]
    select_parts += ["{} AS {}".format(m.sql, quote_ident("__v{}".format(i)))
                     for i, m in enumerate(measures)]
    sets = "({}), ()".format(", ".join(quote_ident(f) for f in columns)) if columns else "()"
    where, params = build_where(filters, dataset.column_types)
    sql = ("SELECT {sel} FROM {src} {where} "
           "GROUP BY GROUPING SETS ({sets})").format(
               src=engine._from(dataset), sel=", ".join(select_parts), where=where, sets=sets)

    cur = engine.cursor()
    cur.execute(sql, [dataset.path] + params)
    n_cols = len(columns)
    out = {}
    for record in cur.fetchall():
        key = tuple(record[:n_cols])
        flags = record[n_cols:2 * n_cols]
        data = list(record[2 * n_cols:])
        col_key = key if n_cols and not flags[0] else None
        out[((), col_key)] = data
    return out


# --------------------------------------------------------------- header + rows

def _build_header(columns, ordered_cols, measures, row_totals):
    """Leaf columns, the merged header levels above them, and each leaf's source.

    Returns (leaves, header, lookups) where `lookups[i]` is the
    (column key, measure index) pair leaf `i` reads out of the cell map. A
    column key of None means "aggregated across every column group".
    """
    leaves: List[Dict[str, Any]] = []
    lookups: List[Tuple[Optional[Tuple], int]] = []
    header: List[List[Dict[str, Any]]] = []
    multi = len(measures) > 1

    if not columns:
        for index, measure in enumerate(measures):
            leaves.append({"keys": [], "value_index": index, "label": measure.label,
                           "kind": "data", "is_count": measure.is_count})
            lookups.append((None, index))
        header.append([{"label": m.label, "span": 1, "kind": "data"} for m in measures])
        return leaves, header, lookups

    for key in ordered_cols:
        for index, measure in enumerate(measures):
            leaves.append({
                "keys": [label_of(v) for v in key],
                "value_index": index,
                "label": measure.label if multi else " / ".join(label_of(v) for v in key),
                "kind": "data",
                "is_count": measure.is_count,
            })
            lookups.append((key, index))
    if row_totals:
        for index, measure in enumerate(measures):
            leaves.append({"keys": ["Total"], "value_index": index,
                           "label": measure.label if multi else "Total",
                           "kind": "total", "is_count": measure.is_count})
            lookups.append((None, index))

    per_key = len(measures)
    for level in range(len(columns)):
        cells: List[Dict[str, Any]] = []
        previous = None
        for key in ordered_cols:
            prefix = key[: level + 1]
            if previous is not None and prefix == previous:
                cells[-1]["span"] += per_key
            else:
                cells.append({"label": label_of(key[level]), "span": per_key,
                              "kind": "data", "field": columns[level]})
                previous = prefix
        if row_totals:
            # The Total block spans every remaining header level, so it is only
            # written once -- on the first one.
            cells.append({"label": "Total", "span": per_key, "kind": "total",
                          "field": columns[level], "rowspan": len(columns) - level})
            if level > 0:
                cells[-1]["skip"] = True
        header.append(cells)

    if multi:
        header.append([{"label": leaf["label"], "span": 1, "kind": leaf["kind"], "field": None}
                       for leaf in leaves])
    return leaves, header, lookups


def _build_rows(rows, cells, row_keys, lookups, measures, subtotals, column_totals,
                sort, max_rows, grand_label="Grand Total"):
    """Order the row groups, splice in subtotals, and read each row's cells."""

    def read(row_key: Tuple) -> List[Any]:
        out = []
        for col_key, value_index in lookups:
            record = cells.get((row_key, col_key))
            out.append(None if record is None else record[value_index])
        return out

    ordered = sorted(row_keys, key=_key_sort)
    if rows and sort and sort.get("by") == "value":
        index = int(sort.get("value_index") or 0)
        if not 0 <= index < len(measures):
            index = 0

        def level_key(prefix: Tuple):
            # `(row key, None)` is the measure aggregated across every column
            # group -- the value Excel sorts a pivot field by.
            record = cells.get((prefix, None))
            if record is None:
                return (1, _sort_key(prefix[-1]))
            return (0, _sort_key(record[index]))

        ordered.sort(key=lambda k: tuple(level_key(k[: i + 1]) for i in range(len(k))),
                     reverse=bool(sort.get("descending")))

    shown = ordered[:max_rows]
    body: List[Dict[str, Any]] = []

    def emit(key: Tuple, kind: str, level: int) -> None:
        if kind == "grand":
            labels = [grand_label] + [""] * max(0, len(rows) - 1)
        elif kind == "subtotal":
            labels = [label_of(v) for v in key]
            labels[-1] = "{} Total".format(labels[-1])
            labels += [""] * (len(rows) - len(labels))
        else:
            labels = [label_of(v) for v in key]
        body.append({"labels": labels, "kind": kind, "level": level, "cells": read(key)})

    if not rows:
        emit((), "grand", 0)
        return body, 1

    previous: Optional[Tuple] = None
    for key in shown:
        if previous is not None and subtotals:
            # Close every group that just ended, innermost first.
            for k in range(len(rows) - 1, 0, -1):
                if previous[:k] != key[:k]:
                    emit(previous[:k], "subtotal", k)
        emit(key, "data", len(rows))
        previous = key
    if previous is not None and subtotals:
        for k in range(len(rows) - 1, 0, -1):
            emit(previous[:k], "subtotal", k)

    if column_totals:
        emit((), "grand", 0)

    return body, len(shown)
