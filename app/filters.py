"""Translate the UI's filter model into parameterised DuckDB SQL.

Every filter is scoped to a single column; filters from different columns are
ANDed together (Excel autofilter semantics). Values always travel as bound
parameters -- only identifiers are interpolated, and those are validated
against the file's real schema before they get here.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple

# Ops that read a list of discrete values
SET_OPS = {"in", "not_in"}
# Ops that take no operand at all
NULLARY_OPS = {"is_null", "is_not_null", "is_empty", "is_not_empty"}
# Ops that take two operands
BINARY_OPS = {"between", "not_between"}
# Everything else takes a single operand
SCALAR_OPS = {
    "eq", "ne", "gt", "gte", "lt", "lte",
    "contains", "not_contains", "starts_with", "ends_with", "regex",
}

ALL_OPS = SET_OPS | NULLARY_OPS | BINARY_OPS | SCALAR_OPS

_COMPARATORS = {"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}

# Sentinel the UI sends inside a value list to mean "also keep NULLs".
NULL_TOKEN = "__PQS_NULL__"


# Parts that can be pulled out of a date/timestamp column in place of the full
# value. Each is a DuckDB function of the same name returning an integer.
TRANSFORMS = ("year", "month", "day")
TRANSFORM_TYPE = "BIGINT"


class FilterError(ValueError):
    """A filter the user sent cannot be honoured."""


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def can_transform(sql_type: str) -> bool:
    """Only calendar values have a year, month and day (TIME and INTERVAL don't)."""
    return sql_type.upper().startswith(("DATE", "TIMESTAMP"))


def column_sql(column: str, sql_type: str, transform: Optional[str] = None) -> Tuple[str, str]:
    """(SQL expression, SQL type) for a column, with an optional part extracted.

    The one place an extraction becomes SQL, so a filter, a value list and an
    exported column all mean exactly the same thing by "year of order_date".
    """
    col = quote_ident(column)
    if not transform:
        return col, sql_type
    if transform not in TRANSFORMS:
        raise FilterError("Unsupported extraction: {!r}".format(transform))
    if not can_transform(sql_type):
        raise FilterError("Only date and timestamp columns can have their {} extracted, and {!r} is {}"
                          .format(transform, column, sql_type))
    return "{}({})".format(transform, col), TRANSFORM_TYPE


def _is_text_type(sql_type: str) -> bool:
    return sql_type.upper().startswith(("VARCHAR", "CHAR", "TEXT", "STRING", "UUID", "ENUM"))


def _castable(sql_type: str) -> bool:
    """Nested/binary types can't take a literal cast, so we compare them as text."""
    return not sql_type.upper().startswith(("STRUCT", "MAP", "LIST", "BLOB", "UNION")) and "[]" not in sql_type


def _operand(sql_type: str) -> str:
    """SQL fragment for a bound parameter compared against this column type."""
    if _is_text_type(sql_type) or not _castable(sql_type):
        return "?"
    return "CAST(? AS {})".format(sql_type)


def _text_of(col_sql: str, sql_type: str) -> str:
    """Column expression coerced to text, for substring-style predicates."""
    return col_sql if _is_text_type(sql_type) else "CAST({} AS VARCHAR)".format(col_sql)


def build_predicate(spec: Dict[str, Any], sql_type: str) -> Tuple[str, List[Any]]:
    """Build one column's predicate. Returns (sql, params)."""
    column = spec.get("column")
    op = (spec.get("op") or "in").lower()
    if op not in ALL_OPS:
        raise FilterError("Unsupported filter operator: {!r}".format(op))

    # A filter set on an extracted part ("year of order_date") carries it, so it
    # means the same thing in every view that reads it.
    col, sql_type = column_sql(column, sql_type, spec.get("transform"))
    params: List[Any] = []

    if op == "is_null":
        return "{} IS NULL".format(col), params
    if op == "is_not_null":
        return "{} IS NOT NULL".format(col), params
    if op == "is_empty":
        return "({0} IS NULL OR {1} = '')".format(col, _text_of(col, sql_type)), params
    if op == "is_not_empty":
        return "({0} IS NOT NULL AND {1} <> '')".format(col, _text_of(col, sql_type)), params

    if op in SET_OPS:
        raw = spec.get("values")
        if not isinstance(raw, (list, tuple)):
            raise FilterError("Filter on {!r} needs a list of values".format(column))
        wants_null = any(v is None or v == NULL_TOKEN for v in raw)
        concrete = [v for v in raw if v is not None and v != NULL_TOKEN]
        if not concrete and not wants_null:
            raise FilterError("Filter on {!r} has no selected values".format(column))

        # Pasted value lists ask for case-insensitive matching explicitly; the
        # value picker never does, because it offers the column's exact values.
        fold = spec.get("case_sensitive") is False and _is_text_type(sql_type)
        target = "lower({})".format(col) if fold else col

        clauses: List[str] = []
        if concrete:
            placeholder = "lower(?)" if fold else _operand(sql_type)
            placeholders = ", ".join(placeholder for _ in concrete)
            clauses.append("{} {}IN ({})".format(target, "NOT " if op == "not_in" else "", placeholders))
            params.extend(concrete)

        if op == "in":
            if wants_null:
                clauses.append("{} IS NULL".format(col))
            elif concrete:
                # NULL is never "in" a set, but be explicit for readability.
                pass
            return "(" + " OR ".join(clauses) + ")", params

        # not_in: NULLs survive an exclusion filter unless NULL is excluded too.
        if wants_null:
            clauses.append("{} IS NOT NULL".format(col))
            return "(" + " AND ".join(clauses) + ")", params
        return "({} OR {} IS NULL)".format(clauses[0], col) if clauses else "TRUE", params

    if op in BINARY_OPS:
        lo, hi = spec.get("value"), spec.get("value2")
        parts: List[str] = []
        if lo is not None and lo != "":
            parts.append("{} >= {}".format(col, _operand(sql_type)))
            params.append(lo)
        if hi is not None and hi != "":
            parts.append("{} <= {}".format(col, _operand(sql_type)))
            params.append(hi)
        if not parts:
            raise FilterError("Range filter on {!r} needs a bound".format(column))
        clause = "(" + " AND ".join(parts) + ")"
        return ("NOT " + clause + " AND {} IS NOT NULL".format(col)) if op == "not_between" else clause, params

    # Single-operand ops
    value = spec.get("value")
    if value is None or value == "":
        raise FilterError("Filter on {!r} needs a value".format(column))

    if op in _COMPARATORS:
        return "{} {} {}".format(col, _COMPARATORS[op], _operand(sql_type)), [value]

    text = _text_of(col, sql_type)
    case_sensitive = bool(spec.get("case_sensitive"))
    like = "LIKE" if case_sensitive else "ILIKE"
    escaped = str(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    if op == "contains":
        return "{} {} ? ESCAPE '\\'".format(text, like), ["%{}%".format(escaped)]
    if op == "not_contains":
        return "({0} NOT {1} ? ESCAPE '\\' OR {0} IS NULL)".format(text, like), ["%{}%".format(escaped)]
    if op == "starts_with":
        return "{} {} ? ESCAPE '\\'".format(text, like), ["{}%".format(escaped)]
    if op == "ends_with":
        return "{} {} ? ESCAPE '\\'".format(text, like), ["%{}".format(escaped)]
    if op == "regex":
        return "regexp_matches({}, ?)".format(text), [str(value)]

    raise FilterError("Unsupported filter operator: {!r}".format(op))


def build_where(
    specs: Sequence[Dict[str, Any]],
    column_types: Dict[str, str],
    skip_column: str = None,
) -> Tuple[str, List[Any]]:
    """Combine every filter into a WHERE clause.

    `skip_column` omits one column's own filter -- that is what makes the value
    list in a filter panel behave like Excel's, showing the options that are
    still reachable given the *other* active filters.
    """
    clauses: List[str] = []
    params: List[Any] = []
    for spec in specs or []:
        column = spec.get("column")
        if column not in column_types:
            raise FilterError("Unknown column: {!r}".format(column))
        if skip_column is not None and column == skip_column:
            continue
        if spec.get("enabled") is False:
            continue
        sql, sql_params = build_predicate(spec, column_types[column])
        clauses.append(sql)
        params.extend(sql_params)

    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(clauses), params


def describe(spec: Dict[str, Any]) -> str:
    """Human-readable summary, used for the export manifest sheet."""
    op = (spec.get("op") or "in").lower()
    col = spec.get("column")
    if spec.get("transform"):
        col = "{} ({})".format(col, spec["transform"])
    if op in NULLARY_OPS:
        return "{} {}".format(col, op.replace("_", " "))
    if op in SET_OPS:
        values = [("(blank)" if v is None or v == NULL_TOKEN else str(v)) for v in spec.get("values") or []]
        shown = ", ".join(values[:12]) + (" ... (+{} more)".format(len(values) - 12) if len(values) > 12 else "")
        return "{} {} [{}]".format(col, "is any of" if op == "in" else "is none of", shown)
    if op in BINARY_OPS:
        return "{} {} {} .. {}".format(col, op.replace("_", " "), spec.get("value"), spec.get("value2"))
    return "{} {} {}".format(col, op.replace("_", " "), spec.get("value"))
