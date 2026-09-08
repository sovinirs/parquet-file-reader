"""Do the rows sharing a key actually agree with each other?

The question this answers comes up before any de-duplication: a fixed-asset
extract holds one row per (asset, depreciation area), but the business wants one
row per asset. Collapsing is only safe where the other seventy-odd columns say
the same thing across an asset's rows -- and nobody knows, column by column,
whether they do.

So for every column we ask, per asset: how many *distinct non-blank* values are
there? That single number separates the four cases that matter. None means the
asset carries nothing there. One, with no blanks, means the rows agree and any of
them can be kept. One, with some rows blank, means the rows complement rather
than contradict each other -- coalesce them. More than one is a real conflict,
and the only interesting question left is whether the depreciation area explains
it: if each (asset, area) pair is internally consistent, the column is simply
area-specific and collapsing needs a rule for which area wins. If values still
disagree *inside* one area, no amount of grain reasoning explains it and a human
has to look.

Two things shape the implementation. Parquet is columnar, so each column gets its
own query touching only that column and the key -- unpivoting seventy columns
would turn 100M rows into 7B. And blankness has to mean one thing across typed
columns, so every value goes through `NULLIF(TRIM(CAST(x AS VARCHAR)), '')`:
NULL, empty string and whitespace are one concept, not three.

Nothing here imports FastAPI or touches a job queue. `prepare()` sizes the run,
`analyse_column()` does one column, `examples()` fetches evidence on demand; the
caller decides how to thread them and when to stop.
"""

import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .engine import Dataset, Engine, categorise, to_jsonable
from .filters import FilterError, build_where, quote_ident

# One verdict per column. The order is the order a reviewer should read them in:
# everything above TRUE_DIFF_OTHER is mechanical, TRUE_DIFF_OTHER is the pile
# that needs a person.
ALL_BLANK = "ALL_BLANK"
CONSTANT = "CONSTANT"
SPARSE_SINGLE_VALUE = "SPARSE_SINGLE_VALUE"
TRUE_DIFF_BY_DEPR_AREA = "TRUE_DIFF_BY_DEPR_AREA"
TRUE_DIFF_OTHER = "TRUE_DIFF_OTHER"
ERROR = "ERROR"

VERDICTS = (ALL_BLANK, CONSTANT, SPARSE_SINGLE_VALUE, TRUE_DIFF_BY_DEPR_AREA,
            TRUE_DIFF_OTHER, ERROR)

# What to do with the column when the extract is collapsed to one row per asset.
RECOMMENDATIONS: Dict[str, str] = {
    ALL_BLANK: "Nothing to keep — the column is blank in every row. Drop it.",
    CONSTANT: "Safe to take any row — every row of an asset already agrees.",
    SPARSE_SINGLE_VALUE: "Coalesce with MAX/ANY_VALUE — the rows complement each "
                         "other, so the one non-blank value survives.",
    TRUE_DIFF_BY_DEPR_AREA: "Requires a business rule for which depreciation area "
                            "wins — values differ, but never inside one area.",
    TRUE_DIFF_OTHER: "Requires a business rule — variation is not explained by "
                     "depreciation area, so pick the rule after reviewing examples.",
    ERROR: "Could not be analysed — see the error and decide by hand.",
}

# Columns the business already knows to leave out of a fixed-asset run: run
# metadata, and the period-dependent amounts that are *expected* to differ per
# depreciation area. Offered as the starting selection, not imposed.
DEFAULT_EXCLUDED_COLUMNS = (
    "Posting Period",
    "PULLDATE",
    "ACID_ACCUM_DEPR_AMT",
    "ACTD_NET_BOOK_VALUE_AMT",
    "ASSET_UFE_MONTH_QTY",
    "FIXED_ASSET_QTY",
)

# Example assets fetched per drill-down page.
DEFAULT_EXAMPLE_ASSETS = 5
MAX_EXAMPLE_ASSETS = 50


class DiffError(FilterError):
    """The analysis as configured cannot be run."""


@dataclass
class DiffConfig:
    """Everything a run needs to know, and nothing about how it is driven.

    `key_column` is a selection rather than a constant: asset ID alone defines
    uniqueness in this extract, but the next pull may well label it something
    else, and the analysis does not care what it is called.
    """

    key_column: str
    explain_column: Optional[str] = None
    exclude: Sequence[str] = ()
    sample_assets: Optional[int] = None
    filters: Sequence[Dict[str, Any]] = ()
    exclude_all_blank: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return {
            "key_column": self.key_column,
            "explain_column": self.explain_column,
            "exclude": list(self.exclude),
            "sample_assets": self.sample_assets,
            "filters": list(self.filters),
            "exclude_all_blank": self.exclude_all_blank,
        }


@dataclass
class ColumnResult:
    """One column's verdict and the counts behind it."""

    column: str
    verdict: str
    assets: int = 0
    constant: int = 0
    sparse: int = 0
    differing: int = 0
    blank: int = 0
    max_distinct: int = 0
    # Only filled in for columns that reached the attribution pass.
    conflicting_assets: Optional[int] = None
    error: Optional[str] = None

    def _pct(self, n: int) -> Optional[float]:
        return round(100.0 * n / self.assets, 2) if self.assets else None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "verdict": self.verdict,
            "recommendation": RECOMMENDATIONS.get(self.verdict, ""),
            "assets": self.assets,
            "constant": self.constant,
            "sparse": self.sparse,
            "differing": self.differing,
            "blank": self.blank,
            "constant_pct": self._pct(self.constant),
            "sparse_pct": self._pct(self.sparse),
            "differing_pct": self._pct(self.differing),
            "blank_pct": self._pct(self.blank),
            "max_distinct": self.max_distinct,
            "conflicting_assets": self.conflicting_assets,
            "error": self.error,
        }


@dataclass
class RunPlan:
    """What `prepare()` worked out before the per-column pass starts.

    Holds the name of the sampled-key table when one was built, which is why it
    has to be released: it is a real table in the shared in-memory database, not
    a per-connection temporary one, because each column's query runs on its own
    cursor.
    """

    columns: List[str]
    excluded: List[Dict[str, str]]
    all_blank: List[str]
    duplicate_grain: Dict[str, Any]
    assets: int
    rows: int
    sample_table: Optional[str] = None

    def release(self, engine: Engine) -> None:
        if self.sample_table:
            engine.cursor().execute('DROP TABLE IF EXISTS "{}"'.format(self.sample_table))
            self.sample_table = None


# ------------------------------------------------------------------ SQL pieces


def _blank_normalised(column: str, alias: str = "t") -> str:
    """The one definition of "blank" this module uses.

    Casting to VARCHAR first is what makes it uniform: a NULL number, an empty
    string and a column of spaces all collapse to NULL, so `count()` and
    `count(DISTINCT)` on the result mean "non-blank" for every column type. The
    alias is always spelled out -- a file is free to contain a column called `k`,
    which is also the sampled-key table's column name.
    """
    return "NULLIF(TRIM(CAST({}.{} AS VARCHAR)), '')".format(alias, quote_ident(column))


def _scan(engine: Engine, dataset: Dataset, config: DiffConfig,
          plan: Optional[RunPlan] = None) -> Tuple[str, List[Any]]:
    """`FROM ... WHERE ...` for the rows a run covers, and its bound params.

    The filter clause comes from filters.py rather than a parallel implementation
    so that "analyse what I am looking at" means exactly what the grid shows.
    """
    where, params = build_where(config.filters, dataset.column_types)
    # engine._from() rather than another copy of the literal: the source
    # expression stays identical to every other query in the app.
    source = "FROM {} AS t".format(engine._from())
    if plan is not None and plan.sample_table:
        source += ' SEMI JOIN "{}" AS s ON t.{} IS NOT DISTINCT FROM s.k'.format(
            plan.sample_table, quote_ident(config.key_column))
    return "{} {}".format(source, where).strip(), [dataset.path] + params


def _and(scan: str, predicate: str) -> str:
    """Add one more predicate to a scan that may or may not already have a WHERE."""
    return "{} {} {}".format(scan, "AND" if " WHERE " in scan + " " else "WHERE", predicate)


def _validate(dataset: Dataset, config: DiffConfig) -> None:
    types = dataset.column_types
    if config.key_column not in types:
        raise DiffError("Unknown key column: {!r}".format(config.key_column))
    if categorise(types[config.key_column]) == "complex":
        raise DiffError(
            "{!r} is a nested column ({}) -- it cannot identify a row.".format(
                config.key_column, types[config.key_column]))
    if config.explain_column is not None:
        if config.explain_column not in types:
            raise DiffError("Unknown explanatory column: {!r}".format(config.explain_column))
        if categorise(types[config.explain_column]) == "complex":
            raise DiffError(
                "{!r} is a nested column ({}) -- it cannot explain a difference.".format(
                    config.explain_column, types[config.explain_column]))
        if config.explain_column == config.key_column:
            raise DiffError(
                "The key and explanatory columns must be different -- "
                "{!r} cannot explain variation within itself.".format(config.key_column))
    if config.sample_assets is not None and int(config.sample_assets) < 1:
        raise DiffError("The sample size must be at least one asset.")


# --------------------------------------------------------------- run preparation


def _materialise_sample(engine: Engine, dataset: Dataset, config: DiffConfig) -> str:
    """Pick N asset IDs at random and keep them in a table for the whole run.

    The sample has to be of *assets*, not of rows: comparing an asset's rows to
    each other only means something when all of them are present. Every column
    query then semi-joins to this table, so the distinct-key pass happens once
    rather than seventy times.
    """
    scan, params = _scan(engine, dataset, config)
    table = "diff_sample_{}".format(uuid.uuid4().hex[:12])
    key = quote_ident(config.key_column)
    cur = engine.cursor()
    cur.execute(
        'CREATE TABLE "{table}" AS SELECT k FROM '
        "(SELECT t.{key} AS k {scan} GROUP BY 1) USING SAMPLE {n} ROWS".format(
            table=table, key=key, scan=scan, n=int(config.sample_assets)),
        params,
    )
    return table


def _all_blank_columns(engine: Engine, dataset: Dataset, config: DiffConfig,
                       plan: RunPlan, candidates: Sequence[str]) -> List[str]:
    """Which of these columns hold nothing at all, in one pass and no grouping.

    Worth doing first: an empty column cannot differ, so answering this here
    spares the expensive per-asset query for every column it catches -- and on a
    70-column SAP extract it catches a lot of them.
    """
    if not candidates:
        return []
    scan, params = _scan(engine, dataset, config, plan)
    aggregates = ", ".join(
        "count({})".format(_blank_normalised(name)) for name in candidates)
    cur = engine.cursor()
    row = cur.execute("SELECT {} {}".format(aggregates, scan), params).fetchone()
    return [name for name, non_blank in zip(candidates, row) if not non_blank]


def _duplicate_grain(engine: Engine, dataset: Dataset, config: DiffConfig,
                     plan: RunPlan) -> Dict[str, Any]:
    """Does (key, explanatory column) actually identify a row?

    Asked up front and reported on its own, because the answer changes how the
    rest of the run reads: if a pair repeats, the grain is not what the business
    believes, and an unexplained difference may be nothing more than that. Fold
    it into the per-column numbers and nobody would ever find out.
    """
    if not config.explain_column:
        return {"checked": False}
    scan, params = _scan(engine, dataset, config, plan)
    sql = (
        "SELECT count(*) AS pairs, coalesce(sum(n), 0) AS rows_in_pairs, "
        "coalesce(max(n), 0) AS worst FROM (SELECT t.{key} AS k, t.{area} AS a, "
        "count(*) AS n {scan} GROUP BY 1, 2 HAVING count(*) > 1)"
    ).format(key=quote_ident(config.key_column),
             area=quote_ident(config.explain_column), scan=scan)
    pairs, rows_in_pairs, worst = engine.cursor().execute(sql, params).fetchone()
    return {
        "checked": True,
        "column": config.explain_column,
        "duplicate_pairs": int(pairs),
        "extra_rows": int(rows_in_pairs) - int(pairs),
        "max_rows_per_pair": int(worst),
        "unique": not pairs,
    }


def prepare(engine: Engine, dataset: Dataset, config: DiffConfig) -> RunPlan:
    """Size the run: which columns to analyse, and what is already known.

    Everything here is cheap relative to the per-column pass, and all of it is
    needed before the first column is touched -- including the sample, which the
    per-column queries join to.
    """
    _validate(dataset, config)
    excluded: List[Dict[str, str]] = []
    requested = {name for name in config.exclude if name in dataset.column_types}
    candidates: List[str] = []
    for column in dataset.columns:
        name = column.name
        if name == config.key_column:
            excluded.append({"column": name, "reason": "Grouping key"})
        elif name in requested:
            excluded.append({"column": name, "reason": "Excluded by the run configuration"})
        else:
            candidates.append(name)

    plan = RunPlan(columns=[], excluded=excluded, all_blank=[],
                   duplicate_grain={"checked": False}, assets=0, rows=0)
    if config.sample_assets:
        plan.sample_table = _materialise_sample(engine, dataset, config)

    try:
        blank = set(_all_blank_columns(engine, dataset, config, plan, candidates))
        plan.all_blank = [name for name in candidates if name in blank]
        if config.exclude_all_blank:
            for name in plan.all_blank:
                excluded.append({"column": name, "reason": "Blank in every row"})
            candidates = [name for name in candidates if name not in blank]
        plan.columns = candidates

        scan, params = _scan(engine, dataset, config, plan)
        rows, assets = engine.cursor().execute(
            "SELECT count(*), count(DISTINCT t.{key}) {scan}".format(
                key=quote_ident(config.key_column), scan=scan), params).fetchone()
        plan.rows, plan.assets = int(rows), int(assets)
        plan.duplicate_grain = _duplicate_grain(engine, dataset, config, plan)
    except Exception:
        plan.release(engine)
        raise
    return plan


# ------------------------------------------------------------ the per-column pass


def _attribute(engine: Engine, dataset: Dataset, config: DiffConfig, plan: RunPlan,
               column: str) -> Optional[int]:
    """How many assets still disagree once the depreciation area is held fixed?

    Zero means the column is simply area-specific. Anything else is variation the
    grain does not account for, which is the set worth a human's time.
    """
    if not config.explain_column:
        return None
    scan, params = _scan(engine, dataset, config, plan)
    sql = (
        "SELECT count(DISTINCT k) FROM (SELECT t.{key} AS k, t.{area} AS a, "
        "count(DISTINCT {value}) AS nd {scan} GROUP BY 1, 2) WHERE nd > 1"
    ).format(key=quote_ident(config.key_column),
             area=quote_ident(config.explain_column),
             value=_blank_normalised(column), scan=scan)
    return int(engine.cursor().execute(sql, params).fetchone()[0])


def analyse_column(engine: Engine, dataset: Dataset, config: DiffConfig,
                   plan: RunPlan, column: str) -> ColumnResult:
    """Classify one column. Never raises -- a bad column is a result, not a stop.

    Seventy columns of SAP data will contain at least one that DuckDB refuses to
    cast or compare; losing the whole run to it would be absurd, so the failure
    is recorded against the column and the caller carries on.
    """
    if column in plan.all_blank:
        return ColumnResult(column=column, verdict=ALL_BLANK, assets=plan.assets,
                            blank=plan.assets)
    try:
        scan, params = _scan(engine, dataset, config, plan)
        # Per asset: how many rows, how many non-blank, how many distinct
        # non-blank. Those three numbers place the asset in exactly one bucket.
        sql = (
            "SELECT count(*) AS assets, "
            "count(*) FILTER (WHERE nd = 1 AND nb = rows_n) AS constant_n, "
            "count(*) FILTER (WHERE nd = 1 AND nb < rows_n) AS sparse_n, "
            "count(*) FILTER (WHERE nd > 1) AS differing_n, "
            "count(*) FILTER (WHERE nd = 0) AS blank_n, "
            "coalesce(max(nd), 0) AS max_nd FROM ("
            "SELECT t.{key} AS k, count(*) AS rows_n, count({value}) AS nb, "
            "count(DISTINCT {value}) AS nd {scan} GROUP BY 1)"
        ).format(key=quote_ident(config.key_column),
                 value=_blank_normalised(column), scan=scan)
        assets, constant, sparse, differing, blank, max_nd = (
            engine.cursor().execute(sql, params).fetchone())

        result = ColumnResult(
            column=column, verdict=CONSTANT, assets=int(assets), constant=int(constant),
            sparse=int(sparse), differing=int(differing), blank=int(blank),
            max_distinct=int(max_nd))

        if not assets or not (constant or sparse or differing):
            # Nothing but blank assets: the upfront pass will normally have
            # caught this, but a filter can empty a column it did not empty.
            result.verdict = ALL_BLANK
        elif differing:
            conflicting = _attribute(engine, dataset, config, plan, column)
            result.conflicting_assets = conflicting
            result.verdict = (TRUE_DIFF_OTHER if conflicting
                              else TRUE_DIFF_BY_DEPR_AREA)
            if conflicting is None:
                # With no explanatory column there is nothing to attribute the
                # difference to, so it stays unexplained by definition.
                result.verdict = TRUE_DIFF_OTHER
        elif sparse:
            # Blanks alongside a single value complement each other; assets that
            # are wholly blank contradict nothing, so they do not count against
            # a column being constant.
            result.verdict = SPARSE_SINGLE_VALUE
        return result
    except Exception as exc:  # noqa: BLE001 - reported against the column
        return ColumnResult(column=column, verdict=ERROR, assets=plan.assets,
                            error="{}: {}".format(type(exc).__name__, exc))


def summarise(dataset: Dataset, config: DiffConfig, plan: RunPlan,
              results: Sequence[ColumnResult]) -> Dict[str, Any]:
    """The whole run as one JSON-ready structure."""
    by_verdict: Dict[str, int] = {verdict: 0 for verdict in VERDICTS}
    for result in results:
        by_verdict[result.verdict] = by_verdict.get(result.verdict, 0) + 1
    return {
        "config": config.as_dict(),
        "dataset": {"name": dataset.display_name, "path": dataset.path,
                    "row_count": dataset.row_count},
        "rows_analysed": plan.rows,
        "assets_analysed": plan.assets,
        "sampled": bool(plan.sample_table) or bool(config.sample_assets),
        "columns_analysed": len(results),
        "columns_excluded": len(plan.excluded),
        "excluded": plan.excluded,
        "all_blank_columns": plan.all_blank,
        "duplicate_grain": plan.duplicate_grain,
        "verdict_counts": by_verdict,
        "columns": [result.as_dict() for result in results],
    }


def analyse(engine: Engine, dataset: Dataset, config: DiffConfig,
            on_column=None, should_cancel=None) -> Dict[str, Any]:
    """Run every column and return the summary.

    `on_column` is called with each finished `ColumnResult` and `should_cancel`
    is polled between columns, which is all a background job needs -- the module
    itself stays free of any notion of one.
    """
    plan = prepare(engine, dataset, config)
    results: List[ColumnResult] = []
    try:
        for column in plan.columns:
            if should_cancel is not None and should_cancel():
                break
            result = analyse_column(engine, dataset, config, plan, column)
            results.append(result)
            if on_column is not None:
                on_column(result)
    finally:
        plan.release(engine)
    return summarise(dataset, config, plan, results)


# ------------------------------------------------------------------- drill-down


def examples(engine: Engine, dataset: Dataset, config: DiffConfig, column: str,
             limit: int = DEFAULT_EXAMPLE_ASSETS, offset: int = 0,
             asset_id: Optional[str] = None) -> Dict[str, Any]:
    """Real assets whose rows disagree on `column`, with every one of their rows.

    Fetched on demand rather than during the run: a reviewer opens two or three
    columns out of seventy, and gathering evidence for the other sixty-seven
    would cost more than the analysis itself.

    Searched across the whole file even when the run was sampled. The sample is
    there to make the percentages quick, but evidence is evidence -- an asset
    that disagrees is worth showing whether or not it happened to be sampled,
    and by this point the sampled-key table is long released.
    """
    _validate(dataset, config)
    if column not in dataset.column_types:
        raise DiffError("Unknown column: {!r}".format(column))
    limit = max(1, min(int(limit), MAX_EXAMPLE_ASSETS))
    offset = max(0, int(offset))

    key = quote_ident(config.key_column)
    value = _blank_normalised(column)
    scan, params = _scan(engine, dataset, config)

    # The assets to show: either the one asked for, or the next page of those
    # that actually disagree. Ordered so paging is stable.
    if asset_id is not None:
        picker = "SELECT t.{key} AS k {scan} GROUP BY 1".format(
            key=key, scan=_and(scan, "CAST(t.{} AS VARCHAR) = ?".format(key)))
        picker_params = params + [str(asset_id)]
    else:
        picker = ("SELECT t.{key} AS k {scan} GROUP BY 1 HAVING count(DISTINCT {value}) > 1 "
                  "ORDER BY 1 LIMIT {lim} OFFSET {off}").format(
                      key=key, scan=scan, value=value, lim=limit + 1, off=offset)
        picker_params = params

    cur = engine.cursor()
    keys = [row[0] for row in cur.execute(picker, picker_params).fetchall()]
    has_more = len(keys) > limit
    keys = keys[:limit]
    if not keys:
        return {"column": column, "assets": [], "has_more": False, "offset": offset}

    # Then every row of the chosen assets. A handful of bound keys in an `IN`
    # keeps this one scan, with no table built for five values.
    area = "t.{}".format(quote_ident(config.explain_column)) if config.explain_column else "NULL"
    placeholders = ", ".join("?" for _ in keys)
    detail = (
        "SELECT t.{key} AS k, {area} AS a, t.{col} AS raw, {value} AS v {scan} "
        "ORDER BY 1, 2 NULLS LAST"
    ).format(key=key, area=area, col=quote_ident(column), value=value,
             scan=_and(scan, "CAST(t.{} AS VARCHAR) IN ({})".format(key, placeholders)))
    rows = cur.execute(detail, params + [str(k) for k in keys]).fetchall()

    grouped: Dict[Any, Dict[str, Any]] = {}
    for k, a, raw, v in rows:
        asset = grouped.setdefault(str(k), {"asset": to_jsonable(k), "rows": [],
                                            "distinct_values": []})
        asset["rows"].append({"area": to_jsonable(a), "value": to_jsonable(raw),
                              "blank": v is None})
        if v is not None and v not in asset["distinct_values"]:
            asset["distinct_values"].append(v)
    for asset in grouped.values():
        # What the UI highlights: a cell is only worth marking when its asset
        # holds more than one value to disagree about.
        asset["differs"] = len(asset["distinct_values"]) > 1
    return {
        "column": column,
        "key_column": config.key_column,
        "explain_column": config.explain_column,
        "assets": [grouped[str(k)] for k in keys if str(k) in grouped],
        "has_more": has_more,
        "offset": offset,
    }
