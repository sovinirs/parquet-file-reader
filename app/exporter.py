"""Background export jobs.

Excel is written in xlsxwriter's constant-memory mode and fed straight from a
streaming DuckDB cursor, so peak memory stays flat regardless of how many rows
come out the other side. CSV/Parquet exports skip Python entirely and use
DuckDB's own parallel writer.
"""

import csv
import datetime as _dt
import decimal
import os
import shutil
import tempfile
import threading
import time
import traceback
import uuid
import zipfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import xlsxwriter

from . import diffanalysis
from . import pivot as pivot_module
from .engine import Engine, categorise
from .filters import describe

# Excel's hard ceiling is 1,048,576 rows per sheet; one goes to the header.
EXCEL_MAX_DATA_ROWS = 1_048_575
EXCEL_MAX_CELL_CHARS = 32_767
# Excel cannot represent dates before 1900.
EXCEL_MIN_DATE = _dt.date(1900, 1, 1)
# The summary sheet, in the order a reviewer reads it: what the column is, what
# the verdict is, then the evidence, then what to do about it.
DIFF_SUMMARY_FIELDS = (
    "column", "verdict", "assets", "constant", "constant_pct", "sparse", "sparse_pct",
    "differing", "differing_pct", "blank", "blank_pct", "max_distinct",
    "conflicting_assets", "error", "recommendation",
)
DIFF_SUMMARY_HEADERS = (
    "Column", "Verdict", "Assets", "Constant", "Constant %", "Sparse", "Sparse %",
    "Differing", "Differing %", "Blank", "Blank %", "Max distinct",
    "Assets conflicting within an area", "Error", "Recommendation",
)
# Verdict -> the key of the format that colours it.
DIFF_VERDICT_FORMATS = {
    diffanalysis.CONSTANT: "v_constant",
    diffanalysis.SPARSE_SINGLE_VALUE: "v_sparse",
    diffanalysis.TRUE_DIFF_BY_DEPR_AREA: "v_explained",
    diffanalysis.TRUE_DIFF_OTHER: "v_unexplained",
    diffanalysis.ALL_BLANK: "v_blank",
    diffanalysis.ERROR: "v_error",
}
# Evidence is a sample, not a dump: enough to check a verdict by eye. Ten assets
# is what the screen shows for the column a reviewer opened, so the workbook
# carries the same depth for every differing column rather than a thinner one --
# the export is the artefact that leaves the tool, and being asked "can you send
# me a few more rows for this column" is the failure it exists to prevent.
DIFF_EXPORT_EXAMPLE_COLUMNS = 25
DIFF_EXPORT_EXAMPLE_ASSETS = diffanalysis.DEFAULT_EXAMPLE_ASSETS

# Row groups per exported workbook. A pivot bigger than this is split across
# several files, so this is a file-size choice, not a ceiling on the export --
# and it is kept in step with what the pivot can hold at once, so that each file
# is built whole rather than windowed.
PIVOT_EXPORT_MAX_ROWS = pivot_module.HARD_MAX_ROW_KEYS


@dataclass
class ExportJob:
    id: str
    dataset_id: str
    fmt: str
    filename: str
    kind: str = "rows"           # rows | pivot
    status: str = "queued"       # queued | counting | running | done | error | cancelled
    total: Optional[int] = None  # None while unknown
    written: int = 0
    sheets: int = 0
    path: Optional[str] = None
    size: int = 0
    error: Optional[str] = None
    message: str = ""
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    _cancel: threading.Event = field(default_factory=threading.Event, repr=False)

    def as_dict(self) -> Dict[str, Any]:
        elapsed = (self.finished_at or time.time()) - self.created_at
        pct = None
        if self.total:
            pct = min(100.0, round(100.0 * self.written / max(1, self.total), 1))
        elif self.status == "done":
            pct = 100.0
        return {
            "id": self.id,
            "status": self.status,
            "kind": self.kind,
            "format": self.fmt,
            "filename": self.filename,
            "total": self.total,
            "written": self.written,
            "sheets": self.sheets,
            "percent": pct,
            "size": self.size,
            "error": self.error,
            "message": self.message,
            "elapsed": round(elapsed, 1),
            "rows_per_sec": int(self.written / elapsed) if elapsed > 0.5 and self.written else None,
        }


def _excel_value(value: Any) -> Any:
    """Coerce a DuckDB value into something xlsxwriter can write natively."""
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, str) and len(value) > EXCEL_MAX_CELL_CHARS:
            return value[: EXCEL_MAX_CELL_CHARS - 1] + "…"
        if isinstance(value, float) and (value != value or abs(value) == float("inf")):
            return str(value)
        return value
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None:
            value = value.replace(tzinfo=None)  # Excel has no tz-aware type
        return value if value.date() >= EXCEL_MIN_DATE else value.isoformat()
    if isinstance(value, _dt.date):
        return value if value >= EXCEL_MIN_DATE else value.isoformat()
    if isinstance(value, _dt.time):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "0x" + bytes(value)[:512].hex()
    text = str(value)
    return text[: EXCEL_MAX_CELL_CHARS - 1] + "…" if len(text) > EXCEL_MAX_CELL_CHARS else text


class ExportManager:
    def __init__(self, engine: Engine, output_dir: Optional[str] = None):
        self.engine = engine
        self.output_dir = output_dir or os.path.join(tempfile.gettempdir(), "parquet-reader-exports")
        os.makedirs(self.output_dir, exist_ok=True)
        self.jobs: Dict[str, ExportJob] = {}
        self._lock = threading.Lock()

    def get(self, job_id: str) -> ExportJob:
        job = self.jobs.get(job_id)
        if job is None:
            raise KeyError("Unknown export job")
        return job

    def cancel(self, job_id: str) -> ExportJob:
        job = self.get(job_id)
        job._cancel.set()
        if job.status in ("queued", "counting", "running"):
            job.message = "Cancelling…"
        return job

    def cleanup(self, max_age_seconds: int = 6 * 3600) -> None:
        now = time.time()
        for job_id, job in list(self.jobs.items()):
            if job.finished_at and now - job.finished_at > max_age_seconds:
                if job.path and os.path.exists(job.path):
                    try:
                        os.remove(job.path)
                    except OSError:
                        pass
                self.jobs.pop(job_id, None)

    def start(
        self,
        dataset_id: str,
        fmt: str,
        filters: Sequence[Dict[str, Any]],
        columns: Optional[Sequence[str]] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
        row_limit: Optional[int] = None,
        include_manifest: bool = True,
        sheet_name: str = "Data",
        total_hint: Optional[int] = None,
    ) -> ExportJob:
        dataset = self.engine.get(dataset_id)
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        base = os.path.splitext(dataset.display_name)[0][:60] or "export"
        ext = {"xlsx": "xlsx", "csv": "csv", "parquet": "parquet", "json": "json"}[fmt]
        filename = "{}-filtered-{}.{}".format(base, stamp, ext)

        job = ExportJob(
            id=uuid.uuid4().hex[:12],
            dataset_id=dataset_id,
            fmt=fmt,
            filename=filename,
            path=os.path.join(self.output_dir, "{}-{}".format(uuid.uuid4().hex[:8], filename)),
        )
        if total_hint is not None:
            job.total = int(total_hint)
        with self._lock:
            self.jobs[job.id] = job

        args = (job, dataset, filters, list(columns or []), order_by, descending,
                row_limit, include_manifest, sheet_name)
        threading.Thread(target=self._run, args=args, daemon=True).start()
        self.cleanup()
        return job

    def start_diff(
        self,
        dataset_id: str,
        summary: Dict[str, Any],
        config: Any,
        fmt: str = "xlsx",
        include_manifest: bool = True,
        sheet_name: str = "Difference analysis",
    ) -> ExportJob:
        """Queue an export of a finished difference analysis.

        The analysis itself is already done -- this only lays it out -- but it
        still goes through the job machinery, because the examples sheet goes
        back to the file for evidence and that is not something to do inside a
        request.
        """
        dataset = self.engine.get(dataset_id)
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        base = os.path.splitext(dataset.display_name)[0][:60] or "export"
        filename = "{}-diff-{}.{}".format(base, stamp, fmt)

        job = ExportJob(
            id=uuid.uuid4().hex[:12],
            dataset_id=dataset_id,
            fmt=fmt,
            filename=filename,
            kind="diff",
            path=os.path.join(self.output_dir, "{}-{}".format(uuid.uuid4().hex[:8], filename)),
        )
        with self._lock:
            self.jobs[job.id] = job

        threading.Thread(
            target=self._run_diff,
            args=(job, dataset, summary, config, fmt, include_manifest,
                  sheet_name or "Difference analysis"),
            daemon=True,
        ).start()
        self.cleanup()
        return job

    def start_pivot(
        self,
        dataset_id: str,
        spec: Dict[str, Any],
        sheet_name: str = "Pivot",
        include_manifest: bool = True,
        max_rows: int = PIVOT_EXPORT_MAX_ROWS,
    ) -> ExportJob:
        """Queue an Excel export of a pivot table built from `spec`."""
        dataset = self.engine.get(dataset_id)
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        base = os.path.splitext(dataset.display_name)[0][:60] or "export"
        filename = "{}-pivot-{}.xlsx".format(base, stamp)

        job = ExportJob(
            id=uuid.uuid4().hex[:12],
            dataset_id=dataset_id,
            fmt="xlsx",
            filename=filename,
            kind="pivot",
            path=os.path.join(self.output_dir, "{}-{}".format(uuid.uuid4().hex[:8], filename)),
        )
        with self._lock:
            self.jobs[job.id] = job

        threading.Thread(
            target=self._run_pivot,
            args=(job, dataset, spec, sheet_name or "Pivot", include_manifest, max_rows),
            daemon=True,
        ).start()
        self.cleanup()
        return job

    # ------------------------------------------------------------------ worker

    def _run(self, job, dataset, filters, columns, order_by, descending,
             row_limit, include_manifest, sheet_name):
        try:
            if job.total is None:
                job.status = "counting"
                job.message = "Counting matching rows…"
                job.total = self.engine.count(dataset, filters)
            if row_limit:
                job.total = min(job.total, int(row_limit))

            if job._cancel.is_set():
                raise _Cancelled()

            job.status = "running"
            job.message = "Writing {}…".format(job.fmt.upper())

            if job.fmt == "xlsx":
                self._write_xlsx(job, dataset, filters, columns, order_by, descending,
                                 row_limit, include_manifest, sheet_name)
            else:
                self.engine.copy_to(dataset, job.path, job.fmt, filters, columns, order_by, descending)
                job.written = job.total or 0

            if job._cancel.is_set():
                raise _Cancelled()

            job.size = os.path.getsize(job.path) if os.path.exists(job.path) else 0
            job.status = "done"
            job.message = "Ready to download"
        except _Cancelled:
            job.status = "cancelled"
            job.message = "Export cancelled"
            self._discard(job)
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            job.status = "error"
            job.error = "{}: {}".format(type(exc).__name__, exc)
            job.message = "Export failed"
            traceback.print_exc()
            self._discard(job)
        finally:
            job.finished_at = time.time()

    @staticmethod
    def _discard(job: ExportJob) -> None:
        if job.path and os.path.exists(job.path):
            try:
                os.remove(job.path)
            except OSError:
                pass

    def _write_xlsx(self, job, dataset, filters, columns, order_by, descending,
                    row_limit, include_manifest, sheet_name):
        book = xlsxwriter.Workbook(
            job.path,
            {"constant_memory": True, "default_date_format": "yyyy-mm-dd hh:mm:ss", "remove_timezone": True},
        )
        try:
            header_fmt = book.add_format(
                {"bold": True, "bg_color": "#1F2937", "font_color": "#FFFFFF",
                 "border": 1, "border_color": "#374151", "valign": "vcenter"}
            )
            date_fmt = book.add_format({"num_format": "yyyy-mm-dd"})
            datetime_fmt = book.add_format({"num_format": "yyyy-mm-dd hh:mm:ss"})
            time_fmt = book.add_format({"num_format": "hh:mm:ss"})
            title_fmt = book.add_format({"bold": True, "font_size": 13})
            label_fmt = book.add_format({"bold": True, "font_color": "#374151"})

            if include_manifest:
                self._write_manifest(book, job, dataset, filters, columns,
                                     order_by, descending, title_fmt, label_fmt)

            sheet = None
            row_in_sheet = 0
            chosen: List[Any] = []
            col_formats: List[Any] = []
            sheet_index = 0
            written = 0

            def new_sheet():
                nonlocal sheet, row_in_sheet, sheet_index
                sheet_index += 1
                name = sheet_name if sheet_index == 1 else "{} ({})".format(sheet_name, sheet_index)
                sheet = book.add_worksheet(name[:31])
                sheet.write_row(0, 0, [c.name for c in chosen], header_fmt)
                sheet.freeze_panes(1, 0)
                sheet.set_row(0, 20)
                for idx, col in enumerate(chosen):
                    width = 12 if col.category in ("numeric", "boolean") else (
                        20 if col.category == "temporal" else 24)
                    sheet.set_column(idx, idx, min(60, max(len(col.name) + 3, width)))
                row_in_sheet = 1
                job.sheets = sheet_index

            for batch_columns, rows in self.engine.iter_rows(
                dataset, filters, columns, order_by, descending, row_limit=row_limit
            ):
                if not chosen:
                    chosen = batch_columns
                    for col in chosen:
                        upper = col.sql_type.upper()
                        if categorise(col.sql_type) != "temporal":
                            col_formats.append(None)
                        elif upper.startswith("DATE"):
                            col_formats.append(date_fmt)
                        elif upper.startswith("TIME") and not upper.startswith("TIMESTAMP"):
                            col_formats.append(time_fmt)
                        else:
                            col_formats.append(datetime_fmt)
                    new_sheet()

                for record in rows:
                    if job._cancel.is_set():
                        raise _Cancelled()
                    if row_in_sheet > EXCEL_MAX_DATA_ROWS:
                        sheet.autofilter(0, 0, row_in_sheet - 1, len(chosen) - 1)
                        new_sheet()
                    for col_idx, value in enumerate(record):
                        cell = _excel_value(value)
                        if cell is None:
                            continue
                        fmt = col_formats[col_idx]
                        if fmt is not None and isinstance(cell, (_dt.datetime, _dt.date, _dt.time)):
                            sheet.write_datetime(row_in_sheet, col_idx, cell, fmt)
                        else:
                            sheet.write(row_in_sheet, col_idx, cell)
                    row_in_sheet += 1
                    written += 1
                    # Publish often enough that the progress bar moves smoothly.
                    if written % 2000 == 0:
                        job.written = written

                job.written = written

            if sheet is None:
                # No matching rows: still produce a usable, correctly-headed sheet.
                chosen = self.engine._select_list(dataset, columns)[1]
                col_formats = [None] * len(chosen)
                new_sheet()
            else:
                sheet.autofilter(0, 0, max(1, row_in_sheet - 1), len(chosen) - 1)

            job.written = written
        finally:
            book.close()

    @staticmethod
    def _write_manifest(book, job, dataset, filters, columns, order_by, descending,
                        title_fmt, label_fmt):
        sheet = book.add_worksheet("Export info")
        sheet.set_column(0, 0, 22)
        sheet.set_column(1, 1, 90)
        sheet.write(0, 0, "Export summary", title_fmt)

        rows = [
            ("Source file", dataset.path),
            ("Exported at", _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("Rows in file", "{:,}".format(dataset.row_count)),
            ("Rows exported", "{:,}".format(job.total) if job.total is not None else "unknown"),
            ("Columns exported", ", ".join(columns) if columns else "all ({})".format(len(dataset.columns))),
            ("Sort", "{} {}".format(order_by, "desc" if descending else "asc") if order_by else "none"),
        ]
        line = 2
        for label, value in rows:
            sheet.write(line, 0, label, label_fmt)
            sheet.write(line, 1, str(value)[:EXCEL_MAX_CELL_CHARS])
            line += 1

        line += 1
        sheet.write(line, 0, "Filters applied", title_fmt)
        line += 1
        active = [f for f in (filters or []) if f.get("enabled") is not False]
        if not active:
            sheet.write(line, 1, "None — full file exported")
        for index, spec in enumerate(active, start=1):
            sheet.write(line, 0, "Filter {}".format(index), label_fmt)
            sheet.write(line, 1, describe(spec)[:EXCEL_MAX_CELL_CHARS])
            line += 1


    # ------------------------------------------------------------- diff worker

    def _run_diff(self, job, dataset, summary, config, fmt, include_manifest, sheet_name):
        try:
            job.status = "running"
            job.total = len(summary.get("columns") or [])
            job.message = "Writing the analysis…"
            if fmt == "csv":
                self._write_diff_csv(job, summary)
                job.sheets = 1
            else:
                self._write_diff_xlsx(job, dataset, summary, config, sheet_name,
                                      include_manifest)
            if job._cancel.is_set():
                raise _Cancelled()
            job.size = os.path.getsize(job.path) if os.path.exists(job.path) else 0
            job.status = "done"
            job.message = "Ready to download"
        except _Cancelled:
            job.status = "cancelled"
            job.message = "Export cancelled"
            self._discard(job)
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            job.status = "error"
            job.error = "{}: {}".format(type(exc).__name__, exc)
            job.message = "Export failed"
            traceback.print_exc()
            self._discard(job)
        finally:
            job.finished_at = time.time()

    @staticmethod
    def _write_diff_csv(job, summary):
        """The summary sheet, and only that -- CSV has no room for the rest."""
        with open(job.path, "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(DIFF_SUMMARY_HEADERS)
            for record in summary.get("columns") or []:
                writer.writerow([record.get(key) for key in DIFF_SUMMARY_FIELDS])
                job.written += 1

    def _write_diff_xlsx(self, job, dataset, summary, config, sheet_name, include_manifest):
        """Summary, examples and exclusions -- the three questions a reviewer asks.

        The verdict colours go on as conditional formats rather than as cell
        formats so that the meaning survives the reader sorting or filtering the
        sheet, which is the first thing anyone does with 70 rows.
        """
        book = xlsxwriter.Workbook(job.path, {"default_date_format": "yyyy-mm-dd",
                                              "remove_timezone": True})
        try:
            f = _diff_formats(book)
            records = summary.get("columns") or []

            sheet = book.add_worksheet(sheet_name[:31])
            sheet.activate()
            sheet.set_column(0, 0, 34)
            sheet.set_column(1, 1, 24)
            sheet.set_column(2, len(DIFF_SUMMARY_HEADERS) - 2, 14)
            sheet.set_column(len(DIFF_SUMMARY_HEADERS) - 1, len(DIFF_SUMMARY_HEADERS) - 1, 70)
            for index, title in enumerate(DIFF_SUMMARY_HEADERS):
                sheet.write(0, index, title, f["head"])
            for line, record in enumerate(records, start=1):
                for index, key in enumerate(DIFF_SUMMARY_FIELDS):
                    value = record.get(key)
                    fmt = f["pct"] if key.endswith("_pct") else f["cell"]
                    if value is None:
                        sheet.write_blank(line, index, None, fmt)
                    elif isinstance(value, (int, float)) and not isinstance(value, bool):
                        sheet.write_number(line, index, value, fmt)
                    else:
                        sheet.write(line, index, str(value)[:EXCEL_MAX_CELL_CHARS], fmt)
                job.written += 1
            if records:
                sheet.autofilter(0, 0, len(records), len(DIFF_SUMMARY_HEADERS) - 1)
                sheet.freeze_panes(1, 1)
                verdict_at = DIFF_SUMMARY_FIELDS.index("verdict")
                for verdict, key in DIFF_VERDICT_FORMATS.items():
                    sheet.conditional_format(1, verdict_at, len(records), verdict_at, {
                        "type": "cell", "criteria": "==",
                        "value": '"{}"'.format(verdict), "format": f[key]})

            self._write_diff_examples(job, dataset, summary, config, book, f)
            self._write_diff_excluded(summary, book, f)
            if include_manifest:
                self._write_diff_manifest(dataset, summary, config, book, f)
        finally:
            book.close()

    def _write_diff_examples(self, job, dataset, summary, config, book, f):
        """Ten real assets per differing column, so the verdict is checkable."""
        sheet = book.add_worksheet("Examples")
        sheet.set_column(0, 0, 34)
        sheet.set_column(1, 1, 18)
        sheet.set_column(2, 2, 16)
        sheet.set_column(3, 3, 44)
        sheet.set_column(4, 4, 9)
        sheet.set_column(5, 5, 11)
        key_label = (config.as_dict() if hasattr(config, "as_dict")
                     else dict(config or {})).get("key_column") or "Key"
        area_label = (config.as_dict() if hasattr(config, "as_dict")
                      else dict(config or {})).get("explain_column") or "Row"
        for index, title in enumerate(
                ("Column", key_label, area_label, "Value", "Blank?",
                 "Distinct values")):
            sheet.write(0, index, title, f["head"])

        differing = [r["column"] for r in (summary.get("columns") or [])
                     if r["verdict"] in (diffanalysis.TRUE_DIFF_OTHER,
                                         diffanalysis.TRUE_DIFF_BY_DEPR_AREA)]
        line = 1
        for column in differing[:DIFF_EXPORT_EXAMPLE_COLUMNS]:
            if job._cancel.is_set():
                raise _Cancelled()
            try:
                found = diffanalysis.examples(self.engine, dataset, config, column,
                                              limit=DIFF_EXPORT_EXAMPLE_ASSETS)
            except Exception:  # noqa: BLE001 - one column's evidence, not the file
                continue
            for asset in found.get("assets") or []:
                distinct = len(asset.get("distinct_values") or [])
                for row in asset["rows"]:
                    sheet.write(line, 0, column, f["cell"])
                    sheet.write(line, 1, str(asset["asset"]), f["cell"])
                    sheet.write(line, 2, "" if row["area"] is None else str(row["area"]),
                                f["cell"])
                    text = "" if row["value"] is None else str(row["value"])
                    sheet.write(line, 3, text[:EXCEL_MAX_CELL_CHARS],
                                f["diff_cell"] if asset["differs"] else f["cell"])
                    sheet.write(line, 4, "yes" if row["blank"] else "", f["cell"])
                    sheet.write_number(line, 5, distinct, f["cell"])
                    line += 1
        if line == 1:
            sheet.write(1, 0, "No column showed a true difference.", f["cell"])
        else:
            sheet.autofilter(0, 0, line - 1, 5)
            sheet.freeze_panes(1, 0)

    @staticmethod
    def _write_diff_excluded(summary, book, f):
        """Which columns never got a verdict, and why -- an answer to "where is X?"."""
        sheet = book.add_worksheet("Excluded columns")
        sheet.set_column(0, 0, 34)
        sheet.set_column(1, 1, 46)
        sheet.write(0, 0, "Column", f["head"])
        sheet.write(0, 1, "Why it was excluded", f["head"])
        excluded = summary.get("excluded") or []
        for line, record in enumerate(excluded, start=1):
            sheet.write(line, 0, record.get("column", ""), f["cell"])
            sheet.write(line, 1, record.get("reason", ""), f["cell"])
        if not excluded:
            sheet.write(1, 0, "Every column was analysed.", f["cell"])

    @staticmethod
    def _write_diff_manifest(dataset, summary, config, book, f):
        sheet = book.add_worksheet("Export info")
        sheet.set_column(0, 0, 26)
        sheet.set_column(1, 1, 90)
        sheet.write(0, 0, "Difference analysis", f["title"])

        spec = config.as_dict() if hasattr(config, "as_dict") else dict(config or {})
        grain = summary.get("duplicate_grain") or {}
        if not grain.get("checked"):
            grain_text = "Not checked — no depreciation-area column was chosen."
        elif grain.get("unique"):
            grain_text = "One row per ({}, {}) — the grain is what it should be.".format(
                spec.get("key_column"), grain.get("column"))
        else:
            grain_text = (
                "{:,} ({}, {}) pairs appear on more than one row ({:,} extra rows, worst "
                "case {} rows for one pair). The extract is not one row per pair, so an "
                "unexplained difference may be nothing more than that.".format(
                    grain.get("duplicate_pairs", 0), spec.get("key_column"),
                    grain.get("column"), grain.get("extra_rows", 0),
                    grain.get("max_rows_per_pair", 0)))

        sample = spec.get("sample_assets")
        rows = [
            ("Source file", dataset.path),
            ("Exported at", _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("Rows in file", "{:,}".format(dataset.row_count)),
            ("Grouping key", spec.get("key_column")),
            ("Explanatory column", spec.get("explain_column") or "none"),
            ("Sample mode", "{:,} assets sampled — figures are an estimate".format(sample)
                            if sample else "Off — every asset was analysed"),
            ("Assets analysed", "{:,}".format(summary.get("assets_analysed") or 0)),
            ("Rows analysed", "{:,}".format(summary.get("rows_analysed") or 0)),
            ("Columns analysed", str(summary.get("columns_analysed") or 0)),
            ("Columns excluded", str(summary.get("columns_excluded") or 0)),
            ("Columns selected", ", ".join(spec.get("include"))
                                 if spec.get("include") is not None
                                 else "every column in the file"),
            ("Exclusions", ", ".join(spec.get("exclude") or []) or "none"),
            ("Blank columns excluded", "yes" if spec.get("exclude_all_blank") else "no"),
            ("Examples per differing column",
             "{} assets, with every one of their rows".format(DIFF_EXPORT_EXAMPLE_ASSETS)),
            ("Duplicate grain check", grain_text),
        ]
        for verdict, count in (summary.get("verdict_counts") or {}).items():
            if count:
                rows.append(("Verdict: {}".format(verdict), str(count)))

        line = 2
        for label, value in rows:
            sheet.write(line, 0, label, f["manifest_label"])
            sheet.write(line, 1, str(value)[:EXCEL_MAX_CELL_CHARS])
            line += 1

        line += 1
        sheet.write(line, 0, "Filters applied", f["title"])
        line += 1
        active = [x for x in (spec.get("filters") or []) if x.get("enabled") is not False]
        if not active:
            sheet.write(line, 1, "None — the whole file was analysed")
        for index, one in enumerate(active, start=1):
            sheet.write(line, 0, "Filter {}".format(index), f["manifest_label"])
            sheet.write(line, 1, describe(one)[:EXCEL_MAX_CELL_CHARS])
            line += 1

    # ------------------------------------------------------------ pivot worker

    def _run_pivot(self, job, dataset, spec, sheet_name, include_manifest, max_rows):
        try:
            job.status = "counting"
            job.message = "Aggregating…"

            rows = spec.get("rows") or []
            groups = pivot_module.count_row_groups(
                self.engine, dataset, rows, spec.get("filters") or [])
            if rows and groups > max_rows:
                # Too big for one workbook. Split it on the outermost Rows field
                # so each file holds whole groups, and ship the set as a zip.
                self._run_pivot_split(job, dataset, spec, sheet_name, include_manifest,
                                      max_rows, groups)
                return

            result = pivot_module.compute(
                self.engine, dataset,
                rows=spec.get("rows") or [],
                columns=spec.get("columns") or [],
                values=spec.get("values") or [],
                filters=spec.get("filters") or [],
                subtotals=spec.get("subtotals", True),
                row_totals=spec.get("row_totals", True),
                column_totals=spec.get("column_totals", True),
                sort=spec.get("sort"),
                max_rows=max_rows,
                jsonable=False,
            )
            if job._cancel.is_set():
                raise _Cancelled()

            job.total = len(result["rows"])
            job.status = "running"
            job.message = "Writing the pivot…"
            self._write_pivot_xlsx(job, dataset, spec, result, sheet_name, include_manifest)

            if job._cancel.is_set():
                raise _Cancelled()
            job.size = os.path.getsize(job.path) if os.path.exists(job.path) else 0
            job.sheets = 1
            job.status = "done"
            job.message = "Ready to download"
        except _Cancelled:
            job.status = "cancelled"
            job.message = "Export cancelled"
            self._discard(job)
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            job.status = "error"
            job.error = "{}: {}".format(type(exc).__name__, exc)
            job.message = "Export failed"
            traceback.print_exc()
            self._discard(job)
        finally:
            job.finished_at = time.time()

    @staticmethod
    def _pack_batches(sizes, max_rows):
        """Group the outermost values into runs of at most `max_rows` row groups.

        Greedy and order-preserving, so each file covers a contiguous stretch of
        the field and the files read in the same order as the pivot. A single
        value bigger than the cap gets a file to itself rather than being cut.
        """
        batches, current, taken = [], [], 0
        for value, count in sizes:
            if current and taken + count > max_rows:
                batches.append((current, taken))
                current, taken = [], 0
            current.append(value)
            taken += count
        if current:
            batches.append((current, taken))
        return batches

    def _run_pivot_split(self, job, dataset, spec, sheet_name, include_manifest,
                         max_rows, groups):
        """Write the pivot across several workbooks and zip them together."""
        rows = spec["rows"]
        filters = spec.get("filters") or []
        sizes = pivot_module.outer_group_sizes(self.engine, dataset, rows, filters)
        batches = self._pack_batches(sizes, max_rows)

        job.total = groups
        job.status = "running"
        job.sheets = len(batches)
        job.fmt = "zip"
        job.filename = os.path.splitext(job.filename)[0] + ".zip"
        job.path = os.path.splitext(job.path)[0] + ".zip"
        totals = pivot_module.overall_totals(self.engine, dataset, spec["values"], filters)

        stage = tempfile.mkdtemp(prefix="pqs-pivot-", dir=self.output_dir)
        written_groups = 0
        try:
            with zipfile.ZipFile(job.path, "w", zipfile.ZIP_DEFLATED) as bundle:
                for index, (values, count) in enumerate(batches, start=1):
                    if job._cancel.is_set():
                        raise _Cancelled()
                    job.message = "Writing file {} of {}…".format(index, len(batches))
                    part = dict(spec, filters=list(filters) + [
                        {"column": rows[0], "op": "in", "values": list(values)}])
                    result = pivot_module.compute(
                        self.engine, dataset,
                        rows=rows,
                        columns=spec.get("columns") or [],
                        values=spec["values"],
                        filters=part["filters"],
                        subtotals=spec.get("subtotals", True),
                        row_totals=spec.get("row_totals", True),
                        column_totals=spec.get("column_totals", True),
                        sort=spec.get("sort"),
                        max_rows=max(1, count),
                        jsonable=False,
                        # This file holds a slice, so its own total row must not
                        # claim to be the total for the whole pivot.
                        grand_label="Total (file {} of {})".format(index, len(batches)),
                    )
                    name = "{}-{:03d}-of-{:03d}.xlsx".format(
                        os.path.splitext(job.filename)[0], index, len(batches))
                    path = os.path.join(stage, name)
                    part["split"] = {
                        "index": index, "of": len(batches), "field": rows[0],
                        "values": [pivot_module.label_of(v) for v in values],
                        "totals": totals, "row_groups": count, "total_row_groups": groups,
                    }
                    self._write_pivot_xlsx(job, dataset, part, result, sheet_name,
                                           include_manifest, path=path, track_progress=False)
                    bundle.write(path, name)
                    os.remove(path)
                    written_groups += count
                    job.written = min(written_groups, groups)
        finally:
            shutil.rmtree(stage, ignore_errors=True)

        job.size = os.path.getsize(job.path) if os.path.exists(job.path) else 0
        job.status = "done"
        job.message = "{} files ready to download".format(len(batches))

    def _write_pivot_xlsx(self, job, dataset, spec, result, sheet_name, include_manifest,
                          path=None, track_progress=True):
        """Lay the computed pivot out the way Excel lays out a PivotTable."""
        book = xlsxwriter.Workbook(
            path or job.path,
            {"constant_memory": True, "default_date_format": "yyyy-mm-dd hh:mm:ss",
             "remove_timezone": True},
        )
        try:
            f = _pivot_formats(book)
            row_fields = [c["name"] for c in result["row_fields"]]
            column_fields = [c["name"] for c in result["column_fields"]]
            leaves = result["leaves"]
            n_labels = max(1, len(row_fields))

            # The pivot is the point of the file, so it gets the first tab.
            sheet = book.add_worksheet(sheet_name[:31])
            sheet.activate()
            sheet.set_column(0, n_labels - 1, 26)
            sheet.set_column(n_labels, n_labels + len(leaves) - 1, 16)

            line = 0
            # Column-field headers, one merged band per level.
            for level, cells in enumerate(result["header"]):
                if column_fields and level < len(column_fields):
                    sheet.write(line, 0, column_fields[level], f["corner"])
                    for extra in range(1, n_labels):
                        sheet.write(line, extra, "", f["corner"])
                at = n_labels
                for cell in cells:
                    fmt = f["total_head"] if cell.get("kind") == "total" else f["head"]
                    text = "" if cell.get("skip") else cell["label"]
                    span = cell.get("span", 1)
                    if span > 1:
                        sheet.merge_range(line, at, line, at + span - 1, text, fmt)
                    else:
                        sheet.write(line, at, text, fmt)
                    at += span
                sheet.set_row(line, 20)
                line += 1

            # The row-field names sit directly above the label column(s).
            for index in range(n_labels):
                sheet.write(line, index, row_fields[index] if index < len(row_fields) else "",
                            f["corner"])
            for offset in range(len(leaves)):
                sheet.write(line, n_labels + offset, "", f["corner"])
            sheet.set_row(line, 18)
            header_rows = line + 1
            line += 1

            sheet.freeze_panes(header_rows, n_labels)

            # With "Repeat labels" on, every row carries its outer labels, so the
            # sheet can be sorted or filtered without the groups falling apart.
            repeat_labels = bool(spec.get("repeat_labels"))
            previous_labels: List[str] = []
            written = 0
            for record in result["rows"]:
                if job._cancel.is_set():
                    raise _Cancelled()
                kind = record["kind"]
                label_fmt = f["label"] if kind == "data" else (
                    f["grand_label"] if kind == "grand" else f["subtotal_label"])
                cell_fmt = f["cell"] if kind == "data" else (
                    f["grand_cell"] if kind == "grand" else f["subtotal_cell"])
                date_fmt = f["cell_date"] if kind == "data" else f["subtotal_date"]

                labels = record["labels"]
                for index in range(n_labels):
                    text = labels[index] if index < len(labels) else ""
                    # Repeated outer labels are left blank, as Excel does.
                    if (not repeat_labels and kind == "data" and index < n_labels - 1
                            and index < len(previous_labels) and previous_labels[index] == text):
                        text = ""
                    sheet.write(line, index, text, label_fmt)
                if kind == "data":
                    previous_labels = list(labels)
                else:
                    previous_labels = []

                for offset, value in enumerate(record["cells"]):
                    at = n_labels + offset
                    if value is None:
                        sheet.write_blank(line, at, None, cell_fmt)
                        continue
                    value = _excel_value(value)
                    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
                        sheet.write_datetime(line, at, value, date_fmt)
                    elif isinstance(value, (int, float)) and not isinstance(value, bool):
                        fmt = cell_fmt if not leaves[offset].get("is_count") else (
                            f["count"] if kind == "data" else (
                                f["grand_count"] if kind == "grand" else f["subtotal_count"]))
                        sheet.write_number(line, at, value, fmt)
                    else:
                        sheet.write(line, at, value, cell_fmt)
                line += 1
                written += 1
                if track_progress and written % 500 == 0:
                    job.written = written
            if track_progress:
                job.written = written

            if include_manifest:
                self._write_pivot_manifest(book, dataset, spec, result, f)
        finally:
            book.close()

    @staticmethod
    def _write_pivot_manifest(book, dataset, spec, result, f):
        sheet = book.add_worksheet("Export info")
        sheet.set_column(0, 0, 22)
        sheet.set_column(1, 1, 90)
        sheet.write(0, 0, "Pivot summary", f["title"])

        values = ", ".join(v["label"] for v in result["values"]) or "none"
        rows = [
            ("Source file", dataset.path),
            ("Exported at", _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("Rows in file", "{:,}".format(dataset.row_count)),
            ("Row fields", ", ".join(c["name"] for c in result["row_fields"]) or "none"),
            ("Column fields", ", ".join(c["name"] for c in result["column_fields"]) or "none"),
            ("Values", values),
            ("Row groups", "{:,}".format(result["total_row_groups"])),
            ("Column groups", "{:,}".format(result["column_groups"])),
        ]
        if result.get("truncated"):
            rows.append(("Truncated", "Only the first {:,} row groups were written. "
                                      "Totals still cover every matching row.".format(
                                          result["row_groups"])))

        split = spec.get("split")
        if split:
            span = split["values"]
            covers = span[0] if len(span) == 1 else "{} … {}".format(span[0], span[-1])
            rows += [
                ("File", "{} of {}".format(split["index"], split["of"])),
                ("This file covers", "{} = {} ({} of {:,} row groups)".format(
                    split["field"], covers, "{:,}".format(split["row_groups"]),
                    split["total_row_groups"])),
                ("Why it is split", "The pivot has {:,} row groups, more than one sheet "
                                    "reads comfortably, so it was cut on whole values of "
                                    "{!r} -- no group is split across files.".format(
                                        split["total_row_groups"], split["field"])),
                ("Total row", "The total row on the pivot sheet covers this file only."),
            ]
            for label, value in split["totals"]:
                rows.append(("{} (all files)".format(label), _manifest_number(value)))
        line = 2
        for label, value in rows:
            sheet.write(line, 0, label, f["manifest_label"])
            sheet.write(line, 1, str(value)[:EXCEL_MAX_CELL_CHARS])
            line += 1

        line += 1
        sheet.write(line, 0, "Filters applied", f["title"])
        line += 1
        active = [x for x in (spec.get("filters") or []) if x.get("enabled") is not False]
        if not active:
            sheet.write(line, 1, "None — the whole file was aggregated")
        for index, one in enumerate(active, start=1):
            sheet.write(line, 0, "Filter {}".format(index), f["manifest_label"])
            sheet.write(line, 1, describe(one)[:EXCEL_MAX_CELL_CHARS])
            line += 1


def _manifest_number(value: Any) -> str:
    """A measure total as text for the manifest sheet."""
    if value is None:
        return "—"
    if isinstance(value, float):
        return "{:,.2f}".format(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return "{:,}".format(value)
    return str(value)


def _diff_formats(book) -> Dict[str, Any]:
    """Formats for the analysis workbook.

    The verdict colours are the ones the screen uses, so a reviewer who saw the
    table in the browser recognises the sheet: green agrees, blue complements,
    EY yellow is explained by the depreciation area, red is not. The header band
    is EY's #1A1A24 for the same reason -- a workbook that lands in someone's
    inbox should look like it came from the same place as the screen did.
    """
    head = {"bold": True, "bg_color": "#1A1A24", "font_color": "#FFFFFF", "border": 1,
            "border_color": "#2E2E38", "align": "left", "valign": "vcenter"}
    def band(bg, fg):
        return book.add_format({"bg_color": bg, "font_color": fg, "bold": True})
    return {
        "head": book.add_format(head),
        "title": book.add_format({"bold": True, "font_size": 13, "font_color": "#1A1A24"}),
        "manifest_label": book.add_format({"bold": True, "font_color": "#2E2E38"}),
        "cell": book.add_format({"border": 1, "border_color": "#EAEAF2"}),
        "pct": book.add_format({"num_format": "0.00", "border": 1, "border_color": "#EAEAF2"}),
        "diff_cell": book.add_format({"border": 1, "border_color": "#EAEAF2",
                                      "bg_color": "#FFF8B8"}),
        "v_constant": band("#DFF7E4", "#0F6B1F"),
        "v_sparse": band("#DCF0FD", "#035A8F"),
        # The one verdict that fills with the brand yellow, dark ink on it.
        "v_explained": band("#FFEB0A", "#1A1A24"),
        "v_unexplained": band("#F7DDDD", "#A11C1C"),
        "v_blank": band("#EAEAF2", "#747480"),
        "v_error": band("#F7DDDD", "#7A1414"),
    }


def _pivot_formats(book) -> Dict[str, Any]:
    """Every cell format the pivot sheet uses, built once per workbook."""
    number = "#,##0.00"
    count = "#,##0"
    head = {"bold": True, "bg_color": "#1F2937", "font_color": "#FFFFFF", "border": 1,
            "border_color": "#374151", "align": "center", "valign": "vcenter"}
    return {
        "head": book.add_format(head),
        "total_head": book.add_format(dict(head, bg_color="#111827")),
        "corner": book.add_format(dict(head, align="left")),
        "title": book.add_format({"bold": True, "font_size": 13}),
        "manifest_label": book.add_format({"bold": True, "font_color": "#374151"}),
        "label": book.add_format({"border": 1, "border_color": "#E5E7EB"}),
        "subtotal_label": book.add_format({"bold": True, "bg_color": "#F3F4F6", "border": 1,
                                           "border_color": "#E5E7EB"}),
        "grand_label": book.add_format({"bold": True, "bg_color": "#E5E7EB", "border": 1,
                                        "border_color": "#D1D5DB", "top": 2}),
        "cell": book.add_format({"num_format": number, "border": 1, "border_color": "#E5E7EB"}),
        "count": book.add_format({"num_format": count, "border": 1, "border_color": "#E5E7EB"}),
        "cell_date": book.add_format({"num_format": "yyyy-mm-dd hh:mm:ss", "border": 1,
                                      "border_color": "#E5E7EB"}),
        "subtotal_cell": book.add_format({"num_format": number, "bold": True, "bg_color": "#F3F4F6",
                                          "border": 1, "border_color": "#E5E7EB"}),
        "subtotal_count": book.add_format({"num_format": count, "bold": True, "bg_color": "#F3F4F6",
                                           "border": 1, "border_color": "#E5E7EB"}),
        "subtotal_date": book.add_format({"num_format": "yyyy-mm-dd hh:mm:ss", "bold": True,
                                          "bg_color": "#F3F4F6", "border": 1,
                                          "border_color": "#E5E7EB"}),
        "grand_cell": book.add_format({"num_format": number, "bold": True, "bg_color": "#E5E7EB",
                                       "border": 1, "border_color": "#D1D5DB", "top": 2}),
        "grand_count": book.add_format({"num_format": count, "bold": True, "bg_color": "#E5E7EB",
                                        "border": 1, "border_color": "#D1D5DB", "top": 2}),
    }


class _Cancelled(Exception):
    pass
