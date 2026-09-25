"""FastAPI application: static UI + JSON API over the DuckDB engine.

Query endpoints are declared with `def` (not `async def`) so Starlette runs them
in a worker thread -- a slow scan never blocks the event loop or the UI.
"""

import json
import os
import shutil
import tempfile
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import diffanalysis as diff_module
from . import exporter as exporter_module
from . import pivot as pivot_module
from .diffanalysis import DiffConfig
from .engine import Engine
from .exporter import ExportManager
from .filters import FilterError

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(HERE, "static")
RECENTS_FILE = os.path.join(os.path.expanduser("~"), ".parquet-reader-recents.json")
MAX_RECENTS = 12

engine = Engine()
exports = ExportManager(engine)
app = FastAPI(title="Parquet Studio", docs_url="/api/docs", redoc_url=None)


# --------------------------------------------------------------------- models

class OpenRequest(BaseModel):
    path: str


class UnionRequest(BaseModel):
    paths: List[str]
    # Add a column naming the file each row came from.
    source_column: bool = True


class QueryRequest(BaseModel):
    dataset_id: str
    filters: List[Dict[str, Any]] = Field(default_factory=list)
    columns: Optional[List[str]] = None
    limit: int = 10
    offset: int = 0
    order_by: Optional[str] = None
    descending: bool = False
    # column -> part to extract ("year", "month" or "day") in place of the value,
    # or {"part": ..., "format": ...} for a column holding dates as text.
    transforms: Dict[str, Any] = Field(default_factory=dict)


class CountRequest(BaseModel):
    dataset_id: str
    filters: List[Dict[str, Any]] = Field(default_factory=list)


class ValuesRequest(BaseModel):
    dataset_id: str
    column: str
    filters: List[Dict[str, Any]] = Field(default_factory=list)
    search: str = ""
    limit: int = 300
    # A pasted list: look these values up exactly instead of searching.
    exact: Optional[List[str]] = None
    # List an extracted part of the column (e.g. its years) instead.
    transform: Optional[str] = None
    # How to read a column that holds dates as text (one of DATE_FORMATS).
    date_format: Optional[str] = None


class StatsRequest(BaseModel):
    dataset_id: str
    column: str
    filters: List[Dict[str, Any]] = Field(default_factory=list)
    transform: Optional[str] = None
    date_format: Optional[str] = None


class PivotValue(BaseModel):
    column: Optional[str] = None
    agg: str = "sum"
    label: Optional[str] = None


class PivotRequest(BaseModel):
    dataset_id: str
    rows: List[str] = Field(default_factory=list)
    columns: List[str] = Field(default_factory=list)
    values: List[PivotValue] = Field(default_factory=list)
    filters: List[Dict[str, Any]] = Field(default_factory=list)
    subtotals: bool = True
    row_totals: bool = True
    column_totals: bool = True
    # Presentation only: the pivot always carries the full labels, so this
    # decides whether a repeated outer label is printed or left blank.
    repeat_labels: bool = False
    sort: Optional[Dict[str, Any]] = None
    max_rows: int = pivot_module.DEFAULT_MAX_ROW_KEYS


class PivotExportRequest(PivotRequest):
    sheet_name: str = "Pivot"
    include_manifest: bool = True


class DiffStartRequest(BaseModel):
    dataset_id: str
    key_column: str
    explain_column: Optional[str] = None
    # Omitted means "every column", which is what the analysis did before the
    # picker existed; a list is the reviewer's ticked set.
    include: Optional[List[str]] = None
    exclude: List[str] = Field(default_factory=list)
    sample_assets: Optional[int] = None
    exclude_all_blank: bool = True
    # The UI's "use the filters I have set" toggle is expressed by whether it
    # sends them, so there is one source of truth for what a run covered.
    filters: List[Dict[str, Any]] = Field(default_factory=list)


class DiffExportRequest(BaseModel):
    format: str = "xlsx"
    include_manifest: bool = True
    sheet_name: str = "Difference analysis"


class ExportRequest(BaseModel):
    dataset_id: str
    format: str = "xlsx"
    filters: List[Dict[str, Any]] = Field(default_factory=list)
    columns: Optional[List[str]] = None
    order_by: Optional[str] = None
    descending: bool = False
    row_limit: Optional[int] = None
    include_manifest: bool = True
    sheet_name: str = "Data"
    total_hint: Optional[int] = None
    # column -> part to extract; the export writes that part in the column.
    transforms: Dict[str, Any] = Field(default_factory=dict)


# ------------------------------------------------------------------- recents

def _load_recents() -> List[Dict[str, Any]]:
    try:
        with open(RECENTS_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return [r for r in data if isinstance(r, dict) and os.path.exists(r.get("path", ""))]
    except (OSError, ValueError):
        return []


def _remember(path: str, name: str, rows: int) -> None:
    recents = [r for r in _load_recents() if r.get("path") != path]
    recents.insert(0, {"path": path, "name": name, "rows": rows})
    try:
        with open(RECENTS_FILE, "w", encoding="utf-8") as handle:
            json.dump(recents[:MAX_RECENTS], handle, indent=2)
    except OSError:
        pass


# ------------------------------------------------------------------ diff jobs

@dataclass
class DiffJob:
    """One difference-analysis run.

    Seventy sequential aggregate queries over an 11GB file take minutes, so the
    run is a job rather than a request -- same shape as an export. What differs
    is that its partial results are worth reading: each column is a finished
    answer the moment it lands, so the table fills in while the run continues
    rather than staying blank until the end.
    """

    id: str
    dataset_id: str
    config: DiffConfig
    status: str = "queued"        # queued | running | done | error | cancelled
    total: int = 0
    current: Optional[str] = None
    results: List[Dict[str, Any]] = field(default_factory=list)
    summary: Optional[Dict[str, Any]] = None
    plan: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    _cancel: threading.Event = field(default_factory=threading.Event, repr=False)

    def as_dict(self, include_results: bool = False) -> Dict[str, Any]:
        done = len(self.results)
        elapsed = (self.finished_at or time.time()) - self.created_at
        out = {
            "id": self.id,
            "dataset_id": self.dataset_id,
            "status": self.status,
            "done": done,
            "total": self.total,
            "percent": round(100.0 * done / self.total, 1) if self.total else None,
            "current": self.current,
            "error": self.error,
            "elapsed": round(elapsed, 1),
            "config": self.config.as_dict(),
            "plan": self.plan,
        }
        if include_results:
            # Snapshot: the worker appends to this list from its own thread.
            out["results"] = list(self.results)
        return out


class DiffRunner:
    """Runs difference analyses in the background, one thread each.

    Deliberately thin: every decision about *what* the analysis means lives in
    diffanalysis.py, and this only decides when to stop and what to report.
    """

    MAX_JOBS = 12

    def __init__(self, engine: Engine):
        self.engine = engine
        self.jobs: Dict[str, DiffJob] = {}
        self._lock = threading.Lock()

    def start(self, dataset_id: str, config: DiffConfig) -> DiffJob:
        dataset = self.engine.get(dataset_id)
        # Fail a bad configuration here, in the request, rather than in a thread
        # where the only way to see it is to poll for a status of "error".
        diff_module._validate(dataset, config)
        job = DiffJob(id=uuid.uuid4().hex[:12], dataset_id=dataset_id, config=config)
        with self._lock:
            self.jobs[job.id] = job
            if len(self.jobs) > self.MAX_JOBS:
                for stale in sorted(self.jobs.values(), key=lambda j: j.created_at)[
                        :len(self.jobs) - self.MAX_JOBS]:
                    if stale.status in ("done", "error", "cancelled"):
                        self.jobs.pop(stale.id, None)
        threading.Thread(target=self._run, args=(job,), daemon=True).start()
        return job

    def get(self, job_id: str) -> DiffJob:
        job = self.jobs.get(job_id)
        if job is None:
            raise KeyError("That analysis is no longer available. Run it again.")
        return job

    def cancel(self, job_id: str) -> DiffJob:
        job = self.get(job_id)
        job._cancel.set()
        return job

    def _run(self, job: DiffJob) -> None:
        plan = None
        try:
            dataset = self.engine.get(job.dataset_id)
            job.status = "running"
            plan = diff_module.prepare(self.engine, dataset, job.config)
            job.total = len(plan.columns)
            job.plan = {
                "assets": plan.assets,
                "rows": plan.rows,
                "excluded": plan.excluded,
                "all_blank_columns": plan.all_blank,
                "duplicate_grain": plan.duplicate_grain,
                "record_consistency": plan.record_consistency,
                "waterfall": plan.waterfall_steps,
                "columns": list(plan.columns),
            }
            results = []
            for column in plan.columns:
                if job._cancel.is_set():
                    job.status = "cancelled"
                    break
                job.current = column
                result = diff_module.analyse_column(self.engine, dataset, job.config,
                                                    plan, column)
                results.append(result)
                job.results.append(result.as_dict())
            job.current = None
            # A cancelled run still summarises what it managed to finish -- the
            # columns already answered are answered.
            job.summary = diff_module.summarise(dataset, job.config, plan, results)
            if job.status != "cancelled":
                job.status = "done"
        except Exception as exc:  # noqa: BLE001 - surfaced to the UI
            job.status = "error"
            job.error = "{}: {}".format(type(exc).__name__, exc)
            traceback.print_exc()
        finally:
            if plan is not None:
                plan.release(self.engine)
            job.finished_at = time.time()


diffs = DiffRunner(engine)


# --------------------------------------------------------------------- errors

def _guard(fn, *args, **kwargs):
    """Run an engine call, mapping its exceptions onto HTTP responses."""
    try:
        return fn(*args, **kwargs)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0] if exc.args else exc))
    except (FilterError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail="{}: {}".format(type(exc).__name__, exc))


# --------------------------------------------------------------------- routes

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    with open(os.path.join(STATIC_DIR, "index.html"), "r", encoding="utf-8") as handle:
        page = handle.read()
    # Stamp each asset URL with the file's mtime. Without it a browser that has
    # app.js cached will happily pair an old script with a new index.html --
    # every new control renders but nothing is wired to it.
    for asset in ("app.js", "styles.css"):
        try:
            stamp = int(os.path.getmtime(os.path.join(STATIC_DIR, asset)))
        except OSError:
            continue
        page = page.replace("/static/" + asset, "/static/{}?v={}".format(asset, stamp))
    return HTMLResponse(page)


@app.get("/api/recents")
def recents() -> Dict[str, Any]:
    return {"recents": _load_recents()}


@app.get("/api/browse")
def browse(path: str = "~") -> Dict[str, Any]:
    """Minimal directory listing so the UI can pick files without an OS dialog."""
    target = os.path.abspath(os.path.expanduser(path or "~"))
    if not os.path.isdir(target):
        target = os.path.dirname(target) or os.path.expanduser("~")
    entries = []
    try:
        for name in sorted(os.listdir(target), key=str.lower):
            if name.startswith("."):
                continue
            full = os.path.join(target, name)
            is_dir = os.path.isdir(full)
            if not is_dir and not name.lower().endswith(".parquet"):
                continue
            entries.append({
                "name": name,
                "path": full,
                "is_dir": is_dir,
                "size": (os.path.getsize(full) if not is_dir else 0),
            })
    except OSError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "path": target,
        "parent": os.path.dirname(target) if target != os.path.dirname(target) else None,
        "entries": entries[:500],
    }


@app.post("/api/open")
def open_dataset(request: OpenRequest) -> Dict[str, Any]:
    dataset = _guard(engine.open, request.path)
    _remember(dataset.path, dataset.display_name, dataset.row_count)
    return dataset.as_dict()


@app.post("/api/union")
def union_datasets(request: UnionRequest) -> Dict[str, Any]:
    dataset = _guard(engine.open_union, request.paths, request.source_column)
    return dataset.as_dict()


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)) -> Dict[str, Any]:
    if not file.filename or not file.filename.lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail="Please choose a .parquet file.")
    staging = os.path.join(tempfile.gettempdir(), "parquet-reader-uploads")
    os.makedirs(staging, exist_ok=True)
    destination = os.path.join(staging, os.path.basename(file.filename))
    with open(destination, "wb") as handle:
        shutil.copyfileobj(file.file, handle, length=4 * 1024 * 1024)
    dataset = _guard(engine.open, destination, True, os.path.basename(file.filename))
    return dataset.as_dict()


@app.get("/api/dataset/{dataset_id}")
def dataset_info(dataset_id: str) -> Dict[str, Any]:
    return _guard(engine.get, dataset_id).as_dict()


@app.delete("/api/dataset/{dataset_id}")
def close_dataset(dataset_id: str) -> Dict[str, Any]:
    engine.close(dataset_id)
    return {"closed": True}


@app.post("/api/preview")
def preview(request: QueryRequest) -> Dict[str, Any]:
    dataset = _guard(engine.get, request.dataset_id)
    limit = max(1, min(int(request.limit), 1000))
    return _guard(
        engine.preview, dataset, request.filters, request.columns,
        limit, max(0, int(request.offset)), request.order_by, request.descending,
        request.transforms,
    )


@app.post("/api/count")
def count(request: CountRequest) -> Dict[str, Any]:
    dataset = _guard(engine.get, request.dataset_id)
    matched = _guard(engine.count, dataset, request.filters)
    return {"count": matched, "total": dataset.row_count}


@app.post("/api/values")
def values(request: ValuesRequest) -> Dict[str, Any]:
    dataset = _guard(engine.get, request.dataset_id)
    return _guard(
        engine.distinct_values, dataset, request.column, request.filters,
        request.search, max(1, min(int(request.limit), 2000)), request.exact,
        request.transform, request.date_format,
    )


@app.post("/api/stats")
def stats(request: StatsRequest) -> Dict[str, Any]:
    dataset = _guard(engine.get, request.dataset_id)
    return _guard(engine.column_stats, dataset, request.column, request.filters, request.transform,
                  request.date_format)


@app.get("/api/pivot/aggregations")
def pivot_aggregations() -> Dict[str, Any]:
    """The aggregations the Values well offers, and what each one accepts."""
    return {
        "aggregations": [
            {"id": key, "label": verb, "categories": list(allowed) if allowed else None,
             "needs_column": key != "count_rows"}
            for key, (verb, _sql, allowed) in pivot_module.AGGREGATIONS.items()
        ],
        "max_columns": pivot_module.MAX_COLUMN_KEYS,
        "max_rows": pivot_module.DEFAULT_MAX_ROW_KEYS,
        # A pivot bigger than this is exported as several workbooks in a zip.
        "export_rows_per_file": exporter_module.PIVOT_EXPORT_MAX_ROWS,
    }


@app.post("/api/pivot")
def build_pivot(request: PivotRequest) -> Dict[str, Any]:
    dataset = _guard(engine.get, request.dataset_id)
    return _guard(
        pivot_module.compute, engine, dataset, request.rows, request.columns,
        [v.model_dump() for v in request.values], request.filters,
        request.subtotals, request.row_totals, request.column_totals,
        request.sort, max(1, int(request.max_rows)),
    )


@app.post("/api/pivot/export")
def export_pivot(request: PivotExportRequest) -> Dict[str, Any]:
    _guard(engine.get, request.dataset_id)
    spec = {
        "rows": request.rows,
        "columns": request.columns,
        "values": [v.model_dump() for v in request.values],
        "filters": request.filters,
        "subtotals": request.subtotals,
        "row_totals": request.row_totals,
        "column_totals": request.column_totals,
        "repeat_labels": request.repeat_labels,
        "sort": request.sort,
    }
    job = _guard(exports.start_pivot, request.dataset_id, spec,
                 request.sheet_name or "Pivot", request.include_manifest)
    return job.as_dict()


# --------------------------------------------------------- difference analysis

@app.get("/api/diff/defaults")
def diff_defaults() -> Dict[str, Any]:
    """What the config bar starts with, so the business list lives in one place."""
    return {
        "excluded_columns": list(diff_module.DEFAULT_EXCLUDED_COLUMNS),
        "verdicts": list(diff_module.VERDICTS),
        "recommendations": diff_module.RECOMMENDATIONS,
        "default_sample_assets": 10_000,
        "example_page_size": diff_module.DEFAULT_EXAMPLE_ASSETS,
    }


@app.post("/api/diff/start")
def start_diff(request: DiffStartRequest) -> Dict[str, Any]:
    config = DiffConfig(
        key_column=request.key_column,
        explain_column=request.explain_column,
        include=request.include,
        exclude=request.exclude,
        sample_assets=request.sample_assets,
        filters=request.filters,
        exclude_all_blank=request.exclude_all_blank,
    )
    return _guard(diffs.start, request.dataset_id, config).as_dict()


@app.get("/api/diff/status/{job_id}")
def diff_status(job_id: str) -> Dict[str, Any]:
    """Progress plus every column finished so far, so the table fills in live."""
    return _guard(diffs.get, job_id).as_dict(include_results=True)


@app.post("/api/diff/cancel/{job_id}")
def cancel_diff(job_id: str) -> Dict[str, Any]:
    return _guard(diffs.cancel, job_id).as_dict()


@app.get("/api/diff/results/{job_id}")
def diff_results(job_id: str) -> Dict[str, Any]:
    job = _guard(diffs.get, job_id)
    if job.summary is None:
        raise HTTPException(status_code=409, detail="That analysis has not finished yet.")
    return job.summary


@app.get("/api/diff/examples/{job_id}/{column:path}")
def diff_examples(job_id: str, column: str, limit: int = diff_module.DEFAULT_EXAMPLE_ASSETS,
                  offset: int = 0, asset_id: Optional[str] = None) -> Dict[str, Any]:
    """Real rows behind one column's verdict, fetched only when someone looks."""
    job = _guard(diffs.get, job_id)
    dataset = _guard(engine.get, job.dataset_id)
    return _guard(diff_module.examples, engine, dataset, job.config, column,
                  limit, offset, asset_id)


@app.post("/api/diff/export/{job_id}")
def export_diff(job_id: str, request: DiffExportRequest) -> Dict[str, Any]:
    job = _guard(diffs.get, job_id)
    if job.summary is None:
        raise HTTPException(status_code=409, detail="That analysis has not finished yet.")
    if request.format not in ("xlsx", "csv"):
        raise HTTPException(status_code=400,
                            detail="Unsupported export format: " + request.format)
    export = _guard(exports.start_diff, job.dataset_id, job.summary, job.config,
                    request.format, request.include_manifest,
                    request.sheet_name or "Difference analysis")
    return export.as_dict()


@app.post("/api/export")
def start_export(request: ExportRequest) -> Dict[str, Any]:
    if request.format not in ("xlsx", "csv", "parquet", "json"):
        raise HTTPException(status_code=400, detail="Unsupported export format: " + request.format)
    job = _guard(
        exports.start, request.dataset_id, request.format, request.filters, request.columns,
        request.order_by, request.descending, request.row_limit, request.include_manifest,
        request.sheet_name or "Data", request.total_hint, request.transforms,
    )
    return job.as_dict()


@app.get("/api/export/{job_id}")
def export_status(job_id: str) -> Dict[str, Any]:
    return _guard(exports.get, job_id).as_dict()


@app.post("/api/export/{job_id}/cancel")
def cancel_export(job_id: str) -> Dict[str, Any]:
    return _guard(exports.cancel, job_id).as_dict()


@app.get("/api/export/{job_id}/download")
def download_export(job_id: str):
    job = _guard(exports.get, job_id)
    if job.status != "done" or not job.path or not os.path.exists(job.path):
        raise HTTPException(status_code=409, detail="That export is not ready yet.")
    media = {
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "csv": "text/csv",
        "parquet": "application/octet-stream",
        "json": "application/json",
        # A pivot too big for one workbook comes back as a zip of them.
        "zip": "application/zip",
    }[job.fmt]
    return FileResponse(job.path, media_type=media, filename=job.filename)


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
