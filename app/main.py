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
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import exporter as exporter_module
from . import pivot as pivot_module
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


class OpenStartRequest(BaseModel):
    # One of: `path` (a file or folder), or `paths` for a union.
    path: Optional[str] = None
    paths: Optional[List[str]] = None
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


# ----------------------------------------------------------------- open jobs

@dataclass
class StepJob:
    """Work done in the background -- opening a file, building a pivot -- that
    records each step as it goes, so the UI can say what is happening."""

    id: str
    label: str
    result_key: str = "result"       # where as_dict puts the finished result
    status: str = "running"          # running | done | error
    steps: List[Dict[str, Any]] = field(default_factory=list)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started: float = field(default_factory=time.time)
    finished: Optional[float] = None

    def step(self, message: str) -> None:
        now = time.time()
        if self.steps:
            self.steps[-1]["seconds"] = round(now - self.steps[-1]["at"], 3)
        self.steps.append({"message": message, "at": now, "seconds": None})

    def finish(self) -> None:
        self.finished = time.time()
        if self.steps and self.steps[-1]["seconds"] is None:
            self.steps[-1]["seconds"] = round(self.finished - self.steps[-1]["at"], 3)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "status": self.status,
            "steps": [{"message": s["message"], "seconds": s["seconds"]} for s in self.steps],
            self.result_key: self.result,
            "error": self.error,
            "elapsed": round((self.finished or time.time()) - self.started, 2),
        }


def _remember_job(jobs: Dict[str, StepJob], job: StepJob, keep: int) -> None:
    """Track a new job, forgetting the oldest finished ones beyond `keep`."""
    jobs[job.id] = job
    for stale in sorted(jobs.values(), key=lambda j: j.started)[:max(0, len(jobs) - keep)]:
        if stale.status != "running":
            jobs.pop(stale.id, None)


open_jobs: Dict[str, StepJob] = {}
MAX_OPEN_JOBS = 20
pivot_jobs: Dict[str, StepJob] = {}
# A finished pivot holds its whole result (megabytes for a big one), so only a few are kept.
MAX_PIVOT_JOBS = 6


def _run_open(job: StepJob, request: "OpenStartRequest") -> None:
    try:
        if request.paths:
            dataset = _guard(engine.open_union, request.paths, request.source_column, job.step)
        else:
            dataset = _guard(engine.open, request.path or "", progress=job.step)
            _remember(dataset.path, dataset.display_name, dataset.row_count)
        job.step("Ready")
        job.result = dataset.as_dict()
        job.status = "done"
    except HTTPException as exc:
        job.error = exc.detail
        job.status = "error"
    finally:
        job.finish()


def _run_pivot(job: StepJob, request: "PivotRequest") -> None:
    try:
        dataset = _guard(engine.get, request.dataset_id)
        job.result = _guard(
            pivot_module.compute, engine, dataset, request.rows, request.columns,
            [v.model_dump() for v in request.values], request.filters,
            request.subtotals, request.row_totals, request.column_totals,
            request.sort, max(1, int(request.max_rows)), progress=job.step,
        )
        job.status = "done"
    except HTTPException as exc:
        job.error = exc.detail
        job.status = "error"
    finally:
        job.finish()


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


def _json(payload: Dict[str, Any]) -> JSONResponse:
    """Send an already JSON-safe result as it is.

    Returning a dict makes FastAPI walk every value to re-encode it, which for
    a 20,000-row pivot costs as much as building the pivot did. The engine and
    the pivot already coerce every value with to_jsonable, so skip that pass.
    """
    return JSONResponse(payload)


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


@app.post("/api/open/start")
def start_open(request: OpenStartRequest) -> Dict[str, Any]:
    """Open a file (or a union) in the background; poll /api/open/{id} for its steps."""
    if not request.paths and not (request.path or "").strip():
        raise HTTPException(status_code=400, detail="Give a path to open.")
    label = (os.path.basename((request.path or "").rstrip(os.sep)) if not request.paths
             else "{} files".format(len([p for p in request.paths if p.strip()])))
    job = StepJob(id=uuid.uuid4().hex[:12], label=label or "file", result_key="dataset")
    _remember_job(open_jobs, job, MAX_OPEN_JOBS)
    threading.Thread(target=_run_open, args=(job, request), daemon=True).start()
    return job.as_dict()


@app.get("/api/open/{job_id}")
def open_status(job_id: str) -> Dict[str, Any]:
    job = open_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown open job")
    return job.as_dict()


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
def values(request: ValuesRequest) -> JSONResponse:
    dataset = _guard(engine.get, request.dataset_id)
    return _json(_guard(
        engine.distinct_values, dataset, request.column, request.filters,
        request.search, max(1, min(int(request.limit), 2000)), request.exact,
        request.transform, request.date_format,
    ))


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
def build_pivot(request: PivotRequest) -> JSONResponse:
    dataset = _guard(engine.get, request.dataset_id)
    return _json(_guard(
        pivot_module.compute, engine, dataset, request.rows, request.columns,
        [v.model_dump() for v in request.values], request.filters,
        request.subtotals, request.row_totals, request.column_totals,
        request.sort, max(1, int(request.max_rows)),
    ))


@app.post("/api/pivot/start")
def start_pivot(request: PivotRequest) -> Dict[str, Any]:
    """Build a pivot in the background; poll /api/pivot/job/{id} for its steps."""
    _guard(engine.get, request.dataset_id)
    job = StepJob(id=uuid.uuid4().hex[:12], label=" › ".join(request.rows + request.columns) or "pivot")
    _remember_job(pivot_jobs, job, MAX_PIVOT_JOBS)
    threading.Thread(target=_run_pivot, args=(job, request), daemon=True).start()
    return job.as_dict()


@app.get("/api/pivot/job/{job_id}")
def pivot_job(job_id: str) -> JSONResponse:
    job = pivot_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown pivot job")
    return _json(job.as_dict())


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
