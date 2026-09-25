"""FastAPI application: static UI + JSON API over the DuckDB engine.

Query endpoints are declared with `def` (not `async def`) so Starlette runs them
in a worker thread -- a slow scan never blocks the event loop or the UI.
"""

import json
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
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
