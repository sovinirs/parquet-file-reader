# Parquet Studio

A local parquet reader built for files with millions of rows: browse the schema,
filter across as many columns as you like, build Excel-style pivot tables, and
export any of it to Excel, CSV, Parquet or JSON.

All processing is Python — [DuckDB](https://duckdb.org) reads the parquet file
**in place** and pushes filters and column projections down into it. Nothing is
loaded into memory up front, so a 3M-row file answers filter queries in
milliseconds and memory stays flat during export.

## Contents

- [Requirements](#requirements)
- [Running it](#running-it)
- [Opening a file](#opening-a-file)
- [Filtering](#filtering)
- [Pivoting](#pivoting)
- [Exporting](#exporting)
- [Theme](#theme)
- [API reference](#api-reference)
- [Testing](#testing)
- [Layout](#layout)
- [Notes](#notes)

## Requirements

Python 3.9+ and the packages in [requirements.txt](requirements.txt):

| Package | Used for |
| --- | --- |
| `duckdb` | the whole query engine — reads parquet in place, filters, pivots, exports |
| `fastapi` | the JSON API |
| `uvicorn[standard]` | the ASGI server |
| `XlsxWriter` | streaming, constant-memory `.xlsx` writing |
| `python-multipart` | file uploads (drag-and-drop / Browse) |

`./run.sh` installs these automatically into `.venv` — you don't need to install
anything by hand to get started.

## Running it

```bash
./run.sh
```

First run creates `.venv` and installs dependencies; then it opens
<http://127.0.0.1:8000>. Use `PORT=9000 ./run.sh` for a different port, or
`HOST=0.0.0.0 ./run.sh` to change the bind address (see the security note under
[Notes](#notes) before doing that).

No sample data yet? Generate one to try it against:

```bash
.venv/bin/python tests/make_sample.py   # 3M-row orders.parquet — filtering, pivoting, exporting
```

## Opening a file

Paste an **absolute path** into the box and hit Open. This is the path to use for
big files — it reads them where they sit, with no copying. You can also drag a
file onto the window (which copies it to a temp folder first), use **Browse…**, or
pick from Recent. Pointing at a *folder* opens every `.parquet` under it as one
dataset, so partitioned exports work.

### Unioning several files

**Union several files with the same columns…** under the path box stacks two or
more files into one table. Give each file its own row (type a path or use its
**Browse…**, and **+ Add another file** for more), then **Union and open**. The
result behaves exactly like a single file: filtering, pivoting and every export
format all work across it.

- Every file needs the same column names with the same types. Columns are matched
  by name, so their order may differ. If the files don't match, the panel says
  which file and which columns are the problem.
- Leave **Add a `source_file` column** ticked to tag each row with the name of the
  file it came from, so you can filter, pivot or export by file.
- A folder of parquet parts counts as one file in the list.

While a file opens, a loader lists what the backend is doing — finding the
files, reading the schema, checking for dates stored as text, counting rows — with
how long each step took. It only appears if opening takes longer than a moment,
and a dropped file shows its upload progress too.

## Filtering

Click any column — in the left rail or via the ▾ on its grid header — to open its
filter panel. What you get depends on the column's type:

| Tab | Available for | What it does |
| --- | --- | --- |
| **Values** | text, boolean, numbers, dates | Checklist of distinct values with row counts. Search to narrow, `All`/`None` to bulk-select, and switch between *Include selected* and *Exclude selected*. Paste a comma separated list (or a column copied from a spreadsheet) into the search box to tick exactly those values; any it can't find are listed. |
| **Range** | numbers, dates/timestamps | Min/max bounds, with the column's real min/max/avg/median shown above and a *Fill from data* shortcut. Tick *Exclude this range* to invert it. |
| **Condition** | everything | `contains`, `starts with`, `equals`, `>`, `≥`, `matches regex`, `is blank`, and so on. `is any of (list)` / `is none of (list)` take values separated by commas or new lines; pasting a list into the value box switches to them. Text matching is case-insensitive unless you tick *Match case*. |

### Extracting the year, month or day

A date or timestamp column's filter panel has an **Extract** row: *Full value*,
*Year*, *Month* or *Day*. Pick one and that column holds just that part, as a
whole number (month is 1–12), in the grid **and in every export**. The column
keeps its name; its type reads e.g. `year of TIMESTAMP`.

- The column's filter then works on the extracted numbers: the Values list shows
  the years (in calendar order), Range takes e.g. months 3 to 5, and a pasted list
  like `2023, 2024` works too. A filter set on the full date is cleared when you
  switch, since it no longer applies.
- Extraction is a Data view setting. The Pivot tab groups the raw values, but a
  filter set on an extracted part still means the same thing there ("year is
  2024").
- The Excel export's *Export info* sheet lists which columns were extracted.

**Dates stored as text.** When a file opens, the first 2,000 rows of every text
column (and every integer column, for `20240131`-style values) are checked
against common date layouts: `2024-01-31`, `31/01/2024`, `01/31/2024`,
`31-01-2024`, `31.01.2024`, `20240131`, `31-Jan-2024`, `Jan 31, 2024` and their
date-time variants. A column where every sampled value fits one gets the same
**Extract** row, and reads `VARCHAR · dates` in the Columns rail; every other
text column is left alone. The check first compares each value's rough shape
with each layout, so a column of names, codes or hashes is ruled out on its
first value, and only the columns that survive are parsed by DuckDB. On a
5.4 GB file with 40 text columns it adds about 0.2 seconds to opening.

- The panel says how the dates are being read, e.g. *read as DD/MM/YYYY*. When
  every sampled date fits both day-first and month-first (no day above 12), you
  choose which one from a dropdown; day-first is the default.
- `N/A`, `NULL`, `-` and blanks don't stop a column from being recognised. They,
  and any later value that doesn't fit the layout, extract as blank.

Filters on different columns are **ANDed together**, exactly like Excel's
autofilter. Two details that follow from that:

- The value list for a column reflects every *other* active filter, so you only
  ever see values that are still reachable. Counts update to match.
- Blank values appear as `(blank)` and are selectable, so "show me the rows
  missing a customer id" is just a checkbox.

Active filters show as chips above the grid — click one to edit it, `×` to drop
it, or **Clear all filters** to start over. The readout underneath always tells
you how many rows match out of the total.

Other controls: click a header to sort (asc → desc → off), change the sample size
with **Rows** (10 to 250), page through with `‹ ›`, and use the eye icon in the
rail to hide a column — hidden columns leave both the preview *and* the export.

Filters set here apply everywhere: the Pivot tab reads the same chips.

## Pivoting

The **Pivot** tab turns the filtered data into a cross-tab, the way an Excel
PivotTable does. Drag columns out of the left rail into the three wells — or use
**+ Add** on any of them:

| Well | What it does |
| --- | --- |
| **Rows** | Groups rows, one nesting level per field. Two fields give you `region › product`, with a subtotal row closing each region. |
| **Columns** | Spreads the measures across the top. Optional — leave it empty for a plain grouped summary. |
| **Values** | The measures: `Sum`, `Average`, `Min`, `Max`, `Count`, `Distinct count`, `Median`, `Std dev`, and `Count of rows`. Add the same column twice with different aggregations if you want. |

Click any field chip for its menu: change the aggregation, sort the rows by that
measure, move the field to another well, or drop it. **⇄ Swap** trades the Rows
and Columns fields, and the four toggles control subtotals, the per-row **Total**
column, the **Grand Total** row, and **Repeat labels**.

**Repeat labels** prints the outer row label on every row instead of only the
first of each group — Excel's *Repeat All Item Labels*. Leave it off to read the
hierarchy at a glance; turn it on when every row has to stand on its own, which
is what you want if the export is going to be sorted, filtered or pasted into
another sheet. It only changes how the rows are drawn, so it redraws instantly
without asking the server for the pivot again, and it carries into the Excel
export.

Every filter you have set still applies — the chips sit above both views — and the
readout tells you how many row and column groups came back.

Two things are worth knowing about the numbers:

- **Totals are computed from the raw rows, not from the cells above them.** The
  whole pivot, subtotals included, comes out of one `GROUP BY GROUPING SETS`
  query, so the Total under an *Average* column is the real average of that
  group — never an average of averages.
- The pivot itself is **one pass**, however deep it is — every cell and every
  subtotal comes out of a single `GROUP BY GROUPING SETS`. A small count query
  runs first to size the result (a few milliseconds; the file is already in
  DuckDB's buffer pool by the time the real query runs). A three-level pivot over
  3M rows comes back in tens of milliseconds.

A pivot needs a bounded shape to stay readable: more than 200 column groups is
refused (put the wide field in Rows instead). There is no ceiling on row groups.
Past 20,000 the screen shows the first 20,000 — but the readout always tells you
the exact number there are, the way the filter readout tells you how many rows
matched, and **Export to Excel** writes every one of them.

How that stays honest is worth a note, because a truncated pivot is easy to get
wrong. The row-group count comes from its own small `GROUP BY` before anything is
built, so it is exact rather than "20,000+". When the pivot is too large to hold
at once, the query is narrowed to the row groups that will be shown, and then:
the window stops on a whole outer group, so no subtotal covers half a group; and
the Grand Total is re-read from the whole filtered file, so it still means what
it says. Sorting by a measure is the one thing truncation costs you — there are
too many groups to rank them all, so it ranks the ones shown, and the readout
says so.

## Exporting

**Export** opens a drawer showing exactly what you're about to write out. Choose:

- **Excel (.xlsx)** — a formatted sheet with a frozen, styled header row and
  autofilter enabled. Real dates stay real dates and numbers stay numbers.
- **CSV** — much faster for very large results; written by DuckDB directly.
- **Parquet** (ZSTD-compressed) or **JSON**.

From the Pivot tab, **Export to Excel** writes the cross-tab itself: merged
column-group headers, repeat labels blanked (or repeated, per the toggle), bold
shaded subtotals and a ruled grand-total row — laid out the way Excel lays out a
PivotTable, with counts and values carrying their own number formats. The export
is not truncated: it writes every row group, however many the screen showed.

Past 50,000 row groups that is more than one workbook wants to hold, so the
export comes back as a **zip of several .xlsx files** instead. The split is made
on whole values of the first **Rows** field — never mid-group — so every subtotal
in every file covers its whole group, and no group appears in two files. Each
file's **Export info** sheet says which slice it holds (`customer_id =
CUST-000000 … CUST-013332`), warns that the total row at the bottom covers that
file alone, and carries the totals across all files so the real figures are never
more than one sheet away.

Both exports run in the background with a live progress bar and a working Cancel.
Two things worth knowing about Excel specifically:

- Excel's hard limit is **1,048,576 rows per sheet**. Larger exports are split
  automatically into `Data`, `Data (2)`, … and the UI warns you in advance. If
  you're exporting millions of rows, CSV is the better tool — Excel writing runs
  at roughly 17k rows/sec, so 1.8M rows takes about two minutes, while the same
  data as CSV takes under a second.
- The **Export info** sheet (on by default) records the source file, timestamp,
  row counts, and a plain-English list of every filter applied — so a workbook you
  send to someone else explains how it was produced. For a pivot it also records
  the fields in each well and the measures used.

## Theme

The interface follows EY's design language, taken from EY's own stylesheet
rather than from memory: `#1a1a24` dark and `#ffeb0a` yellow over the
`#f6f6fa` / `#eaeaf2` / `#c2c2cf` / `#747480` grey ramp, squared corners
(`border-radius: 0`), and the 4px yellow beam that marks whatever is active —
the selected tab, a filtered column, a field in a pivot well.
Both palettes are in `styles.css`; the ☾/☀ button in the top bar switches them,
and the masthead stays `#1a1a24` in both, as it does on ey.com.

Three deliberate departures:

- **EY's type scale is not used.** 48px headings at weight 200 with 60px margins
  belong on a marketing page, not above a grid showing 250 rows. The brand reads
  through colour, shape and the beam; the density stays.
- **Yellow never carries text on white.** `--accent-text` resolves to `#1a1a24`
  in the light theme, and links are dark text with a yellow underline instead.
  Yellow fills always take dark ink, never white — the contrast the other way
  round is unreadable, and EY's own stylesheet never does it.
- **The EY logo is not reproduced.** The brand mark is a yellow beam beside the
  app name — EY's colour and motif without copying a trademarked asset that this
  tool has no claim to. `EYInterstate` is asked for first in the font stack and
  used if the machine has it; it is licensed to EY and is not bundled here.

## API reference

The UI is a plain client of this JSON API — anything the browser does, a script
can do too. All endpoints live under `/api`; interactive docs are at
`/api/docs`. `dataset_id` comes back from `/api/open` and is required by
everything that queries a file.

| Method & path | What it does |
| --- | --- |
| `GET /api/recents` | Recently opened files (persisted to `~/.parquet-reader-recents.json`) |
| `GET /api/browse?path=` | List a directory's subfolders and `.parquet` files |
| `POST /api/open` | Open a file or folder by absolute path → dataset info (id, columns, row count) |
| `POST /api/union` | Open several files with the same columns as one table (`paths`, optional `source_column`) |
| `POST /api/open/start` | Open a file (`path`) or a union (`paths`) in the background → job id. The UI uses this to show its loader |
| `GET /api/open/{job_id}` | That job's steps so far, each with how long it took, and the dataset once it is done |
| `POST /api/upload` | Upload a `.parquet` file (drag-and-drop / Browse) → dataset info |
| `GET /api/dataset/{id}` | Re-fetch a previously opened dataset's info |
| `DELETE /api/dataset/{id}` | Close a dataset and free its handle |
| `POST /api/preview` | Filtered/sorted/paged rows |
| `POST /api/count` | Matching row count vs. total |
| `POST /api/values` | Distinct values + counts for one column's filter panel |
| `POST /api/stats` | Min/max/avg/median/distinct-approx for one column |
| `GET /api/pivot/aggregations` | The aggregations the Values well offers, and the pivot's size limits |
| `POST /api/pivot` | Build a pivot table (rows, columns, values, filters, sort, toggles) |
| `POST /api/pivot/export` | Start a background pivot → Excel export job |
| `POST /api/export` | Start a background row export job (xlsx/csv/parquet/json) |
| `GET /api/export/{job_id}` | Poll export progress |
| `POST /api/export/{job_id}/cancel` | Cancel a running export |
| `GET /api/export/{job_id}/download` | Download the finished file (or zip, for a split pivot export) |

Filter objects passed to any endpoint share one shape:
`{"column": "status", "op": "in", "values": ["open", "pending"]}` — see
[app/filters.py](app/filters.py) for the full set of operators (`eq`, `ne`,
`gt`, `gte`, `lt`, `lte`, `between`, `not_between`, `in`, `not_in`, `contains`,
`not_contains`, `starts_with`, `ends_with`, `regex`, `is_null`, `is_not_null`,
`is_empty`, `is_not_empty`).

## Testing

```bash
./tests/run.sh
```

Four suites, in this order:

| Suite | What it covers |
| --- | --- |
| `tests/test_units.py` | Every function in `app/`, called directly with no server, against a small fixture that has one of every column type the app handles (nulls, empty strings, decimals, booleans, dates, timestamps, times, lists, structs, dates stored as text and as `YYYYMMDD` integers, literal `%`/`_`). Expectations come from hand-written DuckDB SQL over the same file. Run it on its own with `.venv/bin/python -m unittest tests.test_units -v`. |
| `tests/test_api.py` | The HTTP API end to end on the 3M-row sample: every filter operator, pivot cells and totals, date extraction, text dates, unions and every export format, each cross-checked against SQL. |
| `tests/ui_selftest.html` | The real interface in headless Chrome, from opening a file through filtering, sorting, pivoting, exporting and downloading the result. |
| `tests/js_units.html` | `app.js`'s own helpers in headless Chrome: number and size formatting, pasted-list parsing, filter descriptions, date extraction and pivot state. |

The UI suites need Google Chrome; set `CHROME=/path/to/chrome` if it is not in
the default macOS location, or they are skipped.

## Layout

```
app/
  main.py          FastAPI routes; query endpoints run in a threadpool
  engine.py        DuckDB session, opening files and unions, previews, counts,
                   value lists, stats, and spotting dates stored as text
  filters.py       filter model → parameterised SQL, and date-part extraction
  pivot.py         cross-tabs: one GROUPING SETS query → cells, subtotals, totals
  exporter.py      background export jobs (streaming xlsx, native CSV/parquet/JSON,
                   and the pivot workbook writer)
  static/          the UI — no build step, no dependencies
tests/
  run.sh             runs every suite below
  test_units.py      unit tests for every function in app/
  test_api.py        API tests against the sample file
  ui_selftest.html   headless-Chrome UI walkthrough
  js_units.html      headless-Chrome checks of app.js's helpers
  make_sample.py     generates sample/orders.parquet
```

## Notes

- Column names and values never reach SQL as text: identifiers are validated
  against the file's real schema and quoted, and every value is a bound parameter.
- The server binds to `127.0.0.1` and is not authenticated — it's a local tool,
  and anything that can reach the port can read any parquet file the process can.
  Don't expose it on a shared network.
- Exported files land in a temp directory and are cleaned up after 6 hours.
