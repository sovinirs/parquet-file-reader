# Parquet Studio

A local parquet reader built for files with millions of rows: browse the schema,
filter across as many columns as you like, build Excel-style pivot tables, run a
column-by-column difference analysis before de-duplicating, and export any of it
to Excel, CSV, Parquet or JSON.

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
- [Difference analysis](#difference-analysis)
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
.venv/bin/python tests/make_assets.py   # small fixed-asset file — built for the Difference tab
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
result behaves exactly like a single file: filtering, pivoting, difference
analysis and every export format all work across it.

- Every file needs the same column names with the same types. Columns are matched
  by name, so their order may differ. If the files don't match, the panel says
  which file and which columns are the problem.
- Leave **Add a `source_file` column** ticked to tag each row with the name of the
  file it came from, so you can filter, pivot or export by file.
- A folder of parquet parts counts as one file in the list.

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
- Extraction is a Data view setting. The Pivot and Difference tabs group and
  compare the raw values, but a filter set on an extracted part still means the
  same thing there ("year is 2024").
- The Excel export's *Export info* sheet lists which columns were extracted.

**Dates stored as text.** When a file opens, the first 2,000 rows of every text
column (and every integer column, for `20240131`-style values) are checked
against common date layouts: `2024-01-31`, `31/01/2024`, `01/31/2024`,
`31-01-2024`, `31.01.2024`, `20240131`, `31-Jan-2024`, `Jan 31, 2024` and their
date-time variants. A column where every sampled value fits one gets the same
**Extract** row, and reads `VARCHAR · dates` in the Columns rail.

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

Filters set here apply everywhere: the Pivot and Difference tabs both read the
same chips.

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

## Difference analysis

The **Difference** tab answers the question that comes before de-duplication:
*if I collapse these rows to one per key, what would I lose?*

The case it was built for is a SAP fixed-asset extract — one row per (asset,
depreciation area), where the business wants one row per asset. Asset ID alone
defines uniqueness, so every other column has to be checked: do the rows sharing
an asset agree, and if not, why not?

Setup is three numbered steps. Say which column identifies one thing (**One row
per**) and which column ought to explain any variation (**Differences explained
by**); tick the columns you want checked; choose how much of the file to cover.
Then press **Run analysis** — nothing runs on its own, because this reads the
file once per selected column and takes minutes on a large extract.

**The Columns rail on the left is the picker.** In the Difference tab every rail
row grows a tick box, and nothing starts ticked: you name the handful of columns
you care about rather than un-naming the sixty you don't. The key column is
marked `KEY` and cannot be ticked — it groups the rows, so it is not one of the
columns compared across them. **Suggested** ticks everything except the run
metadata and the period amounts that are *meant* to vary per area; **All** and
**Clear** do what they say. What is ticked also shows as a removable chip in step
2, so "what did I pick?" never means scrolling seventy rail rows.

Once a run finishes the setup folds into a one-line recap (`one row per asset_id
· explained by depr_area · 12 columns checked · every asset`) and the results
take the screen. **Change** reopens it.

For each column, per asset, it counts the **distinct non-blank values**. That one
number separates the cases that matter:

| Verdict | What it means | What to do when collapsing |
| --- | --- | --- |
| **Constant** | Every row of an asset agrees, with no blanks | Take any row |
| **Sparse** | One value, the other rows blank — they complement rather than contradict | `MAX`/`ANY_VALUE` |
| **Differs by area** | Values differ, but never *inside* one depreciation area | Needs a rule for which area wins |
| **Unexplained** | Values still differ within the same area | Needs a business rule — look at the examples |
| **All blank** | Nothing in any row | Drop it |
| **Error** | The column could not be analysed | Decide by hand |

**The headline is about the record, not any one column.** Above the table, a run
reports how many keys collapse to a single row once *every* checked column is
considered together — e.g. *420 of 500 keys (84%) hold a real conflict once all
6 checked columns are considered together*. That's a stricter question than any
single column's verdict: a key can have every individual column come back
Constant or Sparse and still fail to collapse, because different rows disagree
on *different* columns from each other. The number comes from packing every
checked column's value into one tuple per row and counting how many **distinct
tuples** each key produces — one tuple means the whole record already agrees, so
the key collapses with nothing lost; more than one means a real conflict lives
somewhere in the row, and the per-column breakdown underneath is what tells you
where. It's a domain-neutral question — "how many distinct versions of this
record exist" means the same thing whether the key is an asset, an order or a
customer — so it reads the same whether or not the dataset is a SAP extract.

Underneath that headline, a proportional track of the per-column verdicts and
the chips that filter by them break down *which* columns are behind the
conflicts. Clicking a segment isolates a pile. **Unexplained** is the one the
tab exists for, and the table sorts it to the top by default, widest problem
first.

Each row then carries the column, its verdict, a **split bar** — one proportional
bar showing how the keys divide between agreeing, complementing, disagreeing and
blank, with the exact counts on hover — the number of keys that disagree, and
what to do about it when collapsing. The bar replaced four percentage columns and
a distinct-value count: how much of a column disagrees is a proportion, and a
proportion reads faster as a length than as five numbers. The counts themselves
live in the drill-down, where they matter.

Some things worth knowing about how it gets there:

- **Blank means one thing.** Every value goes through
  `NULLIF(TRIM(CAST(x AS VARCHAR)), '')`, so a NULL number, an empty string and a
  cell of spaces are the same concept rather than three different values. Without
  that, a column of whitespace padding reads as a difference.
- **One column at a time.** Parquet is columnar, so each column's query touches
  only that column and the key. Unpivoting 70 columns would turn 100M rows into
  7 billion.
- **Empty columns are found first**, in a single pass with no grouping, and skip
  the expensive per-asset work entirely.
- **The grain is checked up front and reported on its own.** If an (asset,
  depreciation area) pair appears on more than one row, the extract is not at the
  grain the analysis assumes, and an "unexplained" difference may be duplicate
  rows rather than conflicting data. That finding sits above the table rather
  than being folded into the column results, because it changes how they read.
- **One bad column does not stop the run.** It is marked `ERROR` with the reason,
  and the other 69 carry on.

**Sample mode** runs over a random sample of *assets* (not rows — an asset's rows
only mean something together) for a fast preview before committing to the full
pass. Turn on **Only analyse the rows my filters match** to scope a run to
whatever the filter chips currently select.

Selecting a column opens the drill-down, which is the piece that makes this land
with a business reviewer: the counts behind the verdict, then **ten real assets**
per page, each with the distinct values named once and then every one of its rows,
with the cells that disagree highlighted. Page through more examples, or type an
asset ID to jump straight to a case someone has asked about.

**Export** writes a workbook with a summary sheet (one row per column, every
metric, colour-coded by verdict as conditional formatting), an **Examples** sheet
carrying **ten real key values for every differing column** — with each one's full
set of rows, the disagreeing cells highlighted, and a distinct-value count — and a
sheet listing every column that never got a verdict and why. The **Export info**
manifest records the key column, the columns you selected, whether sample mode was
on, how deep the examples go, the duplicate-grain finding, and the same whole-record
consistency figures as the on-screen headline — so the workbook explains how it was
produced. CSV gives the summary table alone, with no examples.

Each column's **"what to do when collapsing"** text names whichever column you
actually picked as "Differences explained by" — "requires a business rule for
which `depr_area` wins," not a fixed reference to depreciation area — so it reads
correctly whatever the explanatory column is actually called, or if you leave
it unset.

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
the selected tab, a filtered column, a field in a pivot well, a warning finding.
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

The verdict colours in the Difference tab map onto EY's semantic palette —
`#1eca3a` agrees, `#21acf6` complements, brand yellow means the depreciation
area explains it, `#a11c1c` means it does not — and the exported workbook uses
the same colours, so a sheet in someone's inbox matches the screen it came from.

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
| `GET /api/diff/defaults` | Suggested column set, verdict list, recommendations text, and the example page size |
| `POST /api/diff/start` | Start a difference-analysis run → job id. `include` is the ticked column set; omit it to analyse every column |
| `GET /api/diff/status/{job_id}` | Poll progress; includes every column finished so far |
| `POST /api/diff/cancel/{job_id}` | Cancel a running analysis |
| `GET /api/diff/results/{job_id}` | The finished run's full summary |
| `GET /api/diff/examples/{job_id}/{column}` | Drill-down: real rows behind one column's verdict |
| `POST /api/diff/export/{job_id}` | Export a finished analysis to Excel or CSV |
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

Runs 126 API checks — every filter operator, every pivot cell, subtotal and grand
total, and every difference-analysis verdict, each cross-checked against the same
question asked of DuckDB in hand-written SQL — plus 68 UI checks that drive the
real interface in headless Chrome, from opening a file through filtering,
sorting, pivoting, exporting and downloading the result, and 35 more that drive
the Difference tab end to end against a purpose-built fixture.

`tests/make_assets.py` generates that fixture: a small fixed-asset file where
every column has exactly one defensible verdict, including a duplicated (asset,
depreciation area) pair. Two of its columns differ in ways that look identical
until you check within a depreciation area — which is precisely what the
attribution pass has to get right.

## Layout

```
app/
  main.py          FastAPI routes; query endpoints run in a threadpool
  engine.py        DuckDB session, previews, counts, value lists, stats
  filters.py       filter model → parameterised SQL
  pivot.py         cross-tabs: one GROUPING SETS query → cells, subtotals, totals
  diffanalysis.py  per-column agreement across rows sharing a key, and why
  exporter.py      background export jobs (streaming xlsx, native CSV/parquet,
                   the pivot workbook writer, and the diff-analysis writer)
  static/          the UI — no build step, no dependencies
tests/
  test_api.py        API + filter/pivot/diff-semantics tests
  ui_selftest.html    headless-Chrome UI walkthrough
  diff_selftest.html  headless-Chrome walkthrough of the Difference tab
  make_sample.py      generates sample/orders.parquet
  make_assets.py      generates sample/assets.parquet (the diff fixture)
```

## Notes

- Column names and values never reach SQL as text: identifiers are validated
  against the file's real schema and quoted, and every value is a bound parameter.
- The server binds to `127.0.0.1` and is not authenticated — it's a local tool,
  and anything that can reach the port can read any parquet file the process can.
  Don't expose it on a shared network.
- Exported files land in a temp directory and are cleaned up after 6 hours.
