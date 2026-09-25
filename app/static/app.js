/* ============================================================
   Parquet Studio — front end.

   The browser holds no data: every filter change is a round trip to DuckDB,
   which returns a fresh page of sample rows plus the matched-row count. That
   keeps memory flat whether the file has ten rows or a hundred million.
   ============================================================ */

'use strict';

const $ = (id) => document.getElementById(id);
const NULL_LABEL = '(blank)';

/* ─────────────────────────── state ─────────────────────────── */

const state = {
  dataset: null,
  mode: 'data',         // 'data' (row grid) | 'pivot' (cross-tab)
  filters: {},          // column name -> filter spec
  transforms: {},       // column name -> 'year' | 'month' | 'day' extracted in the grid + export
  dateFormats: {},      // column name -> how a text column's dates are read (see DATE_FORMATS on the server)
  hidden: new Set(),    // columns excluded from preview + export
  sort: null,           // { column, descending }
  pageSize: 10,
  page: 0,
  matched: null,        // matched row count, null while unknown
  counting: false,
  lastMs: 0,
  columnFilter: '',
  exportJob: null,
  pollTimer: null,
  reqToken: 0,
  pivot: newPivotState(),
};

/* Rows and Columns hold plain column names; Values holds { column, agg }. */
function newPivotState() {
  return {
    rows: [],
    columns: [],
    values: [],
    subtotals: true,
    rowTotals: true,
    columnTotals: true,
    repeatLabels: false,
    sort: null,          // { by: 'value', value_index, descending }
    result: null,
    error: null,
    ms: 0,
    token: 0,
  };
}

/* ─────────────────────────── helpers ─────────────────────────── */

const nf = new Intl.NumberFormat();
const fmtNum = (n) => (n === null || n === undefined ? '—' : nf.format(n));

function fmtBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const value = bytes / Math.pow(1024, i);
  return `${value >= 100 || i === 0 ? Math.round(value) : value.toFixed(1)} ${units[i]}`;
}

function fmtDuration(seconds) {
  if (seconds < 60) return `${seconds.toFixed(0)}s`;
  const m = Math.floor(seconds / 60);
  return `${m}m ${Math.round(seconds - m * 60)}s`;
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = text;
  return node;
}

function debounce(fn, ms) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

function toast(message, kind = 'info', ms = 4200) {
  const node = el('div', `toast ${kind}`, message);
  $('toasts').appendChild(node);
  setTimeout(() => node.remove(), ms);
}

async function api(path, body, method = 'POST') {
  const options = { method, headers: { 'Content-Type': 'application/json' } };
  if (body !== undefined) options.body = JSON.stringify(body);
  if (method === 'GET') delete options.body;
  const response = await fetch(path, options);
  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (_) { /* non-JSON error page */ }
  if (!response.ok) {
    throw new Error((data && data.detail) || text || `Request failed (${response.status})`);
  }
  return data;
}

const TYPE_ABBR = { numeric: '#', text: 'Ab', temporal: '◷', boolean: '⊤', complex: '{}', other: '?' };

/* ─────────────────────────── filter descriptions ─────────────────────────── */

const OP_WORDS = {
  in: 'is any of', not_in: 'is none of',
  eq: '=', ne: '≠', gt: '>', gte: '≥', lt: '<', lte: '≤',
  between: 'between', not_between: 'not between',
  contains: 'contains', not_contains: 'does not contain',
  starts_with: 'starts with', ends_with: 'ends with', regex: 'matches',
  is_null: 'is blank', is_not_null: 'is not blank',
  is_empty: 'is empty', is_not_empty: 'is not empty',
};

function valueLabel(value) {
  if (value === null || value === undefined) return NULL_LABEL;
  if (value === '') return '(empty)';
  return String(value);
}

function describeFilter(spec) {
  const op = spec.op || 'in';
  if (['is_null', 'is_not_null', 'is_empty', 'is_not_empty'].includes(op)) {
    return { op: OP_WORDS[op], value: '' };
  }
  if (op === 'in' || op === 'not_in') {
    const values = spec.values || [];
    const shown = values.slice(0, 3).map(valueLabel).join(', ');
    const extra = values.length > 3 ? ` +${values.length - 3}` : '';
    return { op: OP_WORDS[op], value: shown + extra };
  }
  if (op === 'between' || op === 'not_between') {
    const lo = spec.value === '' || spec.value === null ? '−∞' : spec.value;
    const hi = spec.value2 === '' || spec.value2 === null ? '∞' : spec.value2;
    return { op: OP_WORDS[op], value: `${lo} … ${hi}` };
  }
  return { op: OP_WORDS[op] || op, value: String(spec.value ?? '') };
}

const activeFilters = () => Object.values(state.filters);

/* ─────────────────────────── extracting date parts ─────────────────────────── */

const EXTRACT_PARTS = [['year', 'Year'], ['month', 'Month'], ['day', 'Day']];

const isRealDate = (column) => column.category === 'temporal' && /^(DATE|TIMESTAMP)/i.test(column.type);
// Text (or YYYYMMDD integer) columns the server found dates in when the file opened.
const isTextDate = (column) => !isRealDate(column) && Boolean(column.date_formats && column.date_formats.length);
// Only calendar values have a year, month and day -- not TIME or INTERVAL.
const canExtract = (column) => isRealDate(column) || isTextDate(column);

const rawColumn = (name) => state.dataset.columns.find((c) => c.name === name);

const dateFormatOf = (column) => state.dateFormats[column.name] || column.date_formats[0];

// '%d/%m/%Y' -> 'DD/MM/YYYY'; mirrors format_label() on the server.
function formatLabel(format) {
  if (format === 'iso') return 'YYYY-MM-DD';
  return [['%Y', 'YYYY'], ['%y', 'YY'], ['%m', 'MM'], ['%d', 'DD'], ['%H', 'hh'], ['%M', 'mm'],
    ['%S', 'ss'], ['%B', 'Month'], ['%b', 'Mon']]
    .reduce((text, [token, label]) => text.split(token).join(label), format);
}

/* The column as the Data view presents it: an extracted date column is a plain
   integer column (2023, 2024…), so its filter panel offers number tools. */
function effectiveColumn(column) {
  const raw = rawColumn(column.name) || column;
  const part = state.transforms[raw.name];
  if (!part) return raw;
  return {
    ...raw, type: 'BIGINT', category: 'numeric', transform: part, rawType: raw.type,
    dateFormat: isTextDate(raw) ? dateFormatOf(raw) : null,
  };
}

// What the server needs to extract: a bare part for a real date column, the
// part plus how to read it for a text one.
function transformsPayload() {
  const out = {};
  for (const [name, part] of Object.entries(state.transforms)) {
    const raw = rawColumn(name);
    out[name] = raw && isTextDate(raw) ? { part, format: dateFormatOf(raw) } : part;
  }
  return out;
}

function typeLabel(name, fallback) {
  const part = state.transforms[name];
  const raw = rawColumn(name);
  if (!part || !raw) return fallback;
  return isTextDate(raw) ? `${part} of ${raw.type} (${formatLabel(dateFormatOf(raw))})` : `${part} of ${raw.type}`;
}
function setExtract(name, part) {
  const previous = state.transforms[name] || null;
  part = part || null;
  if (part === previous) return;
  if (part) state.transforms[name] = part;
  else delete state.transforms[name];

  // The old filter was on the other shape of the column (dates vs years), so
  // it no longer means anything.
  const spec = state.filters[name];
  const dropped = Boolean(spec && (spec.transform || null) !== part);
  if (dropped) {
    delete state.filters[name];
    toast(`Cleared the filter on ${name} — it was set on ${previous ? `the ${previous}` : 'the full value'}.`);
  }
  state.page = 0;
  renderChips();
  renderRail();
  refresh({ countUnchanged: !dropped });
  renderExportSummary();
  if (pop.column && pop.column.name === name) openFilterPopover(rawColumn(name), pop.anchor);
}

function setDateFormat(name, format) {
  const raw = rawColumn(name);
  if (!raw || dateFormatOf(raw) === format) return;
  state.dateFormats[name] = format;
  // A filter on the extracted part read 03/04 one way; the other way it
  // picks different rows, so it goes.
  const spec = state.filters[name];
  const dropped = Boolean(spec && spec.date_format && spec.date_format !== format);
  if (dropped) {
    delete state.filters[name];
    toast(`Cleared the filter on ${name} — it read the dates as ${formatLabel(spec.date_format)}.`);
  }
  if (state.transforms[name] || dropped) {
    state.page = 0;
    renderChips();
    renderRail();
    refresh({ countUnchanged: !dropped });
    renderExportSummary();
  }
  if (pop.column && pop.column.name === name) openFilterPopover(raw, pop.anchor);
}

function renderExtractBar(column) {
  const bar = $('pop-extract');
  bar.innerHTML = '';
  const raw = rawColumn(column.name);
  // Extraction shapes the Data grid and its export, so it is set from there.
  bar.hidden = !raw || !canExtract(raw) || state.mode !== 'data';
  if (bar.hidden) return;
  bar.appendChild(el('span', 'pop-extract-label', 'Extract'));
  const group = el('div', 'seg');
  for (const [part, label] of [['', 'Full value'], ...EXTRACT_PARTS]) {
    const active = (state.transforms[raw.name] || '') === part;
    const button = el('button', `seg-btn${active ? ' active' : ''}`, label);
    button.title = part
      ? `Show and export only the ${part} of ${raw.name}, as a number`
      : `Show and export ${raw.name} as it is`;
    button.onclick = () => setExtract(raw.name, part);
    group.appendChild(button);
  }
  bar.appendChild(group);

  if (isTextDate(raw)) {
    // The dates are text, so say how they are being read -- and when the
    // sample fits more than one layout (no day above 12), let the user pick.
    const line = el('div', 'pop-extract-format');
    line.appendChild(el('span', null, `Dates stored as ${raw.type.toLowerCase()}, read as `));
    const current = dateFormatOf(raw);
    if (raw.date_formats.length > 1) {
      const select = el('select', 'input small');
      for (const format of raw.date_formats) {
        const option = el('option', null, formatLabel(format));
        option.value = format;
        select.appendChild(option);
      }
      select.value = current;
      select.onchange = () => setDateFormat(raw.name, select.value);
      line.appendChild(select);
      line.title = 'Every sampled date fits more than one layout. Pick the one this file uses.';
    } else {
      line.appendChild(el('b', null, formatLabel(current)));
    }
    bar.appendChild(line);
  }
}
const visibleColumns = () =>
  state.dataset.columns.filter((c) => !state.hidden.has(c.name)).map((c) => c.name);

/* ─────────────────────────── opening a file ─────────────────────────── */

async function openPath(path) {
  if (!path || !path.trim()) return;
  setOpenError('');
  $('btn-open-path').disabled = true;
  try {
    const dataset = await api('/api/open', { path: path.trim() });
    mountDataset(dataset);
  } catch (error) {
    setOpenError(error.message);
  } finally {
    $('btn-open-path').disabled = false;
  }
}

async function uploadFile(file) {
  if (!file.name.toLowerCase().endsWith('.parquet')) {
    setOpenError('That is not a .parquet file.');
    return;
  }
  setOpenError('');
  const form = new FormData();
  form.append('file', file);
  toast(`Copying ${file.name} (${fmtBytes(file.size)})…`);
  try {
    const response = await fetch('/api/upload', { method: 'POST', body: form });
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || 'Upload failed');
    mountDataset(data);
  } catch (error) {
    setOpenError(error.message);
  }
}

/* -- union -- */

function unionRow(value = '') {
  const item = el('li', 'union-row');
  const input = el('input', 'input mono');
  input.type = 'text';
  input.spellcheck = false;
  input.placeholder = '/absolute/path/to/file.parquet';
  input.value = value;
  input.onkeydown = (event) => { if (event.key === 'Enter') openUnion(); };
  const browse = el('button', 'btn', 'Browse…');
  browse.onclick = () => openBrowser(browseCurrent, input);
  const remove = el('button', 'btn icon ghost', '×');
  remove.title = 'Remove this file';
  remove.onclick = () => {
    item.remove();
    syncUnionRows();
  };
  item.append(input, browse, remove);
  return item;
}

function syncUnionRows() {
  const rows = [...$('union-list').children];
  // Two files is the least a union can be, so the first two can't be removed.
  rows.forEach((row) => { row.querySelector('.icon').style.visibility = rows.length > 2 ? '' : 'hidden'; });
}

function toggleUnion(show) {
  const panel = $('union-panel');
  panel.hidden = !show;
  $('btn-union-toggle').hidden = show;
  if (show && !$('union-list').children.length) {
    // Start from whatever is already in the main path box.
    $('union-list').append(unionRow($('path-input').value.trim()), unionRow());
    syncUnionRows();
  }
  if (show) {
    const empty = [...$('union-list').querySelectorAll('input')].find((input) => !input.value);
    (empty || $('union-list').querySelector('input')).focus();
  }
}

async function openUnion() {
  const paths = [...$('union-list').querySelectorAll('input')]
    .map((input) => input.value.trim()).filter(Boolean);
  if (paths.length < 2) {
    setUnionError('Enter at least two files to union.');
    return;
  }
  setUnionError('');
  $('btn-union-open').disabled = true;
  try {
    const dataset = await api('/api/union', { paths, source_column: $('union-source').checked });
    mountDataset(dataset);
  } catch (error) {
    setUnionError(error.message);
  } finally {
    $('btn-union-open').disabled = false;
  }
}

// Shown inside the panel, next to the button that caused it.
function setUnionError(message) {
  const node = $('union-error');
  node.textContent = message || '';
  node.hidden = !message;
}

function setOpenError(message) {
  const node = $('open-error');
  node.textContent = message || '';
  node.hidden = !message;
}

function mountDataset(dataset) {
  state.dataset = dataset;
  state.filters = {};
  state.transforms = {};
  state.dateFormats = {};
  state.hidden = new Set();
  state.sort = null;
  state.page = 0;
  state.matched = dataset.row_count;
  state.pivot = newPivotState();
  syncPivotOptions();

  $('welcome').hidden = true;
  $('workspace').hidden = false;
  $('file-meta').hidden = false;
  $('btn-export').hidden = false;
  $('btn-change-file').hidden = false;

  $('file-name').textContent = dataset.name;
  $('file-name').title = dataset.path;
  const parts = [
    `${fmtNum(dataset.row_count)} rows`,
    `${dataset.columns.length} columns`,
    fmtBytes(dataset.file_size),
  ];
  if (dataset.file_count > 1) parts.push(`${dataset.file_count} files`);
  $('file-facts').textContent = parts.join(' · ');
  $('column-count').textContent = dataset.columns.length;

  applyMode('data');
  renderRail();
  renderChips();
  renderWells();
  refresh();
}

function unmountDataset() {
  closePopover();
  closeMenu();
  if (state.dataset) {
    // Releases the handle and deletes the temp copy if the file was uploaded.
    api(`/api/dataset/${state.dataset.id}`, undefined, 'DELETE').catch(() => {});
  }
  state.dataset = null;
  $('workspace').hidden = true;
  $('welcome').hidden = false;
  $('file-meta').hidden = true;
  $('btn-export').hidden = true;
  $('btn-change-file').hidden = true;
  loadRecents();
}

/* ─────────────────────────── columns rail ─────────────────────────── */

function renderRail() {
  const list = $('column-list');
  list.innerHTML = '';
  const needle = state.columnFilter.toLowerCase();
  $('column-count').textContent = state.dataset.columns.length;

  for (const column of state.dataset.columns) {
    if (needle && !column.name.toLowerCase().includes(needle)) continue;

    const item = el('li', 'col-item');
    const isHidden = state.hidden.has(column.name);
    const isFiltered = Boolean(state.filters[column.name]);
    if (isFiltered) item.classList.add('filtered');
    if (isHidden) item.classList.add('hidden-col');

    const badge = el('span', 'type-badge', TYPE_ABBR[column.category] || '?');
    badge.dataset.cat = column.category;
    badge.title = column.type;

    const body = el('div', 'col-body');
    body.appendChild(el('div', 'col-name', column.name));
    // Extraction is a Data view setting; the Pivot reads raw values.
    let shownType = state.mode === 'data' ? typeLabel(column.name, column.type) : column.type;
    // Point out the text columns that hold dates, since that is not obvious.
    if (state.mode === 'data' && shownType === column.type && isTextDate(column)) shownType += ' · dates';
    const extractedHere = state.mode === 'data' && Boolean(state.transforms[column.name]);
    body.appendChild(el('div', `col-type${extractedHere ? ' extracted' : ''}`, shownType));
    body.title = `${column.name} · ${shownType}`;

    const actions = el('div', 'col-actions');

    const filterBtn = el('button', `icon-btn${isFiltered ? ' on' : ''}`, '⌄');
    filterBtn.title = isFiltered ? 'Edit filter' : 'Filter this column';
    filterBtn.onclick = (event) => {
      event.stopPropagation();
      openFilterPopover(column, filterBtn);
    };
    actions.appendChild(filterBtn);

    const eyeBtn = el('button', 'icon-btn', isHidden ? '𝅘' : '◉');
    eyeBtn.title = isHidden ? 'Show column' : 'Hide column from preview and export';
    eyeBtn.onclick = (event) => {
      event.stopPropagation();
      if (isHidden) state.hidden.delete(column.name);
      else state.hidden.add(column.name);
      renderRail();
      renderExportColumns();
      refresh({ countUnchanged: true });
    };
    actions.appendChild(eyeBtn);

    item.append(badge, body, actions);
    item.onclick = () => openFilterPopover(column, item);

    // Rail columns are drag sources for the pivot wells.
    item.draggable = true;
    item.addEventListener('dragstart', (event) => {
      dragPayload = { kind: 'column', name: column.name };
      event.dataTransfer.effectAllowed = 'copy';
      event.dataTransfer.setData('text/plain', column.name);
    });
    item.addEventListener('dragend', () => { dragPayload = null; clearDropMarkers(); });

    list.appendChild(item);
  }

  if (!list.children.length) {
    list.appendChild(el('li', 'hint', 'No column matches that search.'));
  }
}

/* ─────────────────────────── filter chips ─────────────────────────── */

function renderChips() {
  const chips = $('chips');
  chips.innerHTML = '';
  const specs = activeFilters();

  $('btn-clear-filters').hidden = specs.length === 0;
  if (!specs.length) {
    chips.appendChild(el('span', 'chips-empty', 'No filters — showing the whole file. Click any column to filter it.'));
    return;
  }

  for (const spec of specs) {
    const chip = el('span', `chip${spec.enabled === false ? ' off' : ''}`);
    const { op, value } = describeFilter(spec);

    const label = el('span', 'chip-label');
    const subject = spec.transform ? `${spec.column} (${spec.transform})` : spec.column;
    label.appendChild(Object.assign(el('b'), { textContent: subject }));
    label.appendChild(Object.assign(el('i'), { textContent: ` ${op} ` }));
    label.appendChild(document.createTextNode(value));
    label.title = `${subject} ${op} ${value}\nClick to edit`;
    label.onclick = () => {
      const column = state.dataset.columns.find((c) => c.name === spec.column);
      if (column) openFilterPopover(column, chip);
    };

    const remove = el('button', 'chip-x', '×');
    remove.title = 'Remove this filter';
    remove.onclick = () => {
      delete state.filters[spec.column];
      renderChips();
      renderRail();
      refresh();
    };

    chip.append(label, remove);
    chips.appendChild(chip);
  }
}

/* ─────────────────────────── data grid ─────────────────────────── */

async function refresh({ countUnchanged = false } = {}) {
  if (!state.dataset) return;
  const token = ++state.reqToken;
  const filters = activeFilters();
  const columns = visibleColumns();
  const started = performance.now();

  if (!countUnchanged) {
    state.matched = null;
    state.counting = true;
    renderReadout();
    api('/api/count', { dataset_id: state.dataset.id, filters })
      .then((data) => {
        if (token !== state.reqToken) return;
        state.matched = data.count;
        state.counting = false;
        renderReadout();
        renderPivotReadout();
        renderExportSummary();
      })
      .catch((error) => {
        if (token !== state.reqToken) return;
        state.counting = false;
        renderReadout();
        toast(error.message, 'error');
      });
  }

  if (state.mode === 'pivot') {
    // The cross-tab is its own aggregate query; the row grid stays untouched
    // until the Data view is shown again.
    runPivot();
    return;
  }

  $('table-overlay').hidden = false;
  try {
    const data = await api('/api/preview', {
      dataset_id: state.dataset.id,
      filters,
      columns,
      limit: state.pageSize,
      offset: state.page * state.pageSize,
      order_by: state.sort ? state.sort.column : null,
      descending: state.sort ? state.sort.descending : false,
      transforms: transformsPayload(),
    });
    if (token !== state.reqToken) return;
    state.lastMs = performance.now() - started;
    renderGrid(data);
    renderReadout();
    renderStatus(data.rows.length);
  } catch (error) {
    if (token !== state.reqToken) return;
    toast(error.message, 'error');
    renderStatus(0);
  } finally {
    if (token === state.reqToken) $('table-overlay').hidden = true;
  }
}

function renderGrid(data) {
  const head = $('grid-head');
  const body = $('grid-body');
  head.innerHTML = '';
  body.innerHTML = '';

  const headRow = el('tr');
  const corner = el('th', 'row-num-head');
  corner.appendChild(el('div', 'th-inner'));
  headRow.appendChild(corner);

  for (const column of data.columns) {
    const th = el('th');
    const isFiltered = Boolean(state.filters[column.name]);
    if (isFiltered) th.classList.add('is-filtered');

    const inner = el('div', 'th-inner');
    const label = el('div', 'th-label');
    const nameRow = el('div', 'th-name', column.name);
    if (state.sort && state.sort.column === column.name) {
      nameRow.appendChild(el('span', 'th-sort', state.sort.descending ? ' ▼' : ' ▲'));
    }
    const extracted = Boolean(state.transforms[column.name]);
    const shownType = typeLabel(column.name, column.type);
    label.append(nameRow, el('div', `th-type${extracted ? ' extracted' : ''}`, shownType));
    label.title = `${column.name} · ${shownType}\nClick to sort`;
    label.onclick = () => cycleSort(column.name);

    const filterBtn = el('button', `th-filter${isFiltered ? ' active' : ''}`, '▼');
    filterBtn.dataset.column = column.name;
    filterBtn.title = isFiltered ? 'Edit filter' : 'Filter this column';
    filterBtn.onclick = (event) => {
      event.stopPropagation();
      openFilterPopover(column, filterBtn);
    };

    inner.append(label, filterBtn);
    th.appendChild(inner);
    headRow.appendChild(th);
  }
  head.appendChild(headRow);

  const startIndex = state.page * state.pageSize;
  data.rows.forEach((row, rowIndex) => {
    const tr = el('tr');
    tr.appendChild(el('td', 'row-num', fmtNum(startIndex + rowIndex + 1)));
    row.forEach((value, columnIndex) => {
      const column = data.columns[columnIndex];
      const td = el('td', column.category === 'numeric' ? 'num' : column.category);
      if (value === null || value === undefined) {
        td.appendChild(el('span', 'null-tag', 'null'));
      } else if (typeof value === 'object') {
        const text = JSON.stringify(value);
        td.textContent = text;
        td.title = text;
      } else {
        // Timestamps arrive ISO-encoded; a space reads better than the "T".
        const text = column.category === 'temporal'
          ? String(value).replace('T', ' ')
          : String(value);
        td.textContent = text;
        if (text.length > 24) td.title = text;
      }
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });

  if (!data.rows.length) {
    // Keep the header row on screen so filters stay reachable from the grid.
    const tr = el('tr');
    const cell = el('td', 'no-match');
    cell.colSpan = data.columns.length + 1;
    cell.append(
      el('div', 'empty-title', 'No rows match these filters'),
      el('div', 'empty-sub', 'Loosen or remove a filter to see data again.')
    );
    tr.appendChild(cell);
    body.appendChild(tr);
  }
  if (!$('popover').hidden && pop.column) positionPopover(pop.anchor);
}

function cycleSort(column) {
  if (!state.sort || state.sort.column !== column) state.sort = { column, descending: false };
  else if (!state.sort.descending) state.sort = { column, descending: true };
  else state.sort = null;
  state.page = 0;
  refresh({ countUnchanged: true });
}

function renderReadout() {
  const readout = $('match-readout');
  readout.innerHTML = '';
  const total = state.dataset.row_count;
  const specs = activeFilters();

  if (!specs.length) {
    readout.appendChild(el('b', null, fmtNum(total)));
    readout.appendChild(document.createTextNode(' rows in file'));
    return;
  }
  if (state.matched === null) {
    readout.appendChild(el('span', 'counting', 'Counting matching rows…'));
    return;
  }
  const pct = total ? (state.matched / total) * 100 : 0;
  readout.appendChild(el('b', null, fmtNum(state.matched)));
  readout.appendChild(document.createTextNode(` of ${fmtNum(total)} rows match `));
  readout.appendChild(el('span', null, `(${pct < 0.1 && pct > 0 ? '<0.1' : pct.toFixed(1)}%)`));
}

function renderStatus(rowsShown) {
  const first = state.page * state.pageSize + 1;
  const last = state.page * state.pageSize + rowsShown;
  $('status-left').textContent = rowsShown
    ? `Showing sample rows ${fmtNum(first)}–${fmtNum(last)}`
    : 'No rows to show';
  $('status-right').textContent = `query ${Math.round(state.lastMs)} ms · ${
    state.dataset.columns.length - state.hidden.size} of ${state.dataset.columns.length} columns shown`;
  $('page-label').textContent = rowsShown ? `${fmtNum(first)}–${fmtNum(last)}` : '—';
  $('btn-prev').disabled = state.page === 0;
  $('btn-next').disabled = rowsShown < state.pageSize;
}

/* ─────────────────────────── filter popover ─────────────────────────── */

const pop = {
  column: null,
  draft: null,
  tab: null,
  values: [],
  selected: new Set(),   // JSON-encoded values
  truncated: false,
  stats: null,
  anchor: null,
  search: '',
  pasted: null,          // { wanted, missing } after a list is pasted into search
};

function tabsFor(category) {
  if (category === 'numeric') return ['range', 'values', 'condition'];
  if (category === 'temporal') return ['range', 'values', 'condition'];
  if (category === 'boolean') return ['values', 'condition'];
  if (category === 'complex' || category === 'other') return ['condition'];
  return ['values', 'condition'];
}

const TAB_LABELS = { values: 'Values', range: 'Range', condition: 'Condition' };

function openFilterPopover(column, anchor) {
  column = effectiveColumn(column);
  pop.column = column;
  pop.anchor = anchor;
  pop.draft = JSON.parse(JSON.stringify(state.filters[column.name] || { column: column.name }));
  pop.values = [];
  pop.selected = new Set();
  pop.stats = null;
  pop.search = '';
  pop.pasted = null;

  const tabs = tabsFor(column.category);
  const existingOp = pop.draft.op;
  if ((existingOp === 'in' || existingOp === 'not_in') && pop.draft.list && tabs.includes('condition')) {
    // A pasted list reopens as the text the user typed, not as ticked boxes.
    pop.tab = 'condition';
    pop.draft.value = (pop.draft.values || []).map(listToken).join(', ');
  } else if (existingOp === 'in' || existingOp === 'not_in') pop.tab = 'values';
  else if (existingOp === 'between' || existingOp === 'not_between') pop.tab = tabs.includes('range') ? 'range' : 'condition';
  else if (existingOp) pop.tab = 'condition';
  else pop.tab = tabs[0];

  if (pop.draft.values) {
    for (const value of pop.draft.values) pop.selected.add(JSON.stringify(value ?? null));
  }

  $('pop-title').textContent = column.name;
  $('pop-type').textContent = typeLabel(column.name, column.type);
  renderExtractBar(column);
  $('pop-clear').hidden = !state.filters[column.name];

  const tabBar = $('pop-tabs');
  tabBar.innerHTML = '';
  for (const tab of tabs) {
    const button = el('button', `tab${tab === pop.tab ? ' active' : ''}`, TAB_LABELS[tab]);
    button.onclick = () => {
      pop.tab = tab;
      for (const node of tabBar.children) node.classList.toggle('active', node.textContent === TAB_LABELS[tab]);
      renderPopBody();
    };
    tabBar.appendChild(button);
  }

  $('popover').hidden = false;
  positionPopover(anchor);
  renderPopBody();
}

function positionPopover(anchor) {
  // The grid re-renders under an open popover (e.g. after an extraction),
  // replacing the header button it hangs from: follow the new one.
  if (anchor && !anchor.isConnected && pop.column) {
    const fresh = [...document.querySelectorAll('#grid-head .th-filter')]
      .find((button) => button.dataset.column === pop.column.name);
    if (fresh) pop.anchor = anchor = fresh;
  }
  if (!anchor || !anchor.isConnected) return;
  const node = $('popover');
  const box = anchor.getBoundingClientRect();
  // Nothing to measure (hidden or mid-render): stay where we are.
  if (!box.width && !box.height) return;
  const width = node.offsetWidth || 340;
  let left = Math.min(box.left, window.innerWidth - width - 12);
  left = Math.max(12, left);
  let top = box.bottom + 6;
  const height = node.offsetHeight || 420;
  if (top + height > window.innerHeight - 12) {
    top = Math.max(12, Math.min(box.top - height - 6, window.innerHeight - height - 12));
  }
  node.style.left = `${left}px`;
  node.style.top = `${top}px`;
}

function closePopover() {
  $('popover').hidden = true;
  pop.column = null;
}

function renderPopBody() {
  const body = $('pop-body');
  body.innerHTML = '';
  if (pop.tab === 'values') renderValuesTab(body);
  else if (pop.tab === 'range') renderRangeTab(body);
  else renderConditionTab(body);
  positionPopover(pop.anchor);
}

/* -- values tab -- */

function renderValuesTab(body) {
  const list = el('ul', 'value-list');

  const searchBox = el('input', 'input small');
  searchBox.type = 'search';
  searchBox.placeholder = 'Search, or paste a comma separated list…';
  searchBox.value = pop.search;
  const runSearch = () => {
    pop.search = searchBox.value;
    // Several values (a pasted list) are looked up exactly and ticked;
    // a single one is a substring search as before.
    const wanted = parseValueList(pop.search);
    pop.pasted = wanted.length > 1 ? { wanted, missing: [] } : null;
    loadValues(pop.pasted ? '' : pop.search, list, pop.pasted ? wanted : null);
  };
  searchBox.oninput = debounce(runSearch, 260);
  // A single-line input drops the line breaks from a column pasted out of a
  // spreadsheet, gluing the values together -- turn them into commas first.
  searchBox.onpaste = (event) => {
    const text = event.clipboardData && event.clipboardData.getData('text');
    if (!text || !/[\r\n\t]/.test(text.trim())) return;
    event.preventDefault();
    searchBox.value = parseValueList(text).map(listToken).join(', ');
    runSearch();
  };
  body.appendChild(searchBox);

  const tools = el('div', 'value-tools');
  const modeSelect = el('select', 'input small');
  for (const [value, label] of [['in', 'Include selected'], ['not_in', 'Exclude selected']]) {
    const option = el('option', null, label);
    option.value = value;
    modeSelect.appendChild(option);
  }
  modeSelect.value = pop.draft.op === 'not_in' ? 'not_in' : 'in';
  modeSelect.onchange = () => { pop.draft.op = modeSelect.value; };

  const selectAll = el('button', 'link-btn', 'All');
  const selectNone = el('button', 'link-btn', 'None');
  tools.append(modeSelect, el('span', 'grow'), selectAll, el('span', 'dot-sep', '·'), selectNone);
  body.appendChild(tools);
  body.appendChild(list);

  selectAll.onclick = () => {
    for (const entry of pop.values) pop.selected.add(JSON.stringify(entry.value ?? null));
    renderValueRows(list);
  };
  selectNone.onclick = () => {
    pop.selected.clear();
    renderValueRows(list);
  };

  if (!pop.values.length) {
    list.appendChild(el('li', 'loading-line', 'Loading values…'));
    loadValues(pop.search, list);
  } else {
    renderValueRows(list);
  }

  const pasteNote = el('div', 'value-note paste-note');
  pasteNote.hidden = true;
  body.appendChild(pasteNote);
  pop.renderPasteNote = () => renderPasteNote(pasteNote);
  pop.renderPasteNote();

  if (pop.truncated && !pop.pasted) {
    body.appendChild(el(
      'div', 'value-note',
      `Showing the ${pop.values.length} most common values. Search above, or use the Condition tab to match a wider set.`
    ));
  }
}

function renderPasteNote(node) {
  if (!node.isConnected) return;
  const pasted = pop.pasted;
  node.hidden = !pasted;
  if (!pasted) return;
  const found = pasted.wanted.length - pasted.missing.length;
  node.textContent = `Found and ticked ${fmtNum(found)} of ${fmtNum(pasted.wanted.length)} pasted values.`;
  if (pasted.missing.length) {
    const shown = pasted.missing.slice(0, 8).join(', ');
    const extra = pasted.missing.length > 8 ? ` +${pasted.missing.length - 8} more` : '';
    // The list only offers values the *other* filters still allow.
    const narrowed = activeFilters().some((spec) => spec.column !== pop.column.name);
    const label = narrowed ? 'Not found in the rows your other filters keep' : 'Not in this column';
    node.appendChild(el('div', 'paste-missing', `${label}: ${shown}${extra}`));
  }
}

async function loadValues(search = '', listNode = null, exact = null) {
  const column = pop.column;
  const pasted = pop.pasted;
  try {
    const data = await api('/api/values', {
      dataset_id: state.dataset.id,
      column: column.name,
      filters: activeFilters(),
      search,
      exact,
      transform: column.transform || null,
      date_format: column.dateFormat || null,
      limit: exact ? Math.max(300, exact.length) : 300,
    });
    if (!pop.column || pop.column.name !== column.name || pop.pasted !== pasted) return;
    pop.values = data.values;
    pop.truncated = data.truncated;
    if (exact && pasted) {
      // A pasted list replaces the selection with exactly what it names.
      pop.selected = new Set(data.values.map((entry) => JSON.stringify(entry.value ?? null)));
      const hits = new Set(data.values.map((entry) => String(entry.value ?? '').toLowerCase()));
      pasted.missing = exact.filter((value) => !hits.has(value.toLowerCase()));
    }
    if (listNode && listNode.isConnected) {
      renderValueRows(listNode);
      if (pop.renderPasteNote) pop.renderPasteNote();
    } else renderPopBody();
  } catch (error) {
    if (listNode && listNode.isConnected) {
      listNode.innerHTML = '';
      listNode.appendChild(el('li', 'loading-line', error.message));
    }
  }
}

function renderValueRows(list) {
  list.innerHTML = '';
  if (!pop.values.length) {
    list.appendChild(el('li', 'loading-line', 'No values match that search.'));
    return;
  }
  for (const entry of pop.values) {
    const key = JSON.stringify(entry.value ?? null);
    const row = el('li', 'value-row');
    const box = el('input');
    box.type = 'checkbox';
    box.checked = pop.selected.has(key);
    box.onchange = () => {
      if (box.checked) pop.selected.add(key);
      else pop.selected.delete(key);
    };
    const isNull = entry.value === null || entry.value === undefined;
    const text = el('span', `value-text${isNull ? ' null' : ''}`, valueLabel(entry.value));
    text.title = valueLabel(entry.value);
    row.append(box, text, el('span', 'value-count', fmtNum(entry.count)));
    row.onclick = (event) => {
      if (event.target !== box) { box.checked = !box.checked; box.onchange(); }
    };
    list.appendChild(row);
  }
}

/* -- range tab -- */

function renderRangeTab(body) {
  const isDate = pop.column.category === 'temporal';
  const stats = el('div', 'stats-line', 'Loading column statistics…');
  body.appendChild(stats);

  const row = el('div', 'range-row');
  const low = el('input', 'input small');
  const high = el('input', 'input small');
  low.type = high.type = isDate ? 'date' : 'number';
  if (!isDate) low.step = high.step = 'any';
  low.placeholder = 'Minimum';
  high.placeholder = 'Maximum';
  if (pop.draft.op === 'between' || pop.draft.op === 'not_between') {
    low.value = toInputValue(pop.draft.value, isDate);
    high.value = toInputValue(pop.draft.value2, isDate);
  }
  low.oninput = high.oninput = () => {
    pop.draft.op = pop.draft.op === 'not_between' ? 'not_between' : 'between';
    pop.draft.value = low.value === '' ? null : (isDate ? low.value : Number(low.value));
    pop.draft.value2 = high.value === '' ? null : (isDate ? high.value : Number(high.value));
  };
  row.append(low, el('span', 'sep', 'to'), high);
  body.appendChild(row);

  const invert = el('label', 'check');
  const invertBox = el('input');
  invertBox.type = 'checkbox';
  invertBox.checked = pop.draft.op === 'not_between';
  invertBox.onchange = () => { pop.draft.op = invertBox.checked ? 'not_between' : 'between'; };
  invert.append(invertBox, el('span', null, 'Exclude this range instead'));
  invert.style.marginTop = '14px';
  body.appendChild(invert);

  loadStats().then((data) => {
    if (!data || !stats.isConnected) return;
    stats.innerHTML = '';
    const bits = [
      ['min', formatStat(data.lo, isDate)],
      ['max', formatStat(data.hi, isDate)],
    ];
    if (data.mean !== undefined && data.mean !== null) bits.push(['avg', Number(data.mean).toFixed(2)]);
    if (data.med !== undefined && data.med !== null) bits.push(['median', Number(data.med).toFixed(2)]);
    bits.push(['blank', fmtNum(data.nulls)]);
    for (const [label, value] of bits) {
      const chunk = el('span');
      chunk.append(document.createTextNode(`${label} `), Object.assign(el('b'), { textContent: value }));
      stats.appendChild(chunk);
    }
    const useFull = el('button', 'link-btn', 'Fill from data');
    useFull.onclick = () => {
      low.value = toInputValue(data.lo, isDate);
      high.value = toInputValue(data.hi, isDate);
      low.oninput();
    };
    stats.appendChild(useFull);
  });
}

/* A <input type="date"> yields a bare 'YYYY-MM-DD'. Compared against a
   TIMESTAMP column that means midnight, so an upper bound would silently drop
   everything recorded later that same day. Widen bounds to cover the whole day. */
const isTimestampColumn = (column) =>
  column.category === 'temporal' && /^TIMESTAMP/i.test(column.type);

const isBareDate = (value) => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);

const dayStart = (value) => `${String(value).slice(0, 10)} 00:00:00`;
const dayEnd = (value) => `${String(value).slice(0, 10)} 23:59:59.999999`;

function toInputValue(value, isDate) {
  if (value === null || value === undefined || value === '') return '';
  return isDate ? String(value).slice(0, 10) : value;
}

function formatStat(value, isDate) {
  if (value === null || value === undefined) return '—';
  if (isDate) return String(value).slice(0, 10);
  return typeof value === 'number' ? nf.format(Number(value.toFixed(4))) : String(value);
}

async function loadStats() {
  if (pop.stats) return pop.stats;
  const column = pop.column;
  try {
    const data = await api('/api/stats', {
      dataset_id: state.dataset.id,
      column: column.name,
      filters: activeFilters(),
      transform: column.transform || null,
      date_format: column.dateFormat || null,
    });
    if (!pop.column || pop.column.name !== column.name) return null;
    pop.stats = data;
    return data;
  } catch (_) {
    return null;
  }
}

/* -- condition tab -- */

const LIST_OPS = ['in', 'not_in'];
const NULLARY_OPS = ['is_null', 'is_not_null', 'is_empty', 'is_not_empty'];

function opsFor(category) {
  const nullary = [['is_null', 'is blank'], ['is_not_null', 'is not blank']];
  const list = [['in', 'is any of (list)'], ['not_in', 'is none of (list)']];
  if (category === 'numeric' || category === 'temporal') {
    return [['eq', '='], ['ne', '≠'], ['gt', '>'], ['gte', '≥'], ['lt', '<'], ['lte', '≤'], ...list, ...nullary];
  }
  if (category === 'boolean') return [['eq', 'is'], ...nullary];
  if (category === 'complex' || category === 'other') {
    return [
      ['contains', 'contains'], ['not_contains', 'does not contain'],
      ['starts_with', 'starts with'], ['ends_with', 'ends with'],
      ['eq', 'equals'], ['ne', 'does not equal'], ['regex', 'matches regex'],
      ['is_empty', 'is empty'], ['is_not_empty', 'is not empty'], ...nullary,
    ];
  }
  return [
    ['contains', 'contains'], ['not_contains', 'does not contain'],
    ['starts_with', 'starts with'], ['ends_with', 'ends with'],
    ['eq', 'equals'], ['ne', 'does not equal'], ...list, ['regex', 'matches regex'],
    ['is_empty', 'is empty'], ['is_not_empty', 'is not empty'], ...nullary,
  ];
}

/* Split pasted text into values. Commas, tabs and new lines all separate, so a
   column copied out of a spreadsheet works as well as "a, b, c". Wrap a value
   in double quotes to keep a comma inside it. Blanks and repeats are dropped. */
function parseValueList(text) {
  const out = [];
  const seen = new Set();
  // A quoted value may have spaces around it ("a, "b, c"") but must end at a
  // separator; anything else is read as plain text, quotes and all.
  const pattern = /[ ]*"((?:[^"]|"")*)"[ ]*(?=[,\t\r\n]|$)|([^,\t\r\n]+)/g;
  let match;
  while ((match = pattern.exec(String(text ?? ''))) !== null) {
    const value = (match[1] !== undefined ? match[1].replace(/""/g, '"') : match[2]).trim();
    if (value === '' || seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

const listToken = (value) => {
  const text = String(value ?? '');
  return /[,"\t\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
};

function renderConditionTab(body) {
  const options = opsFor(pop.column.category);
  const field = el('label', 'field');
  field.appendChild(el('span', null, 'Condition'));
  const select = el('select', 'input');
  for (const [value, label] of options) {
    const option = el('option', null, label);
    option.value = value;
    select.appendChild(option);
  }
  const current = options.some(([value]) => value === pop.draft.op) ? pop.draft.op : options[0][0];
  select.value = current;
  field.appendChild(select);
  body.appendChild(field);

  const valueField = el('label', 'field');
  valueField.appendChild(el('span', null, 'Value'));
  const input = el('input', 'input');
  const isDate = pop.column.category === 'temporal';
  input.type = pop.column.category === 'numeric' ? 'number' : (isDate ? 'date' : 'text');
  if (pop.column.category === 'numeric') input.step = 'any';
  input.value = toInputValue(pop.draft.value, isDate);
  input.oninput = () => {
    pop.draft.value = input.value === '' ? null
      : (pop.column.category === 'numeric' ? Number(input.value) : input.value);
  };
  valueField.appendChild(input);
  body.appendChild(valueField);

  const listField = el('label', 'field');
  const listHint = el('em', null, '');
  const listLabel = el('span', null, 'Values ');
  listLabel.appendChild(listHint);
  listField.appendChild(listLabel);
  const listBox = el('textarea', 'input list-input');
  listBox.rows = 5;
  listBox.spellcheck = false;
  listBox.placeholder = isDate
    ? 'Paste values separated by commas or new lines\ne.g. 2024-01-31, 2024-02-29'
    : 'Paste values separated by commas or new lines\ne.g. north, south, east';
  const countList = () => {
    const n = parseValueList(listBox.value).length;
    listHint.textContent = n ? `— ${fmtNum(n)} value${n === 1 ? '' : 's'}` : '— comma or line separated';
  };
  listBox.oninput = () => { pop.draft.value = listBox.value; countList(); };

  // Pasting several values into the single-value field means "any of these".
  input.onpaste = (event) => {
    const text = event.clipboardData && event.clipboardData.getData('text');
    if (!text || parseValueList(text).length < 2) return;
    if (!options.some(([value]) => value === 'in')) return;
    event.preventDefault();
    const negative = ['ne', 'not_contains'].includes(select.value);
    select.value = negative ? 'not_in' : 'in';
    select.onchange();
    listBox.value = text.trim();
    pop.draft.value = listBox.value;
    countList();
    listBox.focus();
  };
  listField.appendChild(listBox);
  body.appendChild(listField);

  let caseBox = null;
  if (['text', 'complex', 'other'].includes(pop.column.category)) {
    const wrap = el('label', 'check');
    caseBox = el('input');
    caseBox.type = 'checkbox';
    caseBox.checked = Boolean(pop.draft.case_sensitive);
    caseBox.onchange = () => { pop.draft.case_sensitive = caseBox.checked; };
    wrap.append(caseBox, el('span', null, 'Match case'));
    body.appendChild(wrap);
  }

  const syncVisibility = () => {
    const isList = LIST_OPS.includes(select.value);
    const needsValue = !NULLARY_OPS.includes(select.value);
    valueField.style.display = needsValue && !isList ? '' : 'none';
    listField.style.display = isList ? '' : 'none';
    if (caseBox) {
      caseBox.parentElement.style.display =
        ['contains', 'not_contains', 'starts_with', 'ends_with', ...LIST_OPS].includes(select.value) ? '' : 'none';
    }
  };
  select.onchange = () => {
    const wasList = LIST_OPS.includes(pop.draft.op);
    pop.draft.op = select.value;
    // Carry whatever was typed across when switching between one value and a list.
    if (LIST_OPS.includes(select.value) && !wasList) {
      listBox.value = input.value;
      pop.draft.value = listBox.value;
    } else if (!LIST_OPS.includes(select.value) && wasList) {
      const first = parseValueList(listBox.value)[0] ?? '';
      input.value = toInputValue(first, isDate);
      input.oninput();
    }
    countList();
    syncVisibility();
  };
  if (LIST_OPS.includes(current)) {
    listBox.value = pop.draft.value ?? '';
    input.value = '';
  }
  countList();
  pop.draft.op = current;
  syncVisibility();

  if (pop.column.category === 'boolean') {
    input.type = 'text';
    input.placeholder = 'true or false';
  }
}

/* -- apply -- */

function applyFilter() {
  const draft = pop.draft;
  const column = pop.column;
  draft.column = column.name;
  // A filter on an extracted part says so, so it means "year = 2024" wherever
  // it is read -- the Pivot tab included.
  if (column.transform) draft.transform = column.transform;
  else delete draft.transform;
  if (column.transform && column.dateFormat) draft.date_format = column.dateFormat;
  else delete draft.date_format;

  if (pop.tab === 'values') {
    draft.op = draft.op === 'not_in' ? 'not_in' : 'in';
    draft.values = [...pop.selected].map((key) => JSON.parse(key));
    delete draft.value;
    delete draft.value2;
    delete draft.list;
    delete draft.case_sensitive;
    if (!draft.values.length) {
      showPopError('Select at least one value, or clear the filter.');
      return;
    }
  } else if (pop.tab === 'range') {
    if (draft.op !== 'not_between') draft.op = 'between';
    delete draft.values;
    if ((draft.value === null || draft.value === undefined || draft.value === '')
      && (draft.value2 === null || draft.value2 === undefined || draft.value2 === '')) {
      showPopError('Enter a minimum, a maximum, or both.');
      return;
    }
    if (isTimestampColumn(column)) {
      if (isBareDate(draft.value)) draft.value = dayStart(draft.value);
      if (isBareDate(draft.value2)) draft.value2 = dayEnd(draft.value2);
    }
  } else if (LIST_OPS.includes(draft.op)) {
    const values = parseValueList(draft.value);
    if (!values.length) {
      showPopError('Paste at least one value, separated by commas or new lines.');
      return;
    }
    if (column.category === 'numeric') {
      const bad = values.filter((v) => !Number.isFinite(Number(v)));
      if (bad.length) {
        showPopError(`Not a number: ${bad.slice(0, 5).join(', ')}${bad.length > 5 ? ` +${bad.length - 5} more` : ''}`);
        return;
      }
    }
    if (isTimestampColumn(column) && values.some(isBareDate)) {
      // A bare date would only match rows stamped exactly at midnight.
      showPopError('This column holds timestamps — paste full values (2024-01-31 14:05:00) or use the Range tab.');
      return;
    }
    draft.values = column.category === 'numeric' ? values.map(Number) : values;
    draft.list = true;
    if (column.category === 'text') draft.case_sensitive = Boolean(draft.case_sensitive);
    else delete draft.case_sensitive;
    delete draft.value;
    delete draft.value2;
  } else {
    delete draft.values;
    delete draft.value2;
    delete draft.list;
    const nullary = ['is_null', 'is_not_null', 'is_empty', 'is_not_empty'].includes(draft.op);
    if (!nullary && (draft.value === null || draft.value === undefined || draft.value === '')) {
      showPopError('Enter a value for this condition.');
      return;
    }
    if (isTimestampColumn(column) && isBareDate(draft.value)) {
      if (draft.op === 'eq') {
        // "on this date" over a timestamp really means the whole day.
        draft.op = 'between';
        draft.value2 = dayEnd(draft.value);
        draft.value = dayStart(draft.value);
      } else if (draft.op === 'lte' || draft.op === 'gt') {
        draft.value = dayEnd(draft.value);
      } else {
        draft.value = dayStart(draft.value);
      }
    }
    if (column.category === 'boolean' && draft.op === 'eq') {
      const text = String(draft.value).trim().toLowerCase();
      if (!['true', 'false', '1', '0', 'yes', 'no'].includes(text)) {
        showPopError('Enter true or false.');
        return;
      }
      draft.value = ['true', '1', 'yes'].includes(text);
    }
  }

  state.filters[column.name] = draft;
  state.page = 0;
  closePopover();
  renderChips();
  renderRail();
  refresh();
}

function showPopError(message) {
  const body = $('pop-body');
  const existing = body.querySelector('.pop-error');
  if (existing) existing.remove();
  body.appendChild(el('div', 'pop-error', message));
}

function clearCurrentFilter() {
  if (!pop.column) return;
  delete state.filters[pop.column.name];
  state.page = 0;
  closePopover();
  renderChips();
  renderRail();
  refresh();
}

/* ─────────────────────────── export ─────────────────────────── */

function isPivotExport() {
  return state.mode === 'pivot' && !pivotIsEmpty();
}

function openDrawer() {
  const pivotMode = isPivotExport();
  $('drawer-title').textContent = pivotMode ? 'Export pivot to Excel' : 'Export filtered data';
  // A pivot is always an .xlsx of exactly the table on screen: no format
  // choice, no row limit, and the columns come from the Values well.
  $('field-format').hidden = pivotMode;
  $('field-limit').hidden = pivotMode;
  $('field-columns').hidden = pivotMode;
  $('export-sheet').value = pivotMode ? 'Pivot' : ($('export-sheet').value || 'Data');
  $('btn-start-export').textContent = pivotMode ? 'Export pivot' : 'Start export';
  $('progress-block').hidden = true;

  renderExportSummary();
  renderExportColumns();
  syncFormatFields();
  $('scrim').hidden = false;
  $('drawer').hidden = false;
}

function closeDrawer() {
  $('scrim').hidden = true;
  $('drawer').hidden = true;
}

function renderExportSummary() {
  const node = $('export-summary');
  if (!node || !state.dataset) return;
  node.innerHTML = '';

  if (isPivotExport()) {
    renderPivotExportSummary(node);
    return;
  }

  const rows = [
    ['Source', state.dataset.name],
    ['Filters applied', String(activeFilters().length)],
    ['Columns', `${visibleColumns().length} of ${state.dataset.columns.length}`],
  ];
  const visible = new Set(visibleColumns());
  const extracted = Object.entries(state.transforms).filter(([name]) => visible.has(name));
  if (extracted.length) {
    rows.push(['Extracted', extracted.map(([name, part]) => `${name} → ${part}`).join(', ')]);
  }
  for (const [label, value] of rows) {
    const row = el('div', 'summary-row');
    row.append(el('span', null, label), el('span', null, value));
    node.appendChild(row);
  }
  const matchRow = el('div', 'summary-row emph');
  matchRow.append(
    el('span', null, 'Rows to export'),
    el('span', null, state.matched === null ? 'counting…' : fmtNum(state.matched))
  );
  node.appendChild(matchRow);
  updateExcelNotice();
}

function renderPivotExportSummary(node) {
  const pivotState = state.pivot;
  const result = pivotState.result;
  const rows = [
    ['Source', state.dataset.name],
    ['Rows', pivotState.rows.join(' › ') || 'none'],
    ['Columns', pivotState.columns.join(' › ') || 'none'],
    ['Values', pivotState.values.map(valueChipLabel).join(', ')],
    ['Filters applied', String(activeFilters().length)],
  ];
  for (const [label, value] of rows) {
    const row = el('div', 'summary-row');
    row.append(el('span', null, label), el('span', null, value));
    node.appendChild(row);
  }
  const emph = el('div', 'summary-row emph');
  emph.append(
    el('span', null, 'Rows in the pivot'),
    el('span', null, result ? fmtNum(result.total_row_groups) : '—')
  );
  node.appendChild(emph);
  if (result && result.truncated) {
    const perFile = PIVOT_EXPORT_ROWS_PER_FILE;
    const files = Math.ceil(result.total_row_groups / perFile);
    const note = el('div', 'notice', files > 1
      ? `The screen shows the first ${fmtNum(result.row_groups)} row groups. The export writes all `
        + `${fmtNum(result.total_row_groups)} — too many for one sheet, so they come back as about `
        + `${files} Excel files in a zip, split on whole ${pivotState.rows[0]} groups.`
      : `The screen shows the first ${fmtNum(result.row_groups)} row groups; the export writes all `
        + `${fmtNum(result.total_row_groups)}.`);
    note.hidden = false;
    node.appendChild(note);
  }
}

function renderExportColumns() {
  const node = $('export-columns');
  if (!node || !state.dataset) return;
  const hidden = state.hidden.size;
  node.textContent = hidden
    ? `${visibleColumns().length} columns will be exported. ${hidden} hidden column${hidden > 1 ? 's are' : ' is'} excluded — toggle them back in the Columns rail.`
    : `All ${state.dataset.columns.length} columns will be exported. Hide columns in the Columns rail to leave them out.`;
}

function updateExcelNotice() {
  const notice = $('excel-notice');
  if (!notice) return;
  const limit = 1048575;
  const rows = state.matched;
  if (isPivotExport()) {
    notice.hidden = true;
    return;
  }
  if ($('export-format').value === 'xlsx' && rows !== null && rows > limit) {
    const sheets = Math.ceil(rows / limit);
    notice.textContent =
      `Excel allows ${nf.format(limit)} rows per sheet. These ${fmtNum(rows)} rows will be split across ${sheets} sheets. ` +
      `CSV keeps everything in one file and writes much faster.`;
    notice.hidden = false;
  } else {
    notice.hidden = true;
  }
}

function syncFormatFields() {
  const isExcel = isPivotExport() || $('export-format').value === 'xlsx';
  $('excel-only').style.display = isExcel ? '' : 'none';
  updateExcelNotice();
}

async function startExport() {
  const pivotMode = isPivotExport();
  const limitRaw = $('export-limit').value;
  const payload = pivotMode ? {
    ...pivotPayload(),
    sheet_name: $('export-sheet').value.trim() || 'Pivot',
    include_manifest: $('export-manifest').checked,
  } : {
    dataset_id: state.dataset.id,
    format: $('export-format').value,
    filters: activeFilters(),
    columns: visibleColumns(),
    order_by: state.sort ? state.sort.column : null,
    descending: state.sort ? state.sort.descending : false,
    row_limit: limitRaw ? Math.max(1, parseInt(limitRaw, 10)) : null,
    include_manifest: $('export-manifest').checked,
    sheet_name: $('export-sheet').value.trim() || 'Data',
    total_hint: state.matched,
    transforms: transformsPayload(),
  };

  $('btn-start-export').disabled = true;
  $('btn-download').hidden = true;
  $('progress-block').hidden = false;
  $('progress-fill').className = 'progress-fill indeterminate';
  $('progress-label').textContent = 'Starting…';
  $('progress-detail').textContent = '';

  try {
    const endpoint = pivotMode ? '/api/pivot/export' : '/api/export';
    const job = await api(endpoint, payload);
    state.exportJob = job;
    pollExport(job.id);
  } catch (error) {
    $('btn-start-export').disabled = false;
    $('progress-label').textContent = 'Could not start export';
    $('progress-fill').className = 'progress-fill error';
    toast(error.message, 'error');
  }
}

function pollExport(jobId) {
  clearTimeout(state.pollTimer);
  const tick = async () => {
    let job;
    try {
      job = await api(`/api/export/${jobId}`, undefined, 'GET');
    } catch (error) {
      $('progress-label').textContent = error.message;
      $('btn-start-export').disabled = false;
      return;
    }
    state.exportJob = job;
    renderExportProgress(job);
    if (['running', 'queued', 'counting'].includes(job.status)) {
      state.pollTimer = setTimeout(tick, 400);
    } else {
      $('btn-start-export').disabled = false;
    }
  };
  tick();
}

function renderExportProgress(job) {
  const fill = $('progress-fill');
  const label = $('progress-label');
  const detail = $('progress-detail');
  const cancel = $('btn-cancel-export');
  const download = $('btn-download');

  const STATUS_TEXT = {
    queued: 'Queued',
    counting: job.kind === 'pivot' ? 'Aggregating…' : 'Counting matching rows…',
    running: job.kind === 'pivot' ? 'Writing the pivot…' : 'Writing rows…',
    done: 'Export complete',
    error: 'Export failed',
    cancelled: 'Export cancelled',
  };
  label.textContent = STATUS_TEXT[job.status] || job.status;

  if (job.status === 'done') {
    fill.className = 'progress-fill done';
    fill.style.width = '100%';
    // A split pivot comes back as a zip of workbooks, not as sheets in one book.
    const parts = job.format === 'zip' ? `${job.sheets} files` : (
      job.sheets > 1 ? `${job.sheets} sheets` : '');
    const unit = job.kind === 'pivot'
      ? (job.format === 'zip' ? 'row groups' : 'pivot rows') : 'rows';
    detail.textContent = `${fmtNum(job.written)} ${unit} · ${fmtBytes(job.size)}${
      parts ? ` · ${parts}` : ''} · ${fmtDuration(job.elapsed)}`;
    cancel.hidden = true;
    download.hidden = false;
    download.href = `/api/export/${job.id}/download`;
    download.textContent = `Download ${job.filename}`;
  } else if (job.status === 'error') {
    fill.className = 'progress-fill error';
    fill.style.width = '100%';
    detail.textContent = '';
    label.textContent = job.error || 'Export failed';
    cancel.hidden = true;
    download.hidden = true;
  } else if (job.status === 'cancelled') {
    fill.className = 'progress-fill';
    fill.style.width = '0';
    detail.textContent = '';
    cancel.hidden = true;
    download.hidden = true;
  } else {
    cancel.hidden = false;
    download.hidden = true;
    if (job.percent !== null && job.percent !== undefined && job.total) {
      fill.className = 'progress-fill';
      fill.style.width = `${job.percent}%`;
      detail.textContent = `${fmtNum(job.written)} / ${fmtNum(job.total)} rows${
        job.rows_per_sec ? ` · ${fmtNum(job.rows_per_sec)}/s` : ''}`;
    } else {
      fill.className = 'progress-fill indeterminate';
      detail.textContent = job.written ? `${fmtNum(job.written)} rows` : '';
    }
  }
}

async function cancelExport() {
  if (!state.exportJob) return;
  try {
    await api(`/api/export/${state.exportJob.id}/cancel`, {});
  } catch (error) {
    toast(error.message, 'error');
  }
}

/* ─────────────────────────── file browser ─────────────────────────── */

let browseCurrent = null;
// The union row's input a pick should fill; null means "open what is picked".
let browseTarget = null;

function pickBrowsed(path) {
  const target = browseTarget;
  closeBrowser();
  if (target) {
    target.value = path;
    target.focus();
  } else {
    openPath(path);
  }
}

async function openBrowser(path, target = null) {
  browseTarget = target;
  $('browse-title').textContent = target ? 'Choose a file to union' : 'Choose a parquet file';
  $('browse-open-dir').textContent = target ? 'Use this folder' : 'Open this folder as a dataset';
  try {
    const data = await api(`/api/browse?path=${encodeURIComponent(path || '~')}`, undefined, 'GET');
    browseCurrent = data.path;
    $('browse-path').textContent = data.path;
    const list = $('browse-list');
    list.innerHTML = '';

    if (data.parent) {
      const up = el('li');
      const button = el('button', 'browse-item');
      button.append(el('span', 'browse-icon', '↰'), el('span', 'browse-name', '..'));
      button.onclick = () => openBrowser(data.parent, browseTarget);
      up.appendChild(button);
      list.appendChild(up);
    }

    for (const entry of data.entries) {
      const item = el('li');
      const button = el('button', 'browse-item');
      button.append(
        el('span', 'browse-icon', entry.is_dir ? '📁' : '▤'),
        el('span', 'browse-name', entry.name),
        el('span', 'browse-size', entry.is_dir ? '' : fmtBytes(entry.size))
      );
      button.onclick = () => {
        if (entry.is_dir) openBrowser(entry.path, browseTarget);
        else pickBrowsed(entry.path);
      };
      item.appendChild(button);
      list.appendChild(item);
    }
    if (!data.entries.length) {
      list.appendChild(el('li', 'hint', 'No parquet files or subfolders here.'));
    }

    $('browse-scrim').hidden = false;
    $('browse-modal').hidden = false;
  } catch (error) {
    toast(error.message, 'error');
  }
}

function closeBrowser() {
  browseTarget = null;
  $('browse-scrim').hidden = true;
  $('browse-modal').hidden = true;
}

async function loadRecents() {
  try {
    const data = await api('/api/recents', undefined, 'GET');
    const wrap = $('recents');
    const list = $('recents-list');
    list.innerHTML = '';
    if (!data.recents.length) { wrap.hidden = true; return; }
    for (const entry of data.recents) {
      const item = el('li');
      const button = el('button', 'recent-btn');
      button.append(
        el('span', 'recent-name', entry.name),
        el('span', 'recent-path', entry.path),
        el('span', 'recent-rows', `${fmtNum(entry.rows)} rows`)
      );
      button.title = entry.path;
      button.onclick = () => openPath(entry.path);
      item.appendChild(button);
      list.appendChild(item);
    }
    wrap.hidden = false;
  } catch (_) { /* recents are a nicety, never a blocker */ }
}

/* ─────────────────────────── floating menu ─────────────────────────── */

/* One reusable menu, driven by a list of sections. Sections look like
   { title, items: [{ label, hint, on, disabled, onSelect }] } and a section may
   set `searchable` to get a filter box when it holds a long list. */
function showMenu(anchor, sections) {
  const menu = $('menu');
  menu.innerHTML = '';
  menu.hidden = false;

  const paint = (needle) => {
    menu.innerHTML = '';
    let shown = 0;
    for (const section of sections) {
      const items = needle
        ? section.items.filter((i) => i.label.toLowerCase().includes(needle))
        : section.items;
      if (!items.length) continue;
      if (section.title) menu.appendChild(el('div', 'menu-head', section.title));
      if (section.searchable && !needle && section.items.length > 12) {
        const box = el('input', 'input small menu-search');
        box.type = 'search';
        box.placeholder = 'Filter…';
        box.oninput = () => paint(box.value.trim().toLowerCase());
        menu.appendChild(box);
        box.focus();
      }
      for (const item of items) {
        const button = el('button', `menu-item${item.on ? ' on' : ''}`, item.label);
        button.disabled = Boolean(item.disabled);
        if (item.hint) button.appendChild(el('span', 'mi-type', item.hint));
        button.onclick = () => { closeMenu(); item.onSelect(); };
        menu.appendChild(button);
        shown += 1;
      }
      if (section !== sections[sections.length - 1]) menu.appendChild(el('div', 'menu-sep'));
    }
    if (!shown) menu.appendChild(el('div', 'menu-head', 'Nothing to show'));
  };

  paint('');
  const box = anchor.getBoundingClientRect();
  menu.style.left = `${Math.max(8, Math.min(box.left, window.innerWidth - menu.offsetWidth - 8))}px`;
  const below = box.bottom + 4;
  menu.style.top = `${below + menu.offsetHeight > window.innerHeight - 8
    ? Math.max(8, box.top - menu.offsetHeight - 4) : below}px`;
}

function closeMenu() {
  $('menu').hidden = true;
}

/* ─────────────────────────── pivot ─────────────────────────── */

const num2 = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

/* Filled from /api/pivot/aggregations so the two ends cannot drift apart. */
let AGGS = null;
let aggsPromise = null;

/* The server splits a pivot export into files of this many row groups; the real
   figure arrives with the aggregation list. */
let PIVOT_EXPORT_ROWS_PER_FILE = 100000;

function loadAggregations() {
  if (AGGS) return Promise.resolve(AGGS);
  if (!aggsPromise) {
    aggsPromise = api('/api/pivot/aggregations', undefined, 'GET')
      .then((data) => {
        AGGS = data.aggregations;
        if (data.export_rows_per_file) PIVOT_EXPORT_ROWS_PER_FILE = data.export_rows_per_file;
        return AGGS;
      })
      .catch(() => { aggsPromise = null; return []; });
  }
  return aggsPromise;
}

const aggLabel = (id) => {
  const found = (AGGS || []).find((a) => a.id === id);
  return found ? found.label : id;
};

const aggAllowed = (agg, category) => !agg.categories || agg.categories.includes(category);

/* The aggregation a column gets when it is first dropped into Values. */
function defaultAgg(column) {
  return column.category === 'numeric' ? 'sum' : 'count';
}

function valueChipLabel(value) {
  if (value.agg === 'count_rows') return 'Count of rows';
  return `${aggLabel(value.agg)} of ${value.column}`;
}

const pivotIsEmpty = () =>
  !state.pivot.values.length || (!state.pivot.rows.length && !state.pivot.columns.length);

function setMode(mode) {
  applyMode(mode);
  refresh({ countUnchanged: state.matched !== null });
}

const EXPORT_LABELS = { pivot: 'Export pivot', data: 'Export' };

function applyMode(mode) {
  state.mode = mode;
  for (const name of ['data', 'pivot']) {
    $(`tab-${name}`).classList.toggle('active', mode === name);
    $(`${name}-view`).hidden = mode !== name;
  }
  $('btn-export').textContent = EXPORT_LABELS[mode] || 'Export';
  if (mode === 'pivot') {
    loadAggregations().then(renderWells);
    renderWells();
  }
  // Extracted columns are labelled in the Data view only, so the rail follows the view.
  if (state.dataset) renderRail();
}

/* -- the three wells -- */

const WELL_IDS = { rows: 'drop-rows', columns: 'drop-columns', values: 'drop-values' };
const WELL_EMPTY = {
  rows: 'Drop a column here to group rows',
  columns: 'Optional — drop a column to spread it across',
  values: 'Drop the columns to aggregate',
};

function wellItems(well) {
  return state.pivot[well];
}

function renderWells() {
  if (!state.dataset) return;
  for (const well of ['rows', 'columns', 'values']) {
    const zone = $(WELL_IDS[well]);
    zone.innerHTML = '';
    const items = wellItems(well);
    if (!items.length) {
      zone.appendChild(el('span', 'well-empty', WELL_EMPTY[well]));
      continue;
    }
    items.forEach((item, index) => zone.appendChild(fieldChip(well, item, index)));
  }
}

function fieldChip(well, item, index) {
  const isValue = well === 'values';
  const chip = el('span', `field-chip${isValue ? ' value' : ''}`);
  chip.draggable = true;
  chip.dataset.well = well;
  chip.dataset.index = String(index);

  const name = el('span', 'fc-name');
  if (isValue) {
    name.appendChild(Object.assign(el('span', 'fc-agg'), {
      textContent: item.agg === 'count_rows' ? 'Count of rows' : aggLabel(item.agg),
    }));
    if (item.agg !== 'count_rows') name.appendChild(document.createTextNode(` of ${item.column}`));
  } else {
    name.textContent = item;
  }
  name.title = isValue ? valueChipLabel(item) : item;

  const caret = el('span', 'fc-caret', '▾');
  const remove = el('button', 'fc-x', '×');
  remove.title = 'Remove from ' + well;
  remove.onclick = (event) => {
    event.stopPropagation();
    wellItems(well).splice(index, 1);
    if (well === 'values' && state.pivot.sort) state.pivot.sort = null;
    renderWells();
    runPivot();
  };

  chip.append(name, caret, remove);
  chip.onclick = (event) => {
    if (event.target === remove) return;
    openChipMenu(chip, well, index);
  };
  chip.addEventListener('dragstart', (event) => {
    dragPayload = { kind: 'chip', well, index };
    chip.classList.add('dragging');
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/plain', isValue ? item.column : item);
  });
  chip.addEventListener('dragend', () => {
    chip.classList.remove('dragging');
    dragPayload = null;
    clearDropMarkers();
  });
  return chip;
}

function openChipMenu(anchor, well, index) {
  const item = wellItems(well)[index];
  const sections = [];

  if (well === 'values') {
    // "Count of rows" has no column behind it, so there is nothing to re-summarise.
    if (item.agg !== 'count_rows') {
      const column = state.dataset.columns.find((c) => c.name === item.column);
      const category = column ? column.category : 'other';
      sections.push({
        title: 'Summarise by',
        items: (AGGS || []).filter((agg) => agg.id !== 'count_rows').map((agg) => ({
          label: agg.label,
          on: agg.id === item.agg,
          disabled: !aggAllowed(agg, category),
          onSelect: () => { item.agg = agg.id; renderWells(); runPivot(); },
        })),
      });
    }
    const sort = state.pivot.sort;
    sections.push({
      title: 'Sort rows',
      items: [
        {
          label: 'Largest first',
          on: Boolean(sort && sort.value_index === index && sort.descending),
          onSelect: () => setPivotSort(index, true),
        },
        {
          label: 'Smallest first',
          on: Boolean(sort && sort.value_index === index && !sort.descending),
          onSelect: () => setPivotSort(index, false),
        },
        { label: 'By row label (default)', on: !sort, onSelect: () => setPivotSort(null) },
      ],
    });
  }

  const moves = [];
  if (index > 0) {
    moves.push({ label: 'Move earlier', onSelect: () => moveField(well, index, index - 1) });
  }
  if (index < wellItems(well).length - 1) {
    moves.push({ label: 'Move later', onSelect: () => moveField(well, index, index + 1) });
  }
  for (const target of ['rows', 'columns', 'values']) {
    if (target === well) continue;
    moves.push({
      label: `Move to ${target[0].toUpperCase()}${target.slice(1)}`,
      onSelect: () => moveToWell(well, index, target, wellItems(target).length),
    });
  }
  moves.push({
    label: 'Remove',
    onSelect: () => {
      wellItems(well).splice(index, 1);
      renderWells();
      runPivot();
    },
  });
  sections.push({ title: well === 'values' ? 'Field' : item, items: moves });

  showMenu(anchor, sections);
}

function setPivotSort(valueIndex, descending) {
  state.pivot.sort = valueIndex === null
    ? null
    : { by: 'value', value_index: valueIndex, descending: Boolean(descending) };
  runPivot();
}

function moveField(well, from, to) {
  const items = wellItems(well);
  items.splice(to, 0, items.splice(from, 1)[0]);
  if (well === 'values' && state.pivot.sort) state.pivot.sort = null;
  renderWells();
  runPivot();
}

/* Moving a field between wells has to translate it: the Values well holds
   { column, agg } objects, the other two hold plain column names. */
function moveToWell(fromWell, index, toWell, at) {
  const item = wellItems(fromWell).splice(index, 1)[0];
  const name = fromWell === 'values' ? item.column : item;
  if (!name) {
    renderWells();
    return;
  }
  addToWell(toWell, name, at);
}

function addToWell(well, columnName, at = null) {
  const column = state.dataset.columns.find((c) => c.name === columnName);
  if (!column) return;
  const items = wellItems(well);

  if (well === 'values') {
    const entry = { column: columnName, agg: defaultAgg(column) };
    items.splice(at === null ? items.length : at, 0, entry);
  } else {
    if (column.category === 'complex') {
      toast(`${columnName} is a nested column — it can only be counted in Values.`, 'error');
      return;
    }
    // A field groups in one direction only, exactly as in Excel.
    for (const other of ['rows', 'columns']) {
      const existing = state.pivot[other].indexOf(columnName);
      if (existing !== -1) {
        state.pivot[other].splice(existing, 1);
        if (other === well && at !== null && existing < at) at -= 1;
      }
    }
    items.splice(at === null ? items.length : at, 0, columnName);
  }
  state.pivot.sort = null;
  renderWells();
  runPivot();
}

/* -- drag and drop -- */

let dragPayload = null;

function clearDropMarkers() {
  for (const node of document.querySelectorAll('.drop-marker')) node.remove();
  for (const node of document.querySelectorAll('.well.over')) node.classList.remove('over');
}

/* Where in the chip row the pointer sits, so a drop lands between two chips. */
function dropIndexFor(zone, clientX) {
  const chips = [...zone.querySelectorAll('.field-chip')];
  for (let index = 0; index < chips.length; index += 1) {
    const box = chips[index].getBoundingClientRect();
    if (clientX < box.left + box.width / 2) return index;
  }
  return chips.length;
}

function wireWellDnD() {
  for (const well of ['rows', 'columns', 'values']) {
    const zone = $(WELL_IDS[well]);
    const shell = $(`well-${well}`);

    shell.addEventListener('dragover', (event) => {
      if (!dragPayload) return;
      event.preventDefault();
      event.dataTransfer.dropEffect = dragPayload.kind === 'chip' ? 'move' : 'copy';
      clearDropMarkers();
      shell.classList.add('over');
      const at = dropIndexFor(zone, event.clientX);
      const marker = el('span', 'drop-marker');
      const chips = zone.querySelectorAll('.field-chip');
      if (at >= chips.length) zone.appendChild(marker);
      else zone.insertBefore(marker, chips[at]);
    });
    shell.addEventListener('dragleave', (event) => {
      if (!shell.contains(event.relatedTarget)) {
        shell.classList.remove('over');
        for (const node of zone.querySelectorAll('.drop-marker')) node.remove();
      }
    });
    shell.addEventListener('drop', (event) => {
      event.preventDefault();
      const payload = dragPayload;
      const at = dropIndexFor(zone, event.clientX);
      clearDropMarkers();
      dragPayload = null;
      if (!payload) return;
      if (payload.kind === 'column') {
        addToWell(well, payload.name, at);
      } else if (payload.well === well) {
        const target = at > payload.index ? at - 1 : at;
        if (target !== payload.index) moveField(well, payload.index, target);
      } else {
        moveToWell(payload.well, payload.index, well, at);
      }
    });
  }
}

/* -- running the pivot -- */

/* The placeholder doubles as the error surface, so its own copy is kept. */
let pivotPlaceholderHTML = null;

function showPivotPlaceholder(title, detail) {
  const node = $('pivot-empty');
  if (pivotPlaceholderHTML === null) pivotPlaceholderHTML = node.innerHTML;
  if (title) {
    node.innerHTML = '';
    node.append(el('div', 'empty-title', title), el('div', 'empty-sub', detail || ''));
  } else {
    node.innerHTML = pivotPlaceholderHTML;
  }
  node.hidden = false;
}

async function runPivot() {
  if (!state.dataset || state.mode !== 'pivot') return;
  const pivotState = state.pivot;
  await loadAggregations();

  if (pivotIsEmpty()) {
    pivotState.result = null;
    pivotState.error = null;
    $('pivot-head').innerHTML = '';
    $('pivot-body').innerHTML = '';
    showPivotPlaceholder();
    $('pivot-overlay').hidden = true;
    renderPivotReadout();
    return;
  }

  const token = ++pivotState.token;
  $('pivot-overlay').hidden = false;
  $('pivot-empty').hidden = true;
  const started = performance.now();

  try {
    const result = await api('/api/pivot', pivotPayload());
    if (token !== pivotState.token) return;
    pivotState.result = result;
    pivotState.error = null;
    pivotState.ms = performance.now() - started;
    renderPivotGrid(result);
  } catch (error) {
    if (token !== pivotState.token) return;
    pivotState.result = null;
    pivotState.error = error.message;
    $('pivot-head').innerHTML = '';
    $('pivot-body').innerHTML = '';
    showPivotPlaceholder('This pivot cannot be built', error.message);
  } finally {
    if (token === pivotState.token) {
      $('pivot-overlay').hidden = true;
      renderPivotReadout();
    }
  }
}

function pivotPayload() {
  const pivotState = state.pivot;
  return {
    dataset_id: state.dataset.id,
    rows: [...pivotState.rows],
    columns: [...pivotState.columns],
    values: pivotState.values.map((v) => ({ column: v.column || null, agg: v.agg })),
    filters: activeFilters(),
    subtotals: pivotState.subtotals,
    row_totals: pivotState.rowTotals,
    column_totals: pivotState.columnTotals,
    repeat_labels: pivotState.repeatLabels,
    sort: pivotState.sort,
  };
}

/* -- the pivot grid -- */

const PIVOT_HEADER_ROW_HEIGHT = 30;
const PIVOT_LABEL_WIDTH = 168;

function fmtPivotCell(value, integral) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return String(value);
    return integral ? nf.format(value) : num2.format(value);
  }
  if (typeof value === 'boolean') return String(value);
  return String(value).replace('T', ' ');
}

/* Decimals are decided per column, not per cell: a column of whole numbers
   stays whole, and one with any fraction shows two places all the way down. */
function integralLeaves(result) {
  return result.leaves.map((leaf, index) => {
    if (leaf.is_count) return true;
    return result.rows.every((record) => {
      const value = record.cells[index];
      return value === null || value === undefined
        || typeof value !== 'number' || Number.isInteger(value);
    });
  });
}

function renderPivotGrid(result) {
  const head = $('pivot-head');
  const body = $('pivot-body');
  head.innerHTML = '';
  body.innerHTML = '';
  $('pivot-empty').hidden = true;

  const rowFields = result.row_fields.map((f) => f.name);
  const columnFields = result.column_fields.map((f) => f.name);
  const labelCount = Math.max(1, rowFields.length);
  const leaves = result.leaves;
  const lastLevel = result.header.length - 1;

  result.header.forEach((cells, level) => {
    const tr = el('tr');
    const corner = el('th', 'rl');
    corner.colSpan = labelCount;
    corner.style.left = '0';
    corner.style.top = `${level * PIVOT_HEADER_ROW_HEIGHT}px`;
    if (columnFields[level]) {
      corner.textContent = columnFields[level];
      corner.classList.add('field-name');
      corner.title = `Column field: ${columnFields[level]}`;
    }
    tr.appendChild(corner);

    let leafAt = 0;
    for (const cell of cells) {
      const th = el('th', cell.kind === 'total' ? 'total-head' : null);
      th.colSpan = cell.span || 1;
      th.style.top = `${level * PIVOT_HEADER_ROW_HEIGHT}px`;
      th.textContent = cell.skip ? '' : cell.label;
      th.title = cell.label;
      if (level === lastLevel) {
        // The deepest header level lines up one-to-one with the leaf columns,
        // so clicking it can sort the pivot by that measure.
        const leaf = leaves[leafAt];
        if (leaf) {
          th.style.cursor = 'pointer';
          th.title = `${cell.label}\nClick to sort rows by ${leaf.label}`;
          th.onclick = () => cyclePivotSort(leaf.value_index);
          const sort = state.pivot.sort;
          if (sort && sort.value_index === leaf.value_index) {
            th.appendChild(el('span', 'th-sort', sort.descending ? ' ▼' : ' ▲'));
          }
        }
      }
      leafAt += cell.span || 1;
      tr.appendChild(th);
    }
    head.appendChild(tr);
  });

  // A final header row naming the row fields, over the label columns.
  const fieldRow = el('tr');
  for (let index = 0; index < labelCount; index += 1) {
    const th = el('th', 'rl field-name', rowFields[index] || '');
    th.style.left = `${index * PIVOT_LABEL_WIDTH}px`;
    th.style.top = `${result.header.length * PIVOT_HEADER_ROW_HEIGHT}px`;
    th.style.minWidth = `${PIVOT_LABEL_WIDTH}px`;
    fieldRow.appendChild(th);
  }
  const filler = el('th');
  filler.colSpan = leaves.length;
  filler.style.top = `${result.header.length * PIVOT_HEADER_ROW_HEIGHT}px`;
  fieldRow.appendChild(filler);
  head.appendChild(fieldRow);

  const integral = integralLeaves(result);
  const repeatLabels = state.pivot.repeatLabels;
  let previous = [];
  for (const record of result.rows) {
    const tr = el('tr', record.kind === 'data' ? null : record.kind);
    for (let index = 0; index < labelCount; index += 1) {
      const text = record.labels[index] === undefined ? '' : record.labels[index];
      // Repeat an outer label only when it changes, the way Excel does -- unless
      // "Repeat labels" is on, which prints it on every row so each one stands alone.
      const repeated = !repeatLabels && record.kind === 'data'
        && index < labelCount - 1 && previous[index] === text;
      const td = el('td', 'rl', repeated ? '' : text);
      td.style.left = `${index * PIVOT_LABEL_WIDTH}px`;
      td.style.minWidth = `${PIVOT_LABEL_WIDTH}px`;
      td.title = text;
      tr.appendChild(td);
    }
    previous = record.kind === 'data' ? record.labels : [];

    record.cells.forEach((value, index) => {
      const leaf = leaves[index];
      const td = el('td', `num${leaf && leaf.kind === 'total' ? ' total-col' : ''}`);
      if (value === null || value === undefined) {
        td.classList.add('blank');
        td.textContent = '–';
      } else {
        td.textContent = fmtPivotCell(value, integral[index]);
      }
      tr.appendChild(td);
    });
    body.appendChild(tr);
  }

  renderPivotReadout();
}

function cyclePivotSort(valueIndex) {
  const sort = state.pivot.sort;
  if (!sort || sort.value_index !== valueIndex) setPivotSort(valueIndex, true);
  else if (sort.descending) setPivotSort(valueIndex, false);
  else setPivotSort(null);
}

function renderPivotReadout() {
  const readout = $('pivot-readout');
  const result = state.pivot.result;
  readout.innerHTML = '';

  if (!result) {
    readout.appendChild(el('span', 'counting',
      pivotIsEmpty()
        ? 'Add a field to Rows (or Columns) and one to Values.'
        : (state.pivot.error || 'Building…')));
    $('pivot-status-left').textContent = '';
    $('pivot-status-right').textContent = '';
    $('btn-pivot-export').disabled = true;
    return;
  }

  $('btn-pivot-export').disabled = false;
  readout.appendChild(el('b', null, fmtNum(result.total_row_groups)));
  readout.appendChild(document.createTextNode(
    ` row group${result.total_row_groups === 1 ? '' : 's'}`));
  if (result.column_groups) {
    readout.appendChild(document.createTextNode(' × '));
    readout.appendChild(el('b', null, fmtNum(result.column_groups)));
    readout.appendChild(document.createTextNode(' column groups'));
  }
  if (result.truncated) {
    readout.appendChild(document.createTextNode(' · '));
    readout.appendChild(el('span', 'pivot-note',
      `showing the first ${fmtNum(result.row_groups)} — totals still cover every matching row`));
  }
  if (result.sort_limited) {
    readout.appendChild(document.createTextNode(' · '));
    readout.appendChild(el('span', 'pivot-note',
      'too many groups to rank them all — this sorts the groups shown'));
  }

  const source = state.matched === null ? state.dataset.row_count : state.matched;
  $('pivot-status-left').textContent =
    `Aggregated ${fmtNum(source)} of ${fmtNum(state.dataset.row_count)} rows`;
  $('pivot-status-right').textContent =
    `pivot ${Math.round(state.pivot.ms)} ms · ${result.values.length} measure${
      result.values.length === 1 ? '' : 's'}`;
}

/* -- add-field menus and the toolbar -- */

function openAddMenu(anchor, well) {
  loadAggregations().then(() => {
    const used = new Set([...state.pivot.rows, ...state.pivot.columns]);
    const items = state.dataset.columns
      .filter((column) => (well === 'values' ? true : !used.has(column.name)))
      .filter((column) => (well === 'values' ? true : column.category !== 'complex'))
      .map((column) => ({
        label: column.name,
        hint: column.type,
        onSelect: () => addToWell(well, column.name),
      }));

    const sections = [{ title: `Add to ${well}`, items, searchable: true }];
    if (well === 'values') {
      sections.unshift({
        items: [{
          label: 'Count of rows',
          onSelect: () => {
            state.pivot.values.push({ column: null, agg: 'count_rows' });
            renderWells();
            runPivot();
          },
        }],
      });
    }
    showMenu(anchor, sections);
  });
}

function swapPivotAxes() {
  const { rows, columns } = state.pivot;
  state.pivot.rows = columns;
  state.pivot.columns = rows;
  state.pivot.sort = null;
  renderWells();
  runPivot();
}

function clearPivot() {
  state.pivot.rows = [];
  state.pivot.columns = [];
  state.pivot.values = [];
  state.pivot.sort = null;
  renderWells();
  runPivot();
}

/* A new file resets the pivot options, so put the checkboxes back where the
   fresh state says they are. */
function syncPivotOptions() {
  $('opt-subtotals').checked = state.pivot.subtotals;
  $('opt-row-totals').checked = state.pivot.rowTotals;
  $('opt-grand').checked = state.pivot.columnTotals;
  $('opt-repeat-labels').checked = state.pivot.repeatLabels;
}

function wirePivot() {
  $('tab-data').onclick = () => setMode('data');
  $('tab-pivot').onclick = () => setMode('pivot');

  for (const button of document.querySelectorAll('[data-add]')) {
    button.onclick = (event) => {
      event.stopPropagation();
      openAddMenu(button, button.dataset.add);
    };
  }

  const toggle = (id, key) => {
    $(id).onchange = () => { state.pivot[key] = $(id).checked; runPivot(); };
  };
  toggle('opt-subtotals', 'subtotals');
  toggle('opt-row-totals', 'rowTotals');
  toggle('opt-grand', 'columnTotals');

  // Repeating the labels only changes how the rows already in hand are drawn,
  // so it redraws instead of asking the server for the pivot again.
  $('opt-repeat-labels').onchange = () => {
    state.pivot.repeatLabels = $('opt-repeat-labels').checked;
    if (state.pivot.result) renderPivotGrid(state.pivot.result);
  };

  $('btn-pivot-swap').onclick = swapPivotAxes;
  $('btn-pivot-clear').onclick = clearPivot;
  $('btn-pivot-export').onclick = openDrawer;

  wireWellDnD();

  document.addEventListener('mousedown', (event) => {
    if (!$('menu').hidden && !$('menu').contains(event.target)) closeMenu();
  });
}

/* ─────────────────────────── theme ─────────────────────────── */

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  localStorage.setItem('pqs-theme', theme);
  $('btn-theme').textContent = theme === 'dark' ? '☀' : '☾';
  $('btn-theme').title = `Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`;
}

/* ─────────────────────────── wiring ─────────────────────────── */

function wire() {
  applyTheme(localStorage.getItem('pqs-theme') || 'dark');
  $('btn-theme').onclick = () =>
    applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark');

  $('btn-open-path').onclick = () => openPath($('path-input').value);
  $('path-input').onkeydown = (event) => { if (event.key === 'Enter') openPath($('path-input').value); };
  $('btn-browse').onclick = () => openBrowser(browseCurrent);
  $('browse-close').onclick = closeBrowser;
  $('browse-scrim').onclick = closeBrowser;
  $('browse-open-dir').onclick = () => pickBrowsed(browseCurrent);
  $('btn-union-toggle').onclick = () => toggleUnion(true);
  $('btn-union-cancel').onclick = () => { toggleUnion(false); setUnionError(''); };
  $('btn-union-add').onclick = () => {
    const row = unionRow();
    $('union-list').appendChild(row);
    syncUnionRows();
    row.querySelector('input').focus();
  };
  $('btn-union-open').onclick = openUnion;
  $('btn-change-file').onclick = unmountDataset;

  const zone = $('drop-zone');
  zone.addEventListener('dragover', (event) => {
    event.preventDefault();
    zone.classList.add('dragging');
  });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragging'));
  zone.addEventListener('drop', (event) => {
    event.preventDefault();
    zone.classList.remove('dragging');
    const file = event.dataTransfer.files[0];
    if (file) uploadFile(file);
  });
  zone.addEventListener('click', (event) => {
    if (event.target === zone) $('file-picker').click();
  });
  $('file-picker').onchange = (event) => {
    if (event.target.files[0]) uploadFile(event.target.files[0]);
    event.target.value = '';
  };

  $('column-search').oninput = debounce((event) => {
    state.columnFilter = event.target.value;
    renderRail();
  }, 140);
  $('btn-show-all').onclick = () => {
    state.hidden.clear();
    renderRail();
    renderExportColumns();
    refresh({ countUnchanged: true });
  };
  $('btn-hide-all').onclick = () => {
    // Keep one column so the grid still has something to render.
    state.dataset.columns.slice(1).forEach((c) => state.hidden.add(c.name));
    renderRail();
    renderExportColumns();
    refresh({ countUnchanged: true });
  };

  $('btn-clear-filters').onclick = () => {
    state.filters = {};
    state.page = 0;
    renderChips();
    renderRail();
    refresh();
  };

  $('page-size').onchange = (event) => {
    state.pageSize = parseInt(event.target.value, 10);
    state.page = 0;
    refresh({ countUnchanged: true });
  };
  $('btn-prev').onclick = () => {
    if (state.page > 0) { state.page -= 1; refresh({ countUnchanged: true }); }
  };
  $('btn-next').onclick = () => { state.page += 1; refresh({ countUnchanged: true }); };

  $('pop-close').onclick = closePopover;
  $('pop-cancel').onclick = closePopover;
  $('pop-apply').onclick = applyFilter;
  $('pop-clear').onclick = clearCurrentFilter;

  wirePivot();
  $('btn-export').onclick = openDrawer;
  $('drawer-close').onclick = closeDrawer;
  $('scrim').onclick = closeDrawer;
  $('export-format').onchange = syncFormatFields;
  $('btn-start-export').onclick = startExport;
  $('btn-cancel-export').onclick = cancelExport;

  document.addEventListener('mousedown', (event) => {
    const popover = $('popover');
    if (popover.hidden) return;
    if (popover.contains(event.target)) return;
    if (pop.anchor && pop.anchor.contains(event.target)) return;
    closePopover();
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      if (!$('menu').hidden) closeMenu();
      else if (!$('popover').hidden) closePopover();
      else if (!$('browse-modal').hidden) closeBrowser();
      else if (!$('drawer').hidden) closeDrawer();
    }
    if (event.key === 'Enter' && !$('popover').hidden && event.target.tagName !== 'SELECT') {
      applyFilter();
    }
  });

  window.addEventListener('resize', debounce(() => {
    if (!$('popover').hidden && pop.anchor) positionPopover(pop.anchor);
  }, 120));

  loadRecents();
}

wire();
