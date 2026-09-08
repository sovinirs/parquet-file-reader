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
  diff: newDiffState(),
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

function setOpenError(message) {
  const node = $('open-error');
  node.textContent = message || '';
  node.hidden = !message;
}

function mountDataset(dataset) {
  state.dataset = dataset;
  state.filters = {};
  state.hidden = new Set();
  state.sort = null;
  state.page = 0;
  state.matched = dataset.row_count;
  state.pivot = newPivotState();
  state.diff = newDiffState();
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
    body.appendChild(el('div', 'col-type', column.type));
    body.title = `${column.name} · ${column.type}`;

    const actions = el('div', 'col-actions');

    const filterBtn = el('button', `icon-btn${isFiltered ? ' on' : ''}`, '⌄');
    filterBtn.title = isFiltered ? 'Edit filter' : 'Filter this column';
    filterBtn.onclick = (event) => {
      event.stopPropagation();
      openFilterPopover(column, filterBtn);
    };

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

    actions.append(filterBtn, eyeBtn);
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
    label.appendChild(Object.assign(el('b'), { textContent: spec.column }));
    label.appendChild(Object.assign(el('i'), { textContent: ` ${op} ` }));
    label.appendChild(document.createTextNode(value));
    label.title = `${spec.column} ${op} ${value}\nClick to edit`;
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
    label.append(nameRow, el('div', 'th-type', column.type));
    label.title = `${column.name} · ${column.type}\nClick to sort`;
    label.onclick = () => cycleSort(column.name);

    const filterBtn = el('button', `th-filter${isFiltered ? ' active' : ''}`, '▼');
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
  pop.column = column;
  pop.anchor = anchor;
  pop.draft = JSON.parse(JSON.stringify(state.filters[column.name] || { column: column.name }));
  pop.values = [];
  pop.selected = new Set();
  pop.stats = null;
  pop.search = '';

  const tabs = tabsFor(column.category);
  const existingOp = pop.draft.op;
  if (existingOp === 'in' || existingOp === 'not_in') pop.tab = 'values';
  else if (existingOp === 'between' || existingOp === 'not_between') pop.tab = tabs.includes('range') ? 'range' : 'condition';
  else if (existingOp) pop.tab = 'condition';
  else pop.tab = tabs[0];

  if (pop.draft.values) {
    for (const value of pop.draft.values) pop.selected.add(JSON.stringify(value ?? null));
  }

  $('pop-title').textContent = column.name;
  $('pop-type').textContent = column.type;
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
  const node = $('popover');
  const box = anchor.getBoundingClientRect();
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
  searchBox.placeholder = 'Search values…';
  searchBox.value = pop.search;
  searchBox.oninput = debounce(() => {
    pop.search = searchBox.value;
    loadValues(pop.search, list);
  }, 260);
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

  if (pop.truncated) {
    body.appendChild(el(
      'div', 'value-note',
      `Showing the ${pop.values.length} most common values. Search above, or use the Condition tab to match a wider set.`
    ));
  }
}

async function loadValues(search = '', listNode = null) {
  const column = pop.column;
  try {
    const data = await api('/api/values', {
      dataset_id: state.dataset.id,
      column: column.name,
      filters: activeFilters(),
      search,
      limit: 300,
    });
    if (!pop.column || pop.column.name !== column.name) return;
    pop.values = data.values;
    pop.truncated = data.truncated;
    if (listNode && listNode.isConnected) renderValueRows(listNode);
    else renderPopBody();
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
    });
    if (!pop.column || pop.column.name !== column.name) return null;
    pop.stats = data;
    return data;
  } catch (_) {
    return null;
  }
}

/* -- condition tab -- */

function opsFor(category) {
  const nullary = [['is_null', 'is blank'], ['is_not_null', 'is not blank']];
  if (category === 'numeric' || category === 'temporal') {
    return [['eq', '='], ['ne', '≠'], ['gt', '>'], ['gte', '≥'], ['lt', '<'], ['lte', '≤'], ...nullary];
  }
  if (category === 'boolean') return [['eq', 'is'], ...nullary];
  return [
    ['contains', 'contains'], ['not_contains', 'does not contain'],
    ['starts_with', 'starts with'], ['ends_with', 'ends with'],
    ['eq', 'equals'], ['ne', 'does not equal'], ['regex', 'matches regex'],
    ['is_empty', 'is empty'], ['is_not_empty', 'is not empty'], ...nullary,
  ];
}

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
    const needsValue = !['is_null', 'is_not_null', 'is_empty', 'is_not_empty'].includes(select.value);
    valueField.style.display = needsValue ? '' : 'none';
    if (caseBox) {
      caseBox.parentElement.style.display =
        ['contains', 'not_contains', 'starts_with', 'ends_with'].includes(select.value) ? '' : 'none';
    }
  };
  select.onchange = () => { pop.draft.op = select.value; syncVisibility(); };
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

  if (pop.tab === 'values') {
    draft.op = draft.op === 'not_in' ? 'not_in' : 'in';
    draft.values = [...pop.selected].map((key) => JSON.parse(key));
    delete draft.value;
    delete draft.value2;
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
  } else {
    delete draft.values;
    delete draft.value2;
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

function isDiffExport() {
  return state.mode === 'diff' && !!state.diff.jobId && state.diff.results.length > 0;
}

function openDrawer() {
  const pivotMode = isPivotExport();
  const diffMode = isDiffExport();
  $('drawer-title').textContent = diffMode ? 'Export the difference analysis'
    : (pivotMode ? 'Export pivot to Excel' : 'Export filtered data');
  // A pivot is always an .xlsx of exactly the table on screen: no format
  // choice, no row limit, and the columns come from the Values well. The
  // analysis keeps the format choice -- Excel for the full workbook, CSV for
  // the summary alone -- but nothing else applies to it either.
  $('field-format').hidden = pivotMode;
  $('field-limit').hidden = pivotMode || diffMode;
  $('field-columns').hidden = pivotMode || diffMode;
  setDiffFormatOptions(diffMode);
  $('export-sheet').value = diffMode ? 'Difference analysis'
    : (pivotMode ? 'Pivot' : ($('export-sheet').value || 'Data'));
  $('btn-start-export').textContent = diffMode ? 'Export analysis'
    : (pivotMode ? 'Export pivot' : 'Start export');
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

  if (isDiffExport()) {
    renderDiffExportSummary(node);
    return;
  }
  if (isPivotExport()) {
    renderPivotExportSummary(node);
    return;
  }

  const rows = [
    ['Source', state.dataset.name],
    ['Filters applied', String(activeFilters().length)],
    ['Columns', `${visibleColumns().length} of ${state.dataset.columns.length}`],
  ];
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

function renderDiffExportSummary(node) {
  const diff = state.diff;
  const counts = {};
  for (const record of diff.results) counts[record.verdict] = (counts[record.verdict] || 0) + 1;
  const rows = [
    ['Source', state.dataset.name],
    ['Grouped by', diff.key],
    ['Explained by', diff.explain || 'nothing — differences are not attributed'],
    ['Assets analysed', diff.plan ? fmtNum(diff.plan.assets) : '—'],
    ['Columns excluded', diff.plan ? fmtNum((diff.plan.excluded || []).length) : '—'],
  ];
  for (const [label, value] of rows) {
    const row = el('div', 'summary-row');
    row.append(el('span', null, label), el('span', null, value));
    node.appendChild(row);
  }
  const emph = el('div', 'summary-row emph');
  emph.append(el('span', null, 'Columns analysed'), el('span', null, fmtNum(diff.results.length)));
  node.appendChild(emph);

  const unexplained = counts.TRUE_DIFF_OTHER || 0;
  if (unexplained) {
    const note = el('div', 'notice',
      `${fmtNum(unexplained)} column${unexplained === 1 ? '' : 's'} vary in a way the `
      + `${diff.explain || 'explanatory'} column does not explain. The workbook carries example `
      + `rows for those so a reviewer can see the actual cases.`);
    note.hidden = false;
    node.appendChild(note);
  }
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
  if (isPivotExport() || isDiffExport()) {
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

/* The analysis writes a workbook or a summary CSV; the other two formats mean
   nothing for it, so they leave the list rather than failing on the server. */
function setDiffFormatOptions(diffMode) {
  const select = $('export-format');
  for (const option of select.options) {
    option.hidden = diffMode && !['xlsx', 'csv'].includes(option.value);
  }
  if (diffMode && !['xlsx', 'csv'].includes(select.value)) select.value = 'xlsx';
  select.options[1].textContent = diffMode
    ? 'CSV (.csv) — the summary table only'
    : 'CSV (.csv) — fastest for huge results';
}

function syncFormatFields() {
  const isExcel = isPivotExport() || $('export-format').value === 'xlsx';
  $('excel-only').style.display = isExcel ? '' : 'none';
  updateExcelNotice();
}

async function startExport() {
  const pivotMode = isPivotExport();
  const diffMode = isDiffExport();
  const limitRaw = $('export-limit').value;
  const payload = diffMode ? {
    format: $('export-format').value,
    include_manifest: $('export-manifest').checked,
    sheet_name: $('export-sheet').value.trim() || 'Difference analysis',
  } : pivotMode ? {
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
  };

  $('btn-start-export').disabled = true;
  $('btn-download').hidden = true;
  $('progress-block').hidden = false;
  $('progress-fill').className = 'progress-fill indeterminate';
  $('progress-label').textContent = 'Starting…';
  $('progress-detail').textContent = '';

  try {
    const endpoint = diffMode ? `/api/diff/export/${state.diff.jobId}`
      : (pivotMode ? '/api/pivot/export' : '/api/export');
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

async function openBrowser(path) {
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
      button.onclick = () => openBrowser(data.parent);
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
        if (entry.is_dir) openBrowser(entry.path);
        else { closeBrowser(); openPath(entry.path); }
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

const EXPORT_LABELS = { pivot: 'Export pivot', diff: 'Export analysis', data: 'Export' };

function applyMode(mode) {
  state.mode = mode;
  for (const name of ['data', 'pivot', 'diff']) {
    $(`tab-${name}`).classList.toggle('active', mode === name);
    $(`${name}-view`).hidden = mode !== name;
  }
  $('btn-export').textContent = EXPORT_LABELS[mode] || 'Export';
  if (mode === 'pivot') {
    loadAggregations().then(renderWells);
    renderWells();
  }
  if (mode === 'diff') {
    // Building the config bar is all that happens on arrival: the analysis is
    // minutes of work, so it waits to be asked for.
    loadDiffDefaults().then(buildDiffConfig);
    buildDiffConfig();
  }
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
  $('browse-open-dir').onclick = () => { closeBrowser(); openPath(browseCurrent); };
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
  wireDiff();
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

/* ═══════════════════════ difference analysis ═══════════════════════ */

/* One run's worth of state. `results` grows while the job is still going, which
   is what lets the table fill in column by column instead of after five minutes
   of a blank screen. */
function newDiffState() {
  return {
    jobId: null,
    status: null,
    results: [],
    plan: null,
    key: null,
    explain: null,
    exclude: new Set(),
    skipBlank: true,
    useFilters: false,
    sample: 10000,
    verdictFilter: null,
    sort: { by: 'verdict', desc: false },
    search: '',
    selected: null,
    examples: null,
    exampleOffset: 0,
    assetJump: '',
    pollTimer: null,
    built: false,
  };
}

const VERDICT_LABELS = {
  ALL_BLANK: 'All blank',
  CONSTANT: 'Constant',
  SPARSE_SINGLE_VALUE: 'Sparse',
  TRUE_DIFF_BY_DEPR_AREA: 'Differs by area',
  TRUE_DIFF_OTHER: 'Unexplained',
  ERROR: 'Error',
};
/* Read order, worst last: the unexplained pile is what the tab exists for, so it
   sorts to the bottom where the eye lands after scanning. */
const VERDICT_ORDER = ['CONSTANT', 'SPARSE_SINGLE_VALUE', 'ALL_BLANK',
                       'TRUE_DIFF_BY_DEPR_AREA', 'TRUE_DIFF_OTHER', 'ERROR'];

let DIFF_DEFAULTS = null;

function loadDiffDefaults() {
  if (DIFF_DEFAULTS) return Promise.resolve(DIFF_DEFAULTS);
  return api('/api/diff/defaults', undefined, 'GET')
    .then((data) => { DIFF_DEFAULTS = data; return data; })
    .catch(() => ({ excluded_columns: [], example_page_size: 5 }));
}

/* -- config bar -- */

function buildDiffConfig() {
  const diff = state.diff;
  if (diff.built || !state.dataset) return;
  const columns = state.dataset.columns.filter((c) => c.category !== 'complex');

  const keySelect = $('diff-key');
  const explainSelect = $('diff-explain');
  keySelect.innerHTML = '';
  explainSelect.innerHTML = '';
  explainSelect.appendChild(el('option', null, 'None — do not attribute differences'));
  explainSelect.lastChild.value = '';
  for (const column of columns) {
    for (const select of [keySelect, explainSelect]) {
      const option = el('option', null, column.name);
      option.value = column.name;
      select.appendChild(option);
    }
  }
  // A best guess only — the point of the selects is that it is a guess.
  const guess = (needles) => (columns.find(
    (c) => needles.some((n) => c.name.toLowerCase().includes(n))) || {}).name;
  diff.key = guess(['asset_id', 'assetid', 'asset']) || (columns[0] || {}).name || null;
  diff.explain = guess(['depr', 'area']) || '';
  if (diff.explain === diff.key) diff.explain = '';
  keySelect.value = diff.key || '';
  explainSelect.value = diff.explain || '';

  const preset = new Set(DIFF_DEFAULTS ? DIFF_DEFAULTS.excluded_columns : []);
  diff.exclude = new Set(state.dataset.columns
    .filter((c) => preset.has(c.name)).map((c) => c.name));
  renderDiffExclusions();
  diff.built = true;
  renderDiffReadout();
}

function renderDiffExclusions() {
  const box = $('diff-exclude');
  const diff = state.diff;
  box.innerHTML = '';
  for (const column of state.dataset.columns) {
    if (column.name === diff.key) continue;
    const label = el('label', diff.exclude.has(column.name) ? 'on' : null);
    const input = el('input');
    input.type = 'checkbox';
    input.checked = diff.exclude.has(column.name);
    input.onchange = () => {
      if (input.checked) diff.exclude.add(column.name);
      else diff.exclude.delete(column.name);
      label.classList.toggle('on', input.checked);
      $('diff-exclude-count').textContent = `(${diff.exclude.size} selected)`;
    };
    label.append(input, el('span', null, column.name));
    box.appendChild(label);
  }
  $('diff-exclude-count').textContent = `(${diff.exclude.size} selected)`;
}

/* -- running -- */

async function runDiff() {
  const diff = state.diff;
  if (!diff.key) { toast('Choose the column that identifies one thing.', 'error'); return; }

  clearTimeout(diff.pollTimer);
  diff.results = [];
  diff.plan = null;
  diff.status = null;
  diff.selected = null;
  diff.examples = null;
  diff.verdictFilter = null;
  $('diff-detail').hidden = true;
  $('diff-findings').hidden = true;
  $('diff-empty').hidden = true;
  $('diff-head').innerHTML = '';
  $('diff-body').innerHTML = '';
  $('btn-diff-export').disabled = true;

  const payload = {
    dataset_id: state.dataset.id,
    key_column: diff.key,
    explain_column: diff.explain || null,
    exclude: [...diff.exclude],
    sample_assets: diff.sample || null,
    exclude_all_blank: diff.skipBlank,
    // Sending the filters *is* the opt-in: what the run covered is then exactly
    // what the manifest records.
    filters: diff.useFilters ? activeFilters() : [],
  };

  $('diff-progress').hidden = false;
  $('diff-progress-fill').className = 'progress-fill indeterminate';
  $('diff-progress-fill').style.width = '100%';
  $('diff-progress-label').textContent = 'Sizing the run…';
  $('diff-progress-detail').textContent = '';
  $('btn-diff-run').disabled = true;
  $('btn-diff-cancel').hidden = false;

  try {
    const job = await api('/api/diff/start', payload);
    diff.jobId = job.id;
    pollDiff();
  } catch (error) {
    finishDiff('error');
    $('diff-progress-label').textContent = error.message;
    toast(error.message, 'error');
  }
}

function pollDiff() {
  const diff = state.diff;
  clearTimeout(diff.pollTimer);
  const tick = async () => {
    let status;
    try {
      status = await api(`/api/diff/status/${diff.jobId}`, undefined, 'GET');
    } catch (error) {
      finishDiff('error');
      $('diff-progress-label').textContent = error.message;
      return;
    }
    diff.status = status;
    diff.plan = status.plan;
    diff.results = status.results || [];
    renderDiffProgress(status);
    renderDiffFindings();
    renderDiffTable();
    if (['queued', 'running'].includes(status.status)) {
      diff.pollTimer = setTimeout(tick, 600);
      return;
    }
    finishDiff(status.status);
    if (status.status === 'error') toast(status.error || 'The analysis failed.', 'error');
  };
  tick();
}

function renderDiffProgress(status) {
  const fill = $('diff-progress-fill');
  if (status.total && status.percent !== null) {
    fill.className = 'progress-fill';
    fill.style.width = `${status.percent}%`;
    $('diff-progress-label').textContent = status.current
      ? `Analysing ${status.current}…` : 'Analysing…';
    $('diff-progress-detail').textContent =
      `${status.done} of ${status.total} columns · ${fmtDuration(status.elapsed)}`;
  } else {
    $('diff-progress-label').textContent = 'Sizing the run…';
  }
}

function finishDiff(status) {
  const diff = state.diff;
  clearTimeout(diff.pollTimer);
  $('btn-diff-run').disabled = false;
  $('btn-diff-cancel').hidden = true;
  const fill = $('diff-progress-fill');
  fill.className = `progress-fill${status === 'error' ? ' error' : ' done'}`;
  fill.style.width = '100%';
  if (status === 'done' || status === 'cancelled') {
    $('btn-diff-export').disabled = !diff.results.length;
    const total = diff.status ? diff.status.total : diff.results.length;
    $('diff-progress-label').textContent = status === 'cancelled'
      ? `Cancelled after ${diff.results.length} of ${total} columns`
      : `Analysed ${diff.results.length} column${diff.results.length === 1 ? '' : 's'}`;
    $('diff-progress-detail').textContent = diff.status
      ? fmtDuration(diff.status.elapsed) : '';
  }
  renderDiffReadout();
}

async function cancelDiff() {
  const diff = state.diff;
  if (!diff.jobId) return;
  try { await api(`/api/diff/cancel/${diff.jobId}`, {}); } catch (error) { /* it ends anyway */ }
}

/* -- run-level findings -- */

function renderDiffFindings() {
  const node = $('diff-findings');
  const plan = state.diff.plan;
  if (!plan) { node.hidden = true; return; }
  node.innerHTML = '';
  node.hidden = false;

  const finding = (kind, mark, html) => {
    const row = el('div', `finding ${kind}`);
    row.append(el('span', 'finding-mark', mark));
    const text = el('div', 'finding-text');
    text.innerHTML = html;
    row.appendChild(text);
    node.appendChild(row);
  };

  const grain = plan.duplicate_grain || {};
  if (grain.checked && !grain.unique) {
    // Surfaced on its own because it changes what an unexplained difference
    // means — it may be duplicate rows rather than conflicting data.
    finding('warn', '⚠', `<b>The grain is not one row per (${state.diff.key}, ${grain.column}).</b> `
      + `${fmtNum(grain.duplicate_pairs)} pair${grain.duplicate_pairs === 1 ? '' : 's'} appear on `
      + `more than one row (${fmtNum(grain.extra_rows)} extra row${grain.extra_rows === 1 ? '' : 's'}, `
      + `worst case ${grain.max_rows_per_pair} rows for one pair). Read “Unexplained” below with `
      + `that in mind: some of it may be duplicate rows rather than conflicting data.`);
  } else if (grain.checked) {
    finding('good', '✓', `<b>One row per (${state.diff.key}, ${grain.column}).</b> `
      + `The extract is at the grain the analysis assumes, so an unexplained difference `
      + `is a real disagreement between rows.`);
  }

  const sampled = state.diff.sample;
  const blank = (plan.all_blank_columns || []).length;
  // Blank columns are only "set aside" when the run was told to set them aside;
  // otherwise they are results with an ALL_BLANK verdict, and saying they were
  // excluded would contradict the table right below.
  const blankNote = !blank ? ''
    : (state.diff.skipBlank
        ? ` (${fmtNum(blank)} of them blank in every row)`
        : ` · <b>${fmtNum(blank)}</b> blank in every row`);
  finding('', 'ⓘ', `<b>${fmtNum(plan.assets)} ${sampled ? 'sampled ' : ''}assets</b> across `
    + `${fmtNum(plan.rows)} rows · <b>${fmtNum((plan.columns || []).length)}</b> columns analysed, `
    + `<b>${fmtNum((plan.excluded || []).length)}</b> set aside` + blankNote
    + (sampled ? ` · <b>sample mode</b> — percentages are an estimate, not the final answer.` : '.'));
}

/* -- the results table -- */

const DIFF_COLUMNS = [
  { key: 'column', label: 'Column', sort: 'column' },
  { key: 'verdict', label: 'Verdict', sort: 'verdict' },
  { key: 'constant_pct', label: 'Constant %', sort: 'constant_pct', num: true },
  { key: 'sparse_pct', label: 'Sparse %', sort: 'sparse_pct', num: true },
  { key: 'differing_pct', label: 'Differing %', sort: 'differing_pct', num: true },
  { key: 'blank_pct', label: 'Blank %', sort: 'blank_pct', num: true },
  { key: 'max_distinct', label: 'Max distinct', sort: 'max_distinct', num: true },
  { key: 'recommendation', label: 'What to do when collapsing' },
];

function visibleDiffRows() {
  const diff = state.diff;
  const needle = diff.search.trim().toLowerCase();
  let rows = diff.results.filter((r) =>
    (!diff.verdictFilter || r.verdict === diff.verdictFilter)
    && (!needle || r.column.toLowerCase().includes(needle)));
  const { by, desc } = diff.sort;
  rows = rows.slice().sort((a, b) => {
    let x = a[by];
    let y = b[by];
    if (by === 'verdict') { x = VERDICT_ORDER.indexOf(x); y = VERDICT_ORDER.indexOf(y); }
    if (x === null || x === undefined) x = -1;
    if (y === null || y === undefined) y = -1;
    if (x < y) return desc ? 1 : -1;
    if (x > y) return desc ? -1 : 1;
    return a.column.localeCompare(b.column);
  });
  return rows;
}

function renderDiffTable() {
  const head = $('diff-head');
  const body = $('diff-body');
  const diff = state.diff;
  renderDiffChips();
  $('diff-toolbar').hidden = !diff.results.length;
  $('diff-empty').hidden = diff.results.length > 0;

  head.innerHTML = '';
  const tr = el('tr');
  for (const spec of DIFF_COLUMNS) {
    const th = el('th', spec.sort ? 'sortable' : null, spec.label);
    if (spec.sort) {
      th.onclick = () => {
        diff.sort = { by: spec.sort, desc: diff.sort.by === spec.sort ? !diff.sort.desc : true };
        renderDiffTable();
      };
      if (diff.sort.by === spec.sort) th.appendChild(el('span', 'th-sort', diff.sort.desc ? ' ▼' : ' ▲'));
    }
    tr.appendChild(th);
  }
  head.appendChild(tr);

  body.innerHTML = '';
  for (const record of visibleDiffRows()) {
    const row = el('tr', record.column === diff.selected ? 'selected' : null);
    row.onclick = () => openDiffColumn(record.column);
    for (const spec of DIFF_COLUMNS) {
      if (spec.key === 'verdict') {
        const td = el('td');
        td.appendChild(verdictTag(record.verdict));
        row.appendChild(td);
        continue;
      }
      if (spec.key === 'column') {
        row.appendChild(el('td', 'col-name', record.column));
        continue;
      }
      if (spec.key === 'recommendation') {
        row.appendChild(el('td', 'rec', record.error || record.recommendation));
        continue;
      }
      const value = record[spec.key];
      const td = el('td', spec.num ? 'num' : null);
      td.textContent = value === null || value === undefined ? '–'
        : (spec.key.endsWith('_pct') ? `${value.toFixed(1)}%` : fmtNum(value));
      if (value === null || value === undefined) td.classList.add('blank');
      row.appendChild(td);
    }
    body.appendChild(row);
  }
  renderDiffReadout();
}

function verdictTag(verdict) {
  const tag = el('span', `verdict-tag vt-${verdict}`);
  tag.append(el('span', `vt-dot vd-${verdict}`), document.createTextNode(
    VERDICT_LABELS[verdict] || verdict));
  return tag;
}

function renderDiffChips() {
  const node = $('diff-filter-chips');
  const diff = state.diff;
  node.innerHTML = '';
  const counts = {};
  for (const record of diff.results) counts[record.verdict] = (counts[record.verdict] || 0) + 1;

  const chip = (verdict, label, count) => {
    const item = el('div', 'verdict-chip');
    if (diff.verdictFilter === verdict) item.classList.add('active');
    else if (diff.verdictFilter) item.classList.add('muted');
    if (verdict) item.appendChild(el('span', `vc-dot vd-${verdict}`));
    item.append(el('span', null, label), el('span', 'vc-count', fmtNum(count)));
    item.onclick = () => {
      diff.verdictFilter = diff.verdictFilter === verdict ? null : verdict;
      renderDiffTable();
    };
    item.title = verdict ? `Show only ${label.toLowerCase()} columns` : 'Show every column';
    node.appendChild(item);
  };
  chip(null, 'All', diff.results.length);
  for (const verdict of VERDICT_ORDER) {
    if (counts[verdict]) chip(verdict, VERDICT_LABELS[verdict], counts[verdict]);
  }
}

function renderDiffReadout() {
  const diff = state.diff;
  const shown = diff.results.length ? visibleDiffRows().length : 0;
  $('diff-status-left').textContent = diff.results.length
    ? `${fmtNum(shown)} of ${fmtNum(diff.results.length)} columns shown`
    : (state.dataset ? `${state.dataset.columns.length} columns in this file` : '');
  const unexplained = diff.results.filter((r) => r.verdict === 'TRUE_DIFF_OTHER').length;
  $('diff-status-right').textContent = diff.results.length
    ? `${fmtNum(unexplained)} column${unexplained === 1 ? ' needs' : 's need'} a business rule`
    : '';
}

/* -- drill-down -- */

function openDiffColumn(column) {
  const diff = state.diff;
  diff.selected = column;
  diff.exampleOffset = 0;
  diff.assetJump = '';
  $('diff-asset-jump').value = '';
  $('diff-detail').hidden = false;
  renderDiffTable();
  loadDiffExamples();
}

async function loadDiffExamples() {
  const diff = state.diff;
  const record = diff.results.find((r) => r.column === diff.selected);
  if (!record) return;
  $('diff-detail-title').textContent = record.column;
  const sub = $('diff-detail-sub');
  sub.innerHTML = '';
  sub.appendChild(verdictTag(record.verdict));
  sub.append(document.createTextNode(' ' + (record.error || record.recommendation)));

  const body = $('diff-detail-body');
  body.innerHTML = '';
  body.appendChild(el('div', 'example-empty', 'Loading examples…'));

  const page = DIFF_DEFAULTS ? DIFF_DEFAULTS.example_page_size : 5;
  const query = new URLSearchParams({ limit: String(page), offset: String(diff.exampleOffset) });
  if (diff.assetJump) query.set('asset_id', diff.assetJump);
  try {
    diff.examples = await api(
      `/api/diff/examples/${diff.jobId}/${encodeURIComponent(record.column)}?${query}`,
      undefined, 'GET');
  } catch (error) {
    body.innerHTML = '';
    body.appendChild(el('div', 'example-empty', error.message));
    return;
  }
  renderDiffExamples();
}

function renderDiffExamples() {
  const diff = state.diff;
  const body = $('diff-detail-body');
  const found = diff.examples;
  body.innerHTML = '';
  $('diff-ex-page').textContent = String(Math.floor(diff.exampleOffset / 5) + 1);
  $('diff-ex-prev').disabled = diff.exampleOffset === 0 || !!diff.assetJump;
  $('diff-ex-next').disabled = !found || !found.has_more || !!diff.assetJump;

  if (!found || !found.assets.length) {
    body.appendChild(el('div', 'example-empty', diff.assetJump
      ? `No rows for ${diff.assetJump}. Check the ID, or clear the box to page through examples.`
      : 'No asset holds more than one value here — nothing to show.'));
    return;
  }

  for (const asset of found.assets) {
    const block = el('div', 'example-asset');
    const head = el('div', 'example-head');
    head.append(el('span', 'example-id', String(asset.asset)),
                el('span', 'example-note', asset.differs
                  ? `${asset.distinct_values.length} different values`
                  : 'rows agree'));
    block.appendChild(head);

    const table = el('table', 'example-table');
    const thead = el('tr');
    thead.append(el('th', null, found.explain_column || 'Row'), el('th', null, found.column));
    table.appendChild(thead);
    for (const row of asset.rows) {
      const tr = el('tr', asset.differs ? 'differs' : null);
      tr.appendChild(el('td', 'area', row.area === null ? '—' : String(row.area)));
      const td = el('td', 'val');
      if (row.blank) { td.classList.add('blank'); td.textContent = '(blank)'; }
      else td.textContent = String(row.value);
      tr.appendChild(td);
      table.appendChild(tr);
    }
    block.appendChild(table);
    body.appendChild(block);
  }
}

/* -- wiring -- */

function wireDiff() {
  $('tab-diff').onclick = () => setMode('diff');

  $('diff-key').onchange = () => {
    state.diff.key = $('diff-key').value;
    state.diff.exclude.delete(state.diff.key);
    renderDiffExclusions();
  };
  $('diff-explain').onchange = () => { state.diff.explain = $('diff-explain').value; };
  $('diff-sample').onchange = () => {
    state.diff.sample = parseInt($('diff-sample').value, 10) || null;
  };
  $('diff-skip-blank').onchange = () => { state.diff.skipBlank = $('diff-skip-blank').checked; };
  $('diff-use-filters').onchange = () => { state.diff.useFilters = $('diff-use-filters').checked; };

  $('btn-diff-run').onclick = runDiff;
  $('btn-diff-cancel').onclick = cancelDiff;
  $('btn-diff-export').onclick = openDrawer;

  $('diff-search').oninput = () => {
    state.diff.search = $('diff-search').value;
    renderDiffTable();
  };
  $('diff-detail-close').onclick = () => {
    $('diff-detail').hidden = true;
    state.diff.selected = null;
    renderDiffTable();
  };
  $('diff-ex-prev').onclick = () => {
    const page = DIFF_DEFAULTS ? DIFF_DEFAULTS.example_page_size : 5;
    state.diff.exampleOffset = Math.max(0, state.diff.exampleOffset - page);
    loadDiffExamples();
  };
  $('diff-ex-next').onclick = () => {
    const page = DIFF_DEFAULTS ? DIFF_DEFAULTS.example_page_size : 5;
    state.diff.exampleOffset += page;
    loadDiffExamples();
  };
  let jumpTimer = null;
  $('diff-asset-jump').oninput = () => {
    clearTimeout(jumpTimer);
    jumpTimer = setTimeout(() => {
      state.diff.assetJump = $('diff-asset-jump').value.trim();
      state.diff.exampleOffset = 0;
      loadDiffExamples();
    }, 300);
  };
}
