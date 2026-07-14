const gridApi = agGrid.createGrid(document.getElementById('diffGrid'), {
    columnDefs: [
        { field: 'id', headerName: 'ID', width: 70, filter: false },
        { field: 'tableName', headerName: 'Table', width: 150, colId: 'fTable' },
        { field: 'pkComposite', headerName: 'Primary Key', flex: 2, colId: 'fPk' },
        {
            headerName: 'Inspect',
            width: 120,
            sortable: false,
            filter: false,
            cellRenderer: p => `<button class="btn btn-sm btn-view" onclick="openDiffModal('${String(p.data?.pkComposite || '').replace(/'/g, "\\'")}', '${String(p.data?.tableName || '').replace(/'/g, "\\'")}')">View</button>`
        },
        {
            field: 'diffType',
            headerName: 'Diff Type',
            width: 190,
            colId: 'fDiffType',
            cellRenderer: p => {
                const cls = {
                    HASH_MISMATCH: 'badge-amber',
                    MISSING_IN_DEST: 'badge-red',
                    MISSING_IN_SOURCE: 'badge-blue'
                }[p.value] || 'badge-slate';
                return `<span class="diff-badge ${cls}">${p.value}</span>`;
            }
        },
        { field: 'sourceHash', headerName: 'Source Hash', flex: 1, colId: 'fSourceHash', cellRenderer: hashCellRenderer },
        { field: 'destHash', headerName: 'Dest Hash', flex: 1, colId: 'fDestHash', cellRenderer: hashCellRenderer },
        {
            field: 'changedColumns',
            headerName: 'Changed Columns',
            flex: 1.3,
            colId: 'fChangedColumns',
            cellRenderer: p => p.value ? `<span class="small" style="color:#67e8f9; font-family: monospace;">${p.value}</span>` : ''
        },
        { field: 'createdAt', headerName: 'Detected At', width: 180, filter: false }
    ],
    rowData: [],
    // Per-column search: server-side (we send the typed text to /api/results and
    // re-query SQLite), so we disable ag-grid's own client-side filtering logic
    // and just use its floating-filter text boxes as inputs.
    defaultColDef: {
        sortable: true,
        resizable: true,
        filter: 'agTextColumnFilter',
        floatingFilter: true,
        suppressHeaderFilterButton: true
    },
    animateRows: true,
    onFilterChanged: onColumnFilterChanged
});

/** Column-filter debounce timer + last-sent snapshot to avoid redundant requests. */
let columnFilterTimer = null;

/**
 * Collect the per-column search terms from ag-grid's floating filters and map
 * them to the backend query params (fPk, fDiffType, ...). Debounced so typing
 * doesn't fire a DB query on every keystroke.
 */
function onColumnFilterChanged() {
    clearTimeout(columnFilterTimer);
    columnFilterTimer = setTimeout(() => {
        currentPage = 1; // filtering changes the result set → back to page 1
        loadResults();
    }, 350);
}

/**
 * AG-Grid cell renderer for hash columns: shows a truncated form (72833…615c)
 * with the full hash in a tooltip and a copy-to-clipboard icon on row hover.
 */
function hashCellRenderer(p) {
    const full = String(p.value ?? '');
    if (!full) return '';
    const short = full.length > 14 ? `${full.slice(0, 6)}…${full.slice(-4)}` : full;
    const safe = full.replace(/'/g, "\\'");
    return `<span class="hash-cell" title="${escapeHtml(full)}">`
        + `<span>${escapeHtml(short)}</span>`
        + `<span class="hash-copy" title="Copy full hash" onclick="copyHash(event, '${safe}')">⧉</span>`
        + `</span>`;
}

/** Copy a full hash value to the clipboard from the hover icon. */
function copyHash(evt, value) {
    if (evt) evt.stopPropagation();
    const done = () => toast('Hash copied to clipboard');
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(value).then(done).catch(() => done());
    } else {
        const ta = document.createElement('textarea');
        ta.value = value;
        document.body.appendChild(ta);
        ta.select();
        try { document.execCommand('copy'); } catch (_) {}
        document.body.removeChild(ta);
        done();
    }
}

/** Smooth-scroll the user to the Difference Log (from the Differences KPI card). */
function jumpToDiffLog() {
    const el = document.getElementById('diffGrid') || document.querySelector('.diff-log-card');
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

/**
 * Highlight the active step in the top wizard bar.
 * step: 1 (environments), 2 (tables chosen), 3 (run/results visible).
 */
function updateWizard(step) {
    for (let i = 1; i <= 3; i++) {
        const wrap = document.getElementById('wizStep' + i);
        if (!wrap) continue;
        const dot = wrap.querySelector('.step-dot');
        wrap.classList.toggle('is-active', i === step);
        if (dot) {
            dot.classList.remove('active', 'done', 'pending');
            if (i < step) dot.classList.add('done');
            else if (i === step) dot.classList.add('active');
            else dot.classList.add('pending');
            dot.textContent = i < step ? '✓' : String(i);
        }
    }
}

/** Read the current per-column filter terms as backend query params. */
function currentColumnFilters() {    const model = gridApi.getFilterModel() || {};
    const params = {};
    ['fTable', 'fPk', 'fDiffType', 'fSourceHash', 'fDestHash', 'fChangedColumns'].forEach(colId => {
        const val = model[colId]?.filter;
        if (val != null && String(val).trim() !== '') params[colId] = String(val).trim();
    });
    return params;
}

let schemaData = null;
let sortableAvail = null;
let sortablePk = null;
let sortableHash = null;
let currentPage = 1;
let pollInterval = null;
const PAGE_SIZE = 100;
const POLL_INTERVAL_MS = 2000;
let matchedTables = [];
let allMatchedTables = [];
let selectedMatchedTableSet = new Set();
let configuredTableName = '';
let tableOptionsLoaded = false;
let sortableSchemaAvail = null;
let sortableSchemaSelected = null;
let oracleDbRetryTimer = null;
const ORACLE_DB_RETRY_MS = 3000;
let lastSchemaDragAt = 0;
let lastPanelToggle = { table: '', at: 0 };
let diffChart = null;
const SAVED_CFG_KEY = 'reconcilerSavedTableConfigs';
const DRAFT_CFG_KEY = 'reconcilerDraftTableConfigs';
const TABLE_ROW_COUNTS_KEY = 'reconcilerTableRowCounts';
let savedTableConfigs = loadConfigMap(SAVED_CFG_KEY);
let draftTableConfigs = loadConfigMap(DRAFT_CFG_KEY);
/** localStorage cache: tableName(upper) -> { src, dest } */
let cachedTableRowCounts = loadConfigMap(TABLE_ROW_COUNTS_KEY);
const dirtyTableSet = new Set();

function loadConfigMap(key) {
    try {
        const raw = localStorage.getItem(key);
        const obj = raw ? JSON.parse(raw) : {};
        return obj && typeof obj === 'object' ? obj : {};
    } catch (_) {
        return {};
    }
}

function saveConfigMap(key, obj) {
    try {
        localStorage.setItem(key, JSON.stringify(obj || {}));
    } catch (_) {
        // Ignore quota/storage errors in browser storage.
    }
}


function configStatusForTable(table) {
    if (!table) return '';
    if (dirtyTableSet.has(table)) return 'MODIFIED';
    if (savedTableConfigs[table]) return 'SAVED';
    return '';
}

function markCurrentTableModified() {
    if (!configuredTableName) return;
    dirtyTableSet.add(configuredTableName);
    const cfg = buildJobConfig();
    if (cfg) {
        draftTableConfigs[configuredTableName] = cfg;
        saveConfigMap(DRAFT_CFG_KEY, draftTableConfigs);
    }
    refreshSaveButtonState();
    renderMatchedTables();
}

function refreshSaveButtonState() {
    const btn = document.getElementById('saveTableCfgBtn');
    if (!btn) return;
    btn.disabled = !configuredTableName;
}

/**
 * Simple, non-misleading diff breakdown chart.
 * Renders ONE snapshot of the final counts for the current scope (all tables,
 * or the selected table). It is only drawn once a job has finished (COMPLETED /
 * FAILED / IDLE-with-data) — while RUNNING we show a simple progress message
 * instead of a partially-updating, confusing graph.
 */
function initDiffChart() {
    const el = document.getElementById('liveDiffChart');
    if (!el || typeof Chart === 'undefined' || diffChart) return;

    diffChart = new Chart(el, {
        type: 'bar',
        data: {
            // "Total Rows Scanned" removed — it dwarfed every other bar and is
            // already surfaced as its own KPI. Row counts use calm blue/grey;
            // warning colours are reserved strictly for the three diff types.
            labels: ['Oracle Rows', 'Snowflake Rows', 'Hash Mismatch', 'Missing in Dest', 'Missing in Src'],
            datasets: [{
                data: [0, 0, 0, 0, 0],
                backgroundColor: ['#64748b', '#94a3b8', '#f59e0b', '#ef4444', '#38bdf8'],
                borderRadius: 6,
                barThickness: 22
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 300 },
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: (ctx) => ` ${Number(ctx.parsed.x || 0).toLocaleString()}`
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: { color: '#94a3b8' },
                    grid: { color: 'rgba(148,163,184,0.08)' }
                },
                y: {
                    ticks: { color: '#cbd5e1' },
                    grid: { display: false }
                }
            }
        }
    });
}

/** Update the chart with a single final snapshot for the given scope. */
function renderDiffChart(srcRows, destRows, hashMismatch, missingDest, missingSrc) {
    initDiffChart();
    if (!diffChart) return;
    diffChart.data.datasets[0].data = [
        Number(srcRows || 0),
        Number(destRows || 0),
        Number(hashMismatch || 0),
        Number(missingDest || 0),
        Number(missingSrc || 0)
    ];
    diffChart.update();
}

/** Toggle between "in progress" placeholder and the final chart canvas. */
function setChartRunningState(isRunning) {
    const canvas = document.getElementById('liveDiffChart');
    const msg = document.getElementById('chartRunningMsg');
    if (canvas) canvas.style.display = isRunning ? 'none' : '';
    if (msg) msg.style.display = isRunning ? '' : 'none';
}

/**
 * No-op — the navbar global progress bar was removed in favour of per-table
 * progress bars in the Job Detail panel, which are far more informative when
 * a batch has multiple tables at different stages.
 */
function updateProgressBar(_status, _percent) { /* removed */ }

function updateChartScopeLabel(activeTable) {
    const el = document.getElementById('chartScopeLabel');
    if (!el) return;
    el.textContent = `Scope: ${activeTable ? activeTable : 'ALL TABLES'}`;
}

function renderDiffSummaryBar(data) {
    const bar = document.getElementById('diffSummaryBar');
    if (!bar) return;
    // renderDiffSummary (called separately) handles the full bar; this is a no-op to avoid double-render.
}

/**
 * Persist any non-zero sourceRowCount/destRowCount from tableSummaries into
 * localStorage so they survive service restarts and IDLE state.
 * Also merges the cached values back into summaries entries that have 0 counts.
 */
function syncTableRowCountCache(tableSummaries) {
    let dirty = false;
    (tableSummaries || []).forEach(s => {
        const key = String(s.tableName || '').toUpperCase();
        if (!key) return;
        const src  = Number(s.sourceRowCount || 0);
        const dest = Number(s.destRowCount   || 0);
        if (src > 0 || dest > 0) {
            cachedTableRowCounts[key] = { src, dest };
            dirty = true;
        } else {
            // Restore from cache if backend returns 0 (IDLE / restart)
            const cached = cachedTableRowCounts[key];
            if (cached) {
                s.sourceRowCount = cached.src;
                s.destRowCount   = cached.dest;
            }
        }
    });
    if (dirty) saveConfigMap(TABLE_ROW_COUNTS_KEY, cachedTableRowCounts);
}


function applyStoredConfigForCurrentTable() {
    if (!configuredTableName || !schemaData?.source?.columns) return;
    const stored = draftTableConfigs[configuredTableName] || savedTableConfigs[configuredTableName];
    if (!stored) return;

    const pkSet = new Set((stored.primaryKeys || '').split('~').map(s => s.trim().toUpperCase()).filter(Boolean));
    const hashSet = new Set((stored.hashColumns || []).map(s => String(s).trim().toUpperCase()).filter(Boolean));

    clearZone('availZone');
    clearZone('pkZone');
    clearZone('hashZone');

    schemaData.source.columns.forEach(col => {
        const name = String(col.name || '').toUpperCase();
        if (pkSet.has(name)) appendPill('pkZone', col);
        else if (hashSet.has(name)) appendPill('hashZone', col);
        else appendPill('availZone', col);
    });

    const modSel = document.getElementById('modColSelect');
    if (modSel && stored.modifiedColumn) {
        const opt = [...modSel.options].find(o => o.value.toUpperCase() === String(stored.modifiedColumn).toUpperCase());
        if (opt) modSel.value = opt.value;
    }
    if (stored.bufferMinutes != null) {
        document.getElementById('bufferMin').value = String(stored.bufferMinutes);
        document.getElementById('bufferPreview').textContent = String(stored.bufferMinutes);
    }

    updatePkPreview();
    updateAvailCount();
    updateEmptyMsgs();
}

function saveCurrentTableConfig() {
    if (!configuredTableName) {
        toast('Select a table to configure first', true);
        return;
    }
    const cfg = buildJobConfig();
    if (!cfg) {
        toast('Cannot save: configure PK columns first', true);
        return;
    }
    savedTableConfigs[configuredTableName] = cfg;
    saveConfigMap(SAVED_CFG_KEY, savedTableConfigs);
    delete draftTableConfigs[configuredTableName];
    saveConfigMap(DRAFT_CFG_KEY, draftTableConfigs);
    dirtyTableSet.delete(configuredTableName);
    refreshSaveButtonState();
    renderMatchedTables();
    toast(`Saved config for ${configuredTableName}`);
}

function selectedSourceSchemas() {
    const zone = document.getElementById('schemaSelectedZone');
    if (!zone) return [];
    return [...zone.querySelectorAll('.schema-pill')]
        .map(p => p.dataset.name || '')
        .map(s => s.trim())
        .filter(Boolean);
}

function firstSourceSchema() {
    const schemas = selectedSourceSchemas();
    return schemas.length ? schemas[0] : '';
}

function moveAllSchemas(toSelected) {
    const from = document.getElementById(toSelected ? 'schemaAvailZone' : 'schemaSelectedZone');
    const to = document.getElementById(toSelected ? 'schemaSelectedZone' : 'schemaAvailZone');
    if (!from || !to) return;
    [...from.querySelectorAll('.schema-pill')].forEach(p => to.appendChild(p));
    updateSchemaEmptyMsgs();
}

function appendSchemaPill(zoneId, name) {
    const zone = document.getElementById(zoneId);
    if (!zone) return;
    const pill = document.createElement('div');
    pill.className = 'schema-pill';
    pill.dataset.name = name;
    pill.innerHTML = `<span class="grip">#</span><span class="font-monospace">${escapeHtml(name)}</span>`;
    pill.addEventListener('click', () => {
        // Ignore synthetic click events immediately after a drag operation.
        if (Date.now() - lastSchemaDragAt < 250) return;
        const fromSelected = zoneIdOfPill(pill) === 'schemaSelectedZone';
        const target = document.getElementById(fromSelected ? 'schemaAvailZone' : 'schemaSelectedZone');
        if (!target) return;
        target.appendChild(pill);
        updateSchemaEmptyMsgs();
    });
    zone.appendChild(pill);
}

function zoneIdOfPill(pill) {
    const parent = pill?.parentElement;
    return parent?.id || '';
}

function updateSchemaEmptyMsgs() {
    const avail = document.getElementById('schemaAvailZone');
    const sel = document.getElementById('schemaSelectedZone');
    const availMsg = document.getElementById('schemaAvailEmptyMsg');
    const selMsg = document.getElementById('schemaSelectedEmptyMsg');
    if (avail && availMsg) availMsg.style.display = avail.querySelectorAll('.schema-pill').length ? 'none' : '';
    if (sel && selMsg) selMsg.style.display = sel.querySelectorAll('.schema-pill').length ? 'none' : '';
}

function initSchemaSortables() {
    const avail = document.getElementById('schemaAvailZone');
    const selected = document.getElementById('schemaSelectedZone');
    if (!avail || !selected) return;
    if (!sortableSchemaAvail) {
        sortableSchemaAvail = Sortable.create(avail, {
            group: 'schemas',
            animation: 150,
            onEnd: () => {
                lastSchemaDragAt = Date.now();
                updateSchemaEmptyMsgs();
            }
        });
    }
    if (!sortableSchemaSelected) {
        sortableSchemaSelected = Sortable.create(selected, {
            group: 'schemas',
            animation: 150,
            onEnd: () => {
                lastSchemaDragAt = Date.now();
                updateSchemaEmptyMsgs();
            }
        });
    }
}

async function loadOracleDatabases() {
    const select = document.getElementById('srcDbSelect');
    if (!select) {
        console.error('[loadOracleDatabases] #srcDbSelect element not found in DOM');
        return;
    }
    console.info('[loadOracleDatabases] fetching /api/oracle-databases ...');
    try {
        const res = await fetch('/api/oracle-databases');
        let data;
        try {
            data = await res.json();
        } catch (parseErr) {
            console.error('[loadOracleDatabases] response was not valid JSON', parseErr);
            select.innerHTML = '<option value="">unavailable - retrying...</option>';
            scheduleOracleDbRetry();
            return;
        }
        if (!res.ok) {
            console.error('[loadOracleDatabases] backend returned error', res.status, data);
            select.innerHTML = '<option value="">unavailable - retrying...</option>';
            scheduleOracleDbRetry();
            return;
        }
        const rows = data.rows || [];
        console.info('[loadOracleDatabases] got rows', rows);
        if (!rows.length) {
            select.innerHTML = '<option value="">discovering databases - retrying...</option>';
            scheduleOracleDbRetry();
            return;
        }
        const opts = rows.map(r => {
            const label = `${r.dbName || ''}${r.containerName ? ' / ' + r.containerName : ''}`;
            return `<option value="${escapeHtml(r.containerName || r.dbName || '')}">${escapeHtml(label)}</option>`;
        }).join('');
        select.innerHTML = `<option value="" selected>Select Oracle database...</option>${opts}`;
        if (oracleDbRetryTimer) {
            clearTimeout(oracleDbRetryTimer);
            oracleDbRetryTimer = null;
        }
    } catch (e) {
        console.error('[loadOracleDatabases] fetch threw', e);
        select.innerHTML = '<option value="">unavailable - retrying...</option>';
        scheduleOracleDbRetry();
    }
}

function scheduleOracleDbRetry() {
    if (oracleDbRetryTimer) return;
    oracleDbRetryTimer = setTimeout(async () => {
        oracleDbRetryTimer = null;
        await loadOracleDatabases();
    }, ORACLE_DB_RETRY_MS);
}

async function loadOracleSchemas() {
    const avail = document.getElementById('schemaAvailZone');
    const selected = document.getElementById('schemaSelectedZone');
    if (!avail || !selected) return;
    try {
        const prev = new Set(selectedSourceSchemas().map(s => s.toUpperCase()));
        const db = document.getElementById('srcDbSelect')?.value || '';
        if (!db) {
            avail.querySelectorAll('.schema-pill').forEach(p => p.remove());
            selected.querySelectorAll('.schema-pill').forEach(p => p.remove());
            updateSchemaEmptyMsgs();
            toast('Select Oracle database first', true);
            return;
        }
        const res = await fetch(`/api/schemas?db=oracle&mappedOnly=true&database=${encodeURIComponent(db)}`);
        const data = await res.json();
        if (!res.ok) {
            toast(data.error || 'Failed to load Oracle schemas', true);
            return;
        }
        const rows = (data.rows || []).map(r => String(r.name || r).trim()).filter(Boolean);
        avail.querySelectorAll('.schema-pill').forEach(p => p.remove());
        selected.querySelectorAll('.schema-pill').forEach(p => p.remove());

        rows.forEach(name => {
            const zoneId = prev.has(name.toUpperCase()) ? 'schemaSelectedZone' : 'schemaAvailZone';
            appendSchemaPill(zoneId, name);
        });
        initSchemaSortables();
        updateSchemaEmptyMsgs();
    } catch (e) {
        toast('Failed to load Oracle schemas: ' + e.message, true);
    }
}

function parseSchemaIds() {
    const raw = document.getElementById('schemaIdsInput')?.value || '';
    const ids = raw.split(',').map(s => s.trim()).filter(Boolean);
    return [...new Set(ids.map(s => s.toUpperCase()))];
}

function buildBatchTableConfigs(tables) {
    const merged = { ...savedTableConfigs, ...draftTableConfigs };
    const requested = tables && tables.length ? tables : Object.keys(merged);
    const out = {};
    requested.forEach(t => {
        const key = String(t || '').trim();
        if (!key) return;
        const cfg = merged[key] || merged[key.toUpperCase()] || merged[key.toLowerCase()];
        if (!cfg) return;
        out[key.toUpperCase()] = {
            primaryKeys: cfg.primaryKeys || '',
            hashColumns: Array.isArray(cfg.hashColumns) ? cfg.hashColumns : [],
            modifiedColumn: cfg.modifiedColumn || null,
            bufferMinutes: Number.isFinite(cfg.bufferMinutes) ? cfg.bufferMinutes : null
        };
    });
    return Object.keys(out).length ? out : null;
}

async function loadSchemaMappings() {
    const hint = document.getElementById('mappingsHint');
    if (!hint) return;
    try {
        const res = await fetch('/api/schema-mappings');
        const data = await res.json();
        if (!res.ok) {
            hint.textContent = 'Schema mappings unavailable';
            return;
        }
        const rows = (data.rows || []).filter(r => r.enabled && r.usable);
        if (!rows.length) {
            hint.textContent = 'No enabled schema mappings found';
            return;
        }
        const ids = rows.map(r => String(r.schemaId || '').toUpperCase()).filter(Boolean);
        hint.textContent = `Configured mappings: ${ids.join(', ')}`;

        const input = document.getElementById('schemaIdsInput');
        if (input && !input.value.trim()) {
            input.value = ids.join(',');
        }
    } catch (e) {
        hint.textContent = 'Schema mappings unavailable';
    }
}

const diffModal = new bootstrap.Modal(document.getElementById('diffModal'));

function toast(msg, err = false) {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.style.borderColor = err ? '#b91c1c' : '#475569';
    el.classList.add('show');
    setTimeout(() => el.classList.remove('show'), 3000);
}

function typeClass(t = '') {
    t = t.toUpperCase();
    if (/NUMBER|INT|FLOAT|DECIMAL|DOUBLE|NUMERIC/.test(t)) return 'type-num';
    if (/VARCHAR|CHAR|TEXT|STRING|CLOB/.test(t)) return 'type-str';
    if (/DATE|TIME|TIMESTAMP/.test(t)) return 'type-date';
    return 'type-other';
}

document.getElementById('bufferMin').addEventListener('input', () => {
    document.getElementById('bufferPreview').textContent = document.getElementById('bufferMin').value || '15';
    markCurrentTableModified();
});

document.getElementById('modColSelect')?.addEventListener('change', () => {
    markCurrentTableModified();
});

function setStepDots(active) {
    // Stepper removed intentionally: keep as compatibility no-op.
}

function setSectionVisible(id, visible) {
    if (id === 'step1' || id === 'step2' || id === 'step3') {
        return;
    }
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.toggle('hidden', !visible);
    el.style.display = visible ? '' : 'none';
}

function setMappingVisible(visible) {
    const el = document.getElementById('step2');
    if (!el) return;
    el.classList.toggle('hidden', !visible);
}

/**
 * Scan: finds tables that exist in BOTH the selected Oracle schema(s) and the
 * configured Snowflake schema, via GET /api/match-tables, and populates the
 * selectable list in the sidebar. This does NOT start a comparison job by
 * itself — use "Run Selected Tables" / "Run All Common Tables" afterwards.
 */
async function scanMatchedTables() {
    if (parseSchemaIds().length) {
        toast('Schema ID mode selected — scan is not needed. Use Run Selected/Run All below.', true);
        return;
    }
    const schemas = selectedSourceSchemas();
    if (!schemas.length) {
        toast('Select at least one Oracle schema', true);
        return;
    }
    const destSchema = document.getElementById('destSchemaInput').value.trim();
    const destDatabase = document.getElementById('destDbInput').value.trim();
    if (!destSchema) {
        toast('Enter destination Snowflake schema', true);
        return;
    }

    const btn = document.querySelector('button[onclick="scanMatchedTables()"]');
    if (btn) btn.disabled = true;

    try {
        const found = new Set();
        for (const schema of schemas) {
            const url = `/api/match-tables?sourceSchema=${encodeURIComponent(schema)}&destSchema=${encodeURIComponent(destSchema)}&destDatabase=${encodeURIComponent(destDatabase)}`;
            const res = await fetch(url);
            const data = await res.json();
            if (!res.ok) {
                toast(data.error || `Scan failed for schema ${schema}`, true);
                continue;
            }
            (data.rows || []).forEach(t => found.add(t));
        }

        allMatchedTables = [...found].sort();
        matchedTables = allMatchedTables;
        selectedMatchedTableSet = new Set();
        tableOptionsLoaded = false;
        configuredTableName = '';
        setMappingVisible(false);

        const searchInput = document.getElementById('tableSearchInput');
        const resultsPanel = document.getElementById('matchedTablesResults');
        if (searchInput) { searchInput.value = ''; searchInput.classList.remove('hidden'); }
        if (resultsPanel) resultsPanel.classList.toggle('hidden', !allMatchedTables.length);

        renderMatchedTables();
        setSectionVisible('step3', true);
        if (allMatchedTables.length) updateWizard(2);
        toast(allMatchedTables.length ? `${allMatchedTables.length} common table(s) found` : 'No common tables found');
    } catch (e) {
        toast('Scan failed: ' + e.message, true);
    } finally {
        if (btn) btn.disabled = false;
    }
}

/** Case-insensitive substring filter over the full matched-tables list. */
function filterTablesByQuery(tables, query) {
    const q = String(query || '').trim().toUpperCase();
    if (!q) return tables;
    return tables.filter(t => String(t).toUpperCase().includes(q));
}

/** Currently checked tables in the matched-tables list. */
function selectedMatchedTables() {
    return [...selectedMatchedTableSet];
}

/** Checkbox change handler for a single row in the matched-tables list. */
function onMatchedTableToggle(tableName, checked) {
    if (checked) selectedMatchedTableSet.add(tableName);
    else selectedMatchedTableSet.delete(tableName);
}

/** "all" / "none" links above the matched-tables list. */
function toggleAllTables(selectAll) {
    if (selectAll) matchedTables.forEach(t => selectedMatchedTableSet.add(t));
    else matchedTables.forEach(t => selectedMatchedTableSet.delete(t));
    renderMatchedTables();
}

/** Renders the scrollable matched-tables checkbox list with per-table config status + Configure button. */
function renderMatchedTables() {
    const panel = document.getElementById('matchedTablesPanel');
    const countEl = document.getElementById('matchedCount');
    if (countEl) countEl.textContent = `${matchedTables.length} of ${allMatchedTables.length} table${allMatchedTables.length === 1 ? '' : 's'}`;
    if (!panel) return;

    if (!matchedTables.length) {
        panel.innerHTML = '<div class="small text-secondary text-center py-2">No matching tables</div>';
        return;
    }

    panel.innerHTML = matchedTables.map(t => {
        const checked = selectedMatchedTableSet.has(t) ? 'checked' : '';
        const escaped = String(t).replace(/'/g, "\\'");
        const status = configStatusForTable(t);
        const statusBadge = status === 'SAVED'
            ? '<span class="diff-badge badge-green" style="font-size:0.58rem;">SAVED</span>'
            : status === 'MODIFIED'
                ? '<span class="diff-badge badge-amber" style="font-size:0.58rem;">MODIFIED</span>'
                : '';
        const isActive = configuredTableName === t;
        return `
        <div class="list-group-item d-flex align-items-center justify-content-between gap-2 ${isActive ? 'active' : ''}">
            <div class="d-flex align-items-center gap-2 overflow-hidden">
                <input type="checkbox" class="form-check-input mt-0" ${checked} onchange="onMatchedTableToggle('${escaped}', this.checked)" />
                <span class="font-monospace small text-truncate" title="${escapeHtml(t)}">${escapeHtml(t)}</span>
                ${statusBadge}
            </div>
            <button type="button" class="btn btn-sm btn-outline-info" onclick="toggleConfiguredTable('${escaped}')">${isActive ? 'Close' : 'Configure'}</button>
        </div>`;
    }).join('');
}

/** Opens/closes the Column Mapping section for a specific matched table. */
function toggleConfiguredTable(tableName) {
    if (configuredTableName === tableName) {
        configuredTableName = '';
        setMappingVisible(false);
        renderMatchedTables();
        refreshSaveButtonState();
        return;
    }
    prefillFromTable(tableName);
}

/** Prefills Source/Dest table inputs for the clicked table and loads its schema for mapping. */
async function prefillFromTable(tableName) {
    const schema = firstSourceSchema();
    const destSchema = document.getElementById('destSchemaInput').value.trim();
    const destDatabase = document.getElementById('destDbInput').value.trim();

    document.getElementById('srcTable').value = schema ? `${schema}.${tableName}` : tableName;
    document.getElementById('destTable').value = destDatabase
        ? `${destDatabase}.${destSchema}.${tableName}`
        : `${destSchema}.${tableName}`;

    configuredTableName = tableName;
    await loadSchema();
    applyStoredConfigForCurrentTable();
    setMappingVisible(true);
    renderMatchedTables();
    refreshSaveButtonState();
}

function runBatchSelected() {
    const tables = selectedMatchedTables();
    if (!tables.length) {
        toast('Select at least one table', true);
        return;
    }
    console.info('[runBatchSelected] selected tables', tables);
    runBatch(tables);
}

function runBatchAll() {
    if (parseSchemaIds().length) {
        console.info('[runBatchAll] running all tables for schemaIds');
        runBatch([]);
        return;
    }
    if (!allMatchedTables.length) {
        toast('Scan tables first', true);
        return;
    }
    console.info('[runBatchAll] running all matched tables');
    runBatch([]);
}

/** Shared batch-start call used by both Run Selected and Run All. */
async function runBatch(tables) {
    const schemas = selectedSourceSchemas();
    const destSchema = document.getElementById('destSchemaInput').value.trim();
    const destDatabase = document.getElementById('destDbInput').value.trim();
    const payload = {
        schemaIds: parseSchemaIds().length ? parseSchemaIds() : null,
        sourceSchemas: schemas.length ? schemas : null,
        sourceSchema: schemas.length ? schemas[0] : null,
        destSchema,
        destDatabase,
        tableConfigs: buildBatchTableConfigs(tables),
        tables: tables || [],
        modifiedColumn: document.getElementById('modColSelect').value || null,
        bufferMinutes: parseInt(document.getElementById('bufferMin').value, 10) || 15,
        clearBefore: document.getElementById('clearBeforeCheck')?.checked ?? false
    };
    cachedTableRowCounts = {};
    saveConfigMap(TABLE_ROW_COUNTS_KEY, cachedTableRowCounts);
    console.info('[runBatch] payload', payload);
    try {
        const res = await fetch('/api/start-batch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (!res.ok) {
            toast(data.error || 'Batch failed to start', true);
            return;
        }
        const info = document.getElementById('batchInfo');
        if (info) {
            info.classList.add('hidden');
            info.textContent = '';
        }
        const tableFilterSelect = document.getElementById('tableFilterSelect');
        if (tableFilterSelect) tableFilterSelect.value = '';
        const searchBox = document.getElementById('searchBox');
        if (searchBox) searchBox.value = '';
        tableOptionsLoaded = false;
        currentPage = 1;
        setSectionVisible('step3', true);
        updateWizard(3);
        startPolling();
        toast(data.message || 'Batch started');
    } catch (e) {
        toast('Batch failed: ' + e.message, true);
    }
}

async function loadSchema() {
    const src = document.getElementById('srcTable').value.trim();
    const dst = document.getElementById('destTable').value.trim();
    if (!src || !dst) {
        toast('Enter both table names', true);
        return;
    }
    const btn = document.getElementById('loadSchemaBtn');
    const sp = document.getElementById('loadSpinner');
    btn.disabled = true;
    sp.classList.remove('hidden');
    try {
        console.info('[loadSchema] request', { src, dst });
        const res = await fetch(`/api/schema?source=${encodeURIComponent(src)}&dest=${encodeURIComponent(dst)}`);
        const text = await res.text();
        const data = text ? JSON.parse(text) : {};
        if (!res.ok) {
            toast('Schema load failed: ' + (data.error || res.status), true);
            return;
        }
        schemaData = data;
        buildColumnUI(data);
        setMappingVisible(Boolean(configuredTableName));
        toast('Schema loaded');
    } catch (e) {
        toast('Schema load error: ' + e.message, true);
    } finally {
        btn.disabled = false;
        sp.classList.add('hidden');
    }
}

function resetToStep1() {
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }
    currentPage = 1;
    document.getElementById('searchBox').value = '';
    loadResults();
}

function buildColumnUI(data) {
    const srcCols = data.source.columns;
    const destCols = data.dest.columns;
    const srcPKs = data.source.primaryKeys;

    const sn = new Set(srcCols.map(c => c.name.toUpperCase()));
    const dn = new Set(destCols.map(c => c.name.toUpperCase()));
    const mismatch = [...sn].some(n => !dn.has(n)) || [...dn].some(n => !sn.has(n));
    document.getElementById('colMismatchWarn').classList.toggle('hidden', !mismatch);

    const modSel = document.getElementById('modColSelect');
    modSel.innerHTML = '<option value="">- None -</option>';
    srcCols.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.name;
        opt.textContent = `${c.name} (${c.type})`;
        if (/DATE|TIME|TIMESTAMP/i.test(c.type)) {
            modSel.insertBefore(opt, modSel.options[1]);
        } else {
            modSel.appendChild(opt);
        }
    });

    const pkSet = new Set(srcPKs.map(k => k.toUpperCase()));
    const rest = srcCols.filter(c => !pkSet.has(c.name.toUpperCase()));
    clearZone('availZone');
    clearZone('pkZone');
    clearZone('hashZone');

    rest.forEach(c => appendPill('hashZone', c));
    srcPKs.forEach(name => {
        const col = srcCols.find(c => c.name.toUpperCase() === name.toUpperCase());
        if (col) appendPill('pkZone', col);
    });

    updatePkPreview();
    updateAvailCount();
    updateEmptyMsgs();

    if (sortableAvail) sortableAvail.destroy();
    if (sortablePk) sortablePk.destroy();
    if (sortableHash) sortableHash.destroy();

    const onDrop = () => {
        updatePkPreview();
        updateAvailCount();
        updateEmptyMsgs();
        markCurrentTableModified();
    };

    const shared = { group: 'columns', animation: 150, ghostClass: 'opacity-50' };
    sortableAvail = Sortable.create(document.getElementById('availZone'), { ...shared, onEnd: onDrop });
    sortablePk = Sortable.create(document.getElementById('pkZone'), { ...shared, onEnd: onDrop });
    sortableHash = Sortable.create(document.getElementById('hashZone'), { ...shared, onEnd: onDrop });
}

function clearZone(id) {
    document.getElementById(id).querySelectorAll('.col-pill').forEach(p => p.remove());
}

function appendPill(zoneId, col) {
    const zone = document.getElementById(zoneId);
    const pill = document.createElement('div');
    pill.className = 'col-pill';
    pill.dataset.name = col.name;
    pill.dataset.type = col.type || '';
    pill.innerHTML = `
        <span class="grip">#</span>
        <span class="font-monospace" style="font-size: 0.75rem;">${col.name}</span>
        <span class="diff-badge ${typeClass(col.type)}" style="font-size: 0.58rem;">${col.type || '?'}</span>
        <span class="rm" title="Remove from compare set" onclick="removePill(this)">x</span>`;
    const empty = zone.querySelector('[id$="EmptyMsg"]');
    if (empty) empty.style.display = 'none';
    zone.appendChild(pill);
}

function removePill(rm) {
    const pill = rm.closest('.col-pill');
    const name = pill.dataset.name;
    const type = pill.dataset.type;
    pill.remove();
    appendPill('availZone', { name, type });
    updatePkPreview();
    updateAvailCount();
    updateEmptyMsgs();
    markCurrentTableModified();
}

function updatePkPreview() {
    const pills = [...document.getElementById('pkZone').querySelectorAll('.col-pill')];
    pills.forEach((p, i) => {
        let badge = p.querySelector('.pk-order');
        if (!badge) {
            badge = document.createElement('span');
            badge.className = 'pk-order';
            p.insertBefore(badge, p.firstChild);
        }
        badge.textContent = i + 1;
    });
    document.getElementById('pkPreview').textContent = pills.map(p => p.dataset.name).join(' ~ ') || '-';
}

function updateAvailCount() {
    const n = document.getElementById('availZone').querySelectorAll('.col-pill').length;
    document.getElementById('availCount').textContent = `${n} cols`;
}

function updateEmptyMsgs() {
    ['avail', 'pk', 'hash'].forEach(z => {
        const zone = document.getElementById(z + 'Zone');
        const msg = document.getElementById(z + 'EmptyMsg');
        if (msg) msg.style.display = zone.querySelectorAll('.col-pill').length ? 'none' : '';
    });
}

function selectAllHash() {
    document.getElementById('availZone').querySelectorAll('.col-pill').forEach(p => {
        const name = p.dataset.name;
        const type = p.dataset.type;
        p.remove();
        appendPill('hashZone', { name, type });
    });
    updateAvailCount();
    updateEmptyMsgs();
    markCurrentTableModified();
}

function buildJobConfig() {
    const pkPills = [...document.getElementById('pkZone').querySelectorAll('.col-pill')];
    const hashPills = [...document.getElementById('hashZone').querySelectorAll('.col-pill')];
    if (!pkPills.length) return null;
    return {
        sourceTable: document.getElementById('srcTable').value.trim(),
        destTable: document.getElementById('destTable').value.trim(),
        primaryKeys: pkPills.map(p => p.dataset.name).join('~'),
        hashColumns: hashPills.map(p => p.dataset.name),
        modifiedColumn: document.getElementById('modColSelect').value || null,
        bufferMinutes: parseInt(document.getElementById('bufferMin').value, 10) || 15
    };
}

async function startJob() {
    const cfg = buildJobConfig();
    if (!cfg) {
        const vm = document.getElementById('validationMsg');
        vm.textContent = 'Add at least one Primary Key column before running.';
        vm.classList.remove('hidden');
        setTimeout(() => vm.classList.add('hidden'), 4000);
        return;
    }

    if (!cfg.hashColumns.length && schemaData) {
        cfg.hashColumns = schemaData.source.columns.map(c => c.name);
    }

    const sp = document.getElementById('runSpinner');
    sp.classList.remove('hidden');
    cachedTableRowCounts = {};
    saveConfigMap(TABLE_ROW_COUNTS_KEY, cachedTableRowCounts);
    const res = await fetch('/api/start-job', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg)
    });
    sp.classList.add('hidden');

    if (!res.ok) {
        const e = await res.json();
        toast(e.error || 'Failed', true);
        return;
    }

    const info = document.getElementById('batchInfo');
    if (info) {
        info.classList.add('hidden');
        info.textContent = '';
    }

    startPolling();
    toast('Reconciliation job started');
}

function startPolling() {
    if (pollInterval) clearInterval(pollInterval);
    pollInterval = setInterval(loadResults, POLL_INTERVAL_MS);
    loadResults();
}

function ensurePolling() {
    if (pollInterval) return;
    pollInterval = setInterval(loadResults, POLL_INTERVAL_MS);
}

async function loadResults() {
    const search = document.getElementById('searchBox').value;
    const table = document.getElementById('tableFilterSelect')?.value || '';
    const allHistory = Boolean(document.getElementById('allHistoryToggle')?.checked);
    updateChartScopeLabel(table);
    const colFilters = currentColumnFilters();
    const colParams = Object.entries(colFilters)
        .map(([k, v]) => `&${k}=${encodeURIComponent(v)}`)
        .join('');
    console.info('[loadResults] refresh', { currentPage, search, table, allHistory, colFilters });
    const res = await fetch(`/api/results?page=${currentPage}&size=${PAGE_SIZE}&search=${encodeURIComponent(search)}&table=${encodeURIComponent(table)}&allHistory=${allHistory}${colParams}`);
    const data = await res.json();

    const scopeLabel = document.getElementById('resultsScopeLabel');
    if (scopeLabel) {
        scopeLabel.textContent = data.allHistory
            ? 'Scope: all history'
            : `Scope: latest job${data.scopeJobTs ? ' (' + data.scopeJobTs.slice(0, 19).replace('T', ' ') + ')' : ''}`;
    }

    gridApi.setGridOption('rowData', data.rows);

    // Merge cached per-table row counts so IDLE/restart doesn't show 0s.
    syncTableRowCountCache(data.tableSummaries);
    let resolvedSrcCount = Number(data.sourceCount || 0);
    let resolvedDestCount = Number(data.destCount  || 0);
    {
        // Scope-specific cache key so "all history" and "latest job" totals never
        // leak into each other after a restart/reload while the last scope was different.
        const LS_KEY_COUNTS = allHistory ? 'reconciler_last_row_counts_all' : 'reconciler_last_row_counts_latest';
        if (resolvedSrcCount === 0 && resolvedDestCount === 0 && !table) {
            try {
                const c = JSON.parse(localStorage.getItem(LS_KEY_COUNTS) || 'null');
                if (c) { resolvedSrcCount = Number(c.src || 0); resolvedDestCount = Number(c.dest || 0); }
            } catch (_) {}
        }
        if (!table) {
            // Persist and show global row counts when no table filter is active.
            if (resolvedSrcCount > 0 || resolvedDestCount > 0) {
                try { localStorage.setItem(LS_KEY_COUNTS, JSON.stringify({ src: resolvedSrcCount, dest: resolvedDestCount })); } catch (_) {}
            }
            document.getElementById('statSrc').textContent  = resolvedSrcCount.toLocaleString();
            document.getElementById('statDest').textContent = resolvedDestCount.toLocaleString();
        } else {
            // Show per-table scanned row counts from tableSummaries (or cache) when a table filter is active.
            const tblSummary = (data.tableSummaries || []).find(
                s => String(s.tableName || '').toUpperCase() === table.toUpperCase()
            );
            const cached = cachedTableRowCounts[table.toUpperCase()];
            const tblSrc  = (tblSummary ? Number(tblSummary.sourceRowCount || 0) : 0) || (cached ? Number(cached.src  || 0) : 0);
            const tblDest = (tblSummary ? Number(tblSummary.destRowCount   || 0) : 0) || (cached ? Number(cached.dest || 0) : 0);
            document.getElementById('statSrc').textContent  = tblSrc  > 0 ? tblSrc.toLocaleString()  : '—';
            document.getElementById('statDest').textContent = tblDest > 0 ? tblDest.toLocaleString() : '—';
        }
    }
    document.getElementById('statDiff').textContent = Number(data.totalRows || 0).toLocaleString();
    document.getElementById('statStatus').textContent = data.status;
    document.getElementById('pageInfo').textContent = `Page ${data.page} - ${Number(data.totalRows).toLocaleString()} total`;

    populateTableFilter(data.tableOptions || []);
    renderTablePanels(data.tableSummaries || [], table, search);
    renderDiffSummaryBar(data);
    renderDiffSummary(data, table || null, resolvedSrcCount, resolvedDestCount);


    if (data.error) {
        const info = document.getElementById('batchInfo');
        if (info) {
            info.textContent = data.error;
            info.classList.remove('hidden');
        }
    }

    const badge = document.getElementById('statusBadge');
    badge.textContent = data.status;
    badge.className = 'diff-badge ' + ({
        IDLE: 'badge-blue',
        RUNNING: 'badge-amber',
        COMPLETED: 'badge-green',
        FAILED: 'badge-red'
    }[data.status] || 'badge-blue');

    updateProgressBar(data.status, data.progressPercent);

    if (data.status === 'RUNNING') {
        // If the page loaded mid-run (or was reopened), polling may not exist yet.
        // Start it lazily so progress keeps moving without manual refresh.
        ensurePolling();

        // Keep Job History synced during auto-resume / active runs so newly-created
        // running jobs appear even if the page loaded before the backend inserted them.
        loadJobs();

        if (currentJobDetail) {
            // In-place update of the open Job Detail tables panel.
            refreshJobDetailTables(currentJobDetail);
        }
    }

    if (['COMPLETED', 'FAILED', 'IDLE'].includes(data.status) && pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
        // Job finished — refresh Job History so the new row/status shows up automatically.
        loadJobs();
        if (currentJobDetail) {
            openJobDetail(currentJobDetail);
        }
    }
}

function renderDiffSummary(data, activeTable, resolvedSrcCount, resolvedDestCount) {
    const bar = document.getElementById('diffSummaryBar');
    if (!bar) return;

    const summaries = data.tableSummaries || [];
    let srcRows, destRows, totalCompared;
    let hashMismatch = 0;
    let missingSrc = 0;
    let missingDest = 0;
    let perTable = false;

    if (activeTable) {
        const tbl = summaries.find(s => String(s.tableName || '').toUpperCase() === activeTable.toUpperCase());
        const cached = cachedTableRowCounts[activeTable.toUpperCase()];
        if (tbl) {
            hashMismatch = Number(tbl.hashMismatch    || 0);
            missingSrc   = Number(tbl.missingInSource || 0);
            missingDest  = Number(tbl.missingInDest   || 0);
            srcRows  = Number(tbl.sourceRowCount || 0) || (cached ? Number(cached.src  || 0) : 0);
            destRows = Number(tbl.destRowCount   || 0) || (cached ? Number(cached.dest || 0) : 0);
        } else {
            srcRows  = cached ? Number(cached.src  || 0) : 0;
            destRows = cached ? Number(cached.dest || 0) : 0;
        }
        totalCompared = srcRows + destRows;
        perTable = true;
    } else {
        summaries.forEach(s => {
            hashMismatch += Number(s.hashMismatch    || 0);
            missingSrc   += Number(s.missingInSource || 0);
            missingDest  += Number(s.missingInDest   || 0);
        });
        srcRows  = Number(resolvedSrcCount || 0);
        destRows = Number(resolvedDestCount || 0);
        totalCompared = srcRows + destRows;
    }

    const totalDiffs = hashMismatch + missingSrc + missingDest;

    bar.innerHTML = `
        <div class="diff-summary-item">
            <div class="diff-summary-icon icon-mismatch">&#9888;</div>
            <div>
                <div class="diff-summary-val" style="color:#f59e0b">${totalDiffs.toLocaleString()}</div>
                <div class="diff-summary-label">Total Diffs</div>
            </div>
        </div>
        <div class="diff-summary-item">
            <div class="diff-summary-icon icon-mismatch" style="font-size:0.9rem;">&#8800;</div>
            <div>
                <div class="diff-summary-val" style="color:#f59e0b">${hashMismatch.toLocaleString()}</div>
                <div class="diff-summary-label">Hash Mismatch</div>
            </div>
        </div>
        <div class="diff-summary-item">
            <div class="diff-summary-icon icon-dest">&#8613;</div>
            <div>
                <div class="diff-summary-val" style="color:#fca5a5">${missingDest.toLocaleString()}</div>
                <div class="diff-summary-label">Missing in Dest</div>
            </div>
        </div>
        <div class="diff-summary-item">
            <div class="diff-summary-icon icon-src">&#8615;</div>
            <div>
                <div class="diff-summary-val" style="color:#38bdf8">${missingSrc.toLocaleString()}</div>
                <div class="diff-summary-label">Missing in Src</div>
            </div>
        </div>`;

    // Update the diff log row count badge
    const countEl = document.getElementById('diffLogCount');
    if (countEl) {
        const total = Number(data.totalRows || 0);
        countEl.textContent = total ? `${total.toLocaleString()} rows` : '';
    }

    // Only draw the final chart once the job has actually finished (or was
    // already IDLE with existing data). While RUNNING, show a simple
    // "in progress" placeholder instead of a partially-updating, misleading graph.
    const isRunning = data.status === 'RUNNING';
    setChartRunningState(isRunning);
    if (!isRunning) {
        renderDiffChart(srcRows, destRows, hashMismatch, missingDest, missingSrc);
    }
}

function populateTableFilter(options) {
    const select = document.getElementById('tableFilterSelect');
    if (!select) return;
    const current = select.value;
    const normalized = options.map(o => String(o));
    if (!tableOptionsLoaded || normalized.length + 1 !== select.options.length || !normalized.includes(current)) {
        select.innerHTML = '<option value="">All tables</option>'
            + normalized.map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join('');
        if (current && normalized.includes(current)) {
            select.value = current;
        }
        tableOptionsLoaded = true;
    }
}

function safePanelId(tableName) {
    return 'tbl_' + String(tableName || '').replace(/[^a-zA-Z0-9_-]/g, '_');
}

/** Sort state for the schema summary data table. */
let tablePanelSort = { key: 'hashMismatch', dir: 'desc' };

function setTablePanelSort(key) {
    if (tablePanelSort.key === key) {
        tablePanelSort.dir = tablePanelSort.dir === 'desc' ? 'asc' : 'desc';
    } else {
        tablePanelSort = { key, dir: 'desc' };
    }
    // Re-render using the last known dataset without a server round-trip.
    renderTablePanels(lastTableSummaries, lastActiveTable, lastActiveSearch);
}

let lastTableSummaries = [];
let lastActiveTable = null;
let lastActiveSearch = '';

/**
 * Renders the per-table breakdown as a clean, sortable data table.
 * Clicking a header sorts (default "Most Mismatches"); clicking a row focuses
 * that table in the Difference Log. Warning colours are reserved for the diff
 * counts only.
 */
function renderTablePanels(summaries, activeTable, activeSearch) {
    lastTableSummaries = summaries || [];
    lastActiveTable = activeTable;
    lastActiveSearch = activeSearch || '';

    const host = document.getElementById('tablePanels');
    if (!host) return;
    if (!summaries.length) {
        host.innerHTML = '<div class="small text-secondary">No table groups yet. Run reconciliation to populate table panels.</div>';
        return;
    }

    const num = v => Number(v || 0);
    const accessors = {
        tableName:      s => String(s.tableName || '').toUpperCase(),
        total:          s => num(s.total),
        hashMismatch:   s => num(s.hashMismatch),
        missingInDest:  s => num(s.missingInDest),
        missingInSource:s => num(s.missingInSource)
    };
    const acc = accessors[tablePanelSort.key] || accessors.hashMismatch;
    const dir = tablePanelSort.dir === 'asc' ? 1 : -1;
    const rows = [...summaries].sort((a, b) => {
        const av = acc(a), bv = acc(b);
        if (av < bv) return -1 * dir;
        if (av > bv) return 1 * dir;
        return 0;
    });

    const caret = key => tablePanelSort.key === key
        ? `<span class="sort-caret">${tablePanelSort.dir === 'desc' ? '▼' : '▲'}</span>`
        : '';
    const header = (key, label, cls = '') =>
        `<th class="sortable ${cls}" onclick="setTablePanelSort('${key}')">${label}${caret(key)}</th>`;

    const numCls = (val, kind) => num(val) === 0 ? 'num zero' : `num ${kind}`;

    host.innerHTML = `
        <table class="schema-table">
            <thead><tr>
                ${header('tableName', 'Table')}
                ${header('total', 'Total', 'num')}
                ${header('hashMismatch', 'Mismatch', 'num')}
                ${header('missingInDest', 'Missing Dest', 'num')}
                ${header('missingInSource', 'Missing Src', 'num')}
                <th class="num"></th>
            </tr></thead>
            <tbody>
                ${rows.map(s => {
                    const tableName = String(s.tableName || '');
                    const escapedTable = tableName.replace(/'/g, "\\'");
                    const isActive = activeTable && activeTable === tableName;
                    return `
                    <tr class="${isActive ? 'is-active' : ''}" onclick="focusTableFromPanel('${escapedTable}')">
                        <td><span class="table-panel-title">${escapeHtml(tableName)}</span>${isActive ? ' <span class="diff-badge badge-green">ACTIVE</span>' : ''}</td>
                        <td class="${numCls(s.total, 'info')}">${num(s.total).toLocaleString()}</td>
                        <td class="${numCls(s.hashMismatch, 'warn')}">${num(s.hashMismatch).toLocaleString()}</td>
                        <td class="${numCls(s.missingInDest, 'danger')}">${num(s.missingInDest).toLocaleString()}</td>
                        <td class="${numCls(s.missingInSource, 'info')}">${num(s.missingInSource).toLocaleString()}</td>
                        <td class="num"><button type="button" class="btn btn-sm btn-neutral" onclick="event.stopPropagation(); applyTablePanelFilter('${escapedTable}', null)">Filter</button></td>
                    </tr>`;
                }).join('')}
            </tbody>
        </table>`;
}

function applyTablePanelFilter(tableName, searchInputId) {
    const select = document.getElementById('tableFilterSelect');
    const searchBox = document.getElementById('searchBox');
    const localSearch = searchInputId ? (document.getElementById(searchInputId)?.value || '') : '';
    if (select) select.value = tableName;
    if (searchBox && searchInputId) searchBox.value = localSearch;
    currentPage = 1;
    loadResults();
    jumpToDiffLog();
}

function focusTableFromPanel(tableName) {
    const select = document.getElementById('tableFilterSelect');
    if (!select) return;
    const now = Date.now();
    // Bootstrap accordion interactions can emit duplicate fast clicks.
    if (lastPanelToggle.table === tableName && (now - lastPanelToggle.at) < 250) {
        return;
    }
    lastPanelToggle = { table: tableName, at: now };

    select.value = (select.value === tableName) ? '' : tableName;
    currentPage = 1;
    loadResults();
}

function clearTablePanelFilter(tableName) {
    const select = document.getElementById('tableFilterSelect');
    const searchBox = document.getElementById('searchBox');
    if (select && select.value === tableName) {
        select.value = '';
    }
    if (searchBox) {
        searchBox.value = '';
    }
    currentPage = 1;
    loadResults();
}

async function clearResults() {
    // Clear all cached counts so the summary bar resets to 0 after clearing.
    cachedTableRowCounts = {};
    saveConfigMap(TABLE_ROW_COUNTS_KEY, cachedTableRowCounts);
    try {
        localStorage.removeItem('reconciler_last_row_counts_all');
        localStorage.removeItem('reconciler_last_row_counts_latest');
    } catch (_) {}
    await fetch('/api/results', { method: 'DELETE' });
    currentPage = 1;
    loadResults();
    toast('Results cleared');
}

function nextPage() {
    currentPage += 1;
    loadResults();
}

function prevPage() {
    if (currentPage > 1) {
        currentPage -= 1;
        loadResults();
    }
}

/**
 * Download the FULL filtered Difference Log (all pages) as Excel or XML.
 * Streams from the server (no in-browser buffering) so it scales to millions
 * of rows. Uses the exact same scope + search + per-column filters as the grid.
 */
function exportResults(format) {
    const search = document.getElementById('searchBox')?.value || '';
    const table = document.getElementById('tableFilterSelect')?.value || '';
    const allHistory = Boolean(document.getElementById('allHistoryToggle')?.checked);
    const params = new URLSearchParams({ format, search, table, allHistory });
    Object.entries(currentColumnFilters()).forEach(([k, v]) => params.set(k, v));
    window.location.href = `/api/results/export?${params.toString()}`;
    toast(`Preparing ${format.toUpperCase()} download…`);
}

async function openDiffModal(pk, tableName = '') {
    document.getElementById('diffModalPk').textContent = tableName ? `${tableName} - ${pk}` : pk;
    diffModal.show();
    const body = document.getElementById('diffModalBody');
    body.innerHTML = '<tr><td colspan="4" class="text-secondary">Loading...</td></tr>';

    try {
        const res = await fetch(`/api/diff-details?pk=${encodeURIComponent(pk)}&table=${encodeURIComponent(tableName)}`);
        const data = await res.json();
        if (!res.ok) {
            body.innerHTML = `<tr><td colspan="4" class="text-danger">${data.error || 'Failed to load details'}</td></tr>`;
            return;
        }
        if (!data.rows || !data.rows.length) {
            body.innerHTML = '<tr><td colspan="4" class="text-secondary">No details available</td></tr>';
            return;
        }
        body.innerHTML = data.rows.map(r => `
            <tr ${r.different ? 'class="table-warning"' : ''}>
                <td class="font-monospace text-info">${escapeHtml(r.column || '')}</td>
                <td class="font-monospace">${escapeHtml(r.oracleValue || '')}</td>
                <td class="font-monospace">${escapeHtml(r.snowflakeValue || '')}</td>
                <td>${r.different ? '<span class="diff-badge badge-amber">YES</span>' : '<span class="diff-badge badge-green">NO</span>'}</td>
            </tr>`).join('');
    } catch (e) {
        body.innerHTML = `<tr><td colspan="4" class="text-danger">${escapeHtml(e.message)}</td></tr>`;
    }
}

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// Expose handlers for inline onclick usage.
window.scanMatchedTables = scanMatchedTables;
window.toggleAllTables = toggleAllTables;
window.runBatchSelected = runBatchSelected;
window.runBatchAll = runBatchAll;
window.loadSchema = loadSchema;
window.resetToStep1 = resetToStep1;
window.selectAllHash = selectAllHash;
window.clearResults = clearResults;
window.prevPage = prevPage;
window.nextPage = nextPage;
window.exportResults = exportResults;
window.openDiffModal = openDiffModal;
window.prefillFromTable = prefillFromTable;
window.removePill = removePill;
window.loadResults = loadResults;
window.applyTablePanelFilter = applyTablePanelFilter;
window.clearTablePanelFilter = clearTablePanelFilter;
window.onMatchedTableToggle = onMatchedTableToggle;
window.focusTableFromPanel = focusTableFromPanel;
window.toggleConfiguredTable = toggleConfiguredTable;
window.moveAllSchemas = moveAllSchemas;
window.loadOracleSchemas = loadOracleSchemas;
window.saveCurrentTableConfig = saveCurrentTableConfig;
window.copyHash = copyHash;
window.jumpToDiffLog = jumpToDiffLog;
window.setTablePanelSort = setTablePanelSort;

document.getElementById('tableSearchInput').addEventListener('input', () => {
    matchedTables = filterTablesByQuery(allMatchedTables, document.getElementById('tableSearchInput').value);
    renderMatchedTables();
});

document.getElementById('tableFilterSelect')?.addEventListener('change', () => {
    currentPage = 1;
    loadResults();
});

document.getElementById('allHistoryToggle')?.addEventListener('change', () => {
    currentPage = 1;
    const select = document.getElementById('tableFilterSelect');
    if (select) select.value = '';
    loadResults();
});

document.getElementById('srcDbSelect')?.addEventListener('change', () => {
    loadOracleSchemas();
});

// ═══════════════════════════════════════════════════════════════════════════════
// ── Jobs History ──────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

let jobsPage = 1;
const JOBS_PAGE_SIZE = 10;
let currentJobDetail = null;
let jobDiffPage = 1;
let jobDiffGridApi = null;
let selectedJobIds = new Set();

function statusBadgeClass(status) {
    return {
        RUNNING: 'badge-amber',
        COMPLETED: 'badge-green',
        FAILED: 'badge-red',
        PENDING: 'badge-slate',
        SKIPPED: 'badge-slate'
    }[status] || 'badge-blue';
}

/**
 * Renders the status cell for one Job History row — just the badge pill.
 * Per-table progress bars live only in the Job Detail panel (opened via View).
 */
function jobStatusCellHtml(j) {
    return `<span class="diff-badge ${statusBadgeClass(j.status)}">${j.status}</span>`;
}

async function loadJobs() {
    try {
        const res = await fetch(`/api/jobs?page=${jobsPage}&size=${JOBS_PAGE_SIZE}`);
        const data = await res.json();
        const rows = data.rows || [];
        const total = data.total || 0;
        const body = document.getElementById('jobsTableBody');
        const idsOnPage = new Set(rows.map(j => j.id));
        // Drop selections for jobs no longer present on this page load (e.g. after delete).
        selectedJobIds = new Set([...selectedJobIds].filter(id => idsOnPage.has(id)));
        if (!rows.length) {
            body.innerHTML = '<tr><td colspan="10" class="text-secondary text-center">No jobs yet</td></tr>';
        } else {
            body.innerHTML = rows.map(j => `
                <tr class="job-row ${currentJobDetail === j.id ? 'job-row-active' : ''}" style="cursor:pointer;" onclick="handleJobRowClick(event, ${j.id})">
                    <td class="font-monospace">${j.id}</td>
                    <td>${jobStatusCellHtml(j)}</td>
                    <td>${j.tableCount}</td>
                    <td>${Number(j.sourceCount || 0).toLocaleString()}</td>
                    <td>${Number(j.destCount || 0).toLocaleString()}</td>
                    <td class="text-warning">${Number(j.diffCount || 0).toLocaleString()}</td>
                    <td class="small">${j.startedAt ? j.startedAt.slice(0,19).replace('T',' ') : '—'}</td>
                    <td class="small">${j.finishedAt || '—'}</td>
                    <td>
                        <button class="btn btn-sm btn-view" onclick="event.stopPropagation(); toggleJobDetail(${j.id})">${currentJobDetail === j.id ? 'Hide' : 'View'}</button>
                    </td>
                    <td class="text-center"><input type="checkbox" class="form-check-input mt-0 job-select-check" value="${j.id}" ${selectedJobIds.has(j.id) ? 'checked' : ''} onclick="event.stopPropagation()" onchange="onJobCheckToggle(${j.id}, this.checked)" /></td>
                </tr>
            `).join('');
        }
        document.getElementById('jobsPageInfo').textContent = `Page ${jobsPage} — ${total} total`;
        const selectAll = document.getElementById('jobsSelectAllCheck');
        if (selectAll) {
            selectAll.checked = rows.length > 0 && rows.every(j => selectedJobIds.has(j.id));
        }
        updateJobsSelectionUi();
    } catch (e) {
        console.error('loadJobs failed', e);
    }
}

function onJobCheckToggle(jobId, checked) {
    if (checked) selectedJobIds.add(jobId);
    else selectedJobIds.delete(jobId);
    const selectAll = document.getElementById('jobsSelectAllCheck');
    if (selectAll) {
        const boxes = [...document.querySelectorAll('.job-select-check')];
        selectAll.checked = boxes.length > 0 && boxes.every(b => b.checked);
    }
    updateJobsSelectionUi();
}

function toggleAllJobsSelected(checked) {
    const boxes = [...document.querySelectorAll('.job-select-check')];
    boxes.forEach(b => {
        b.checked = checked;
        const id = Number(b.value);
        if (checked) selectedJobIds.add(id);
        else selectedJobIds.delete(id);
    });
    updateJobsSelectionUi();
}

function updateJobsSelectionUi() {
    const count = selectedJobIds.size;
    const countEl = document.getElementById('jobsSelectedCount');
    const delBtn = document.getElementById('deleteSelectedJobsBtn');
    if (countEl) {
        countEl.textContent = `${count} selected`;
        countEl.classList.toggle('hidden', count === 0);
    }
    if (delBtn) {
        delBtn.classList.toggle('hidden', count === 0);
    }
}

async function deleteSelectedJobs() {
    const ids = [...selectedJobIds];
    if (!ids.length) return;
    if (!confirm(`Delete ${ids.length} selected job(s) and all their diffs?`)) return;
    let failed = 0;
    for (const id of ids) {
        try {
            const res = await fetch(`/api/jobs/${id}`, { method: 'DELETE' });
            if (!res.ok) failed++;
            else if (currentJobDetail === id) closeJobDetail();
        } catch (_) {
            failed++;
        }
    }
    selectedJobIds.clear();
    toast(failed ? `Deleted ${ids.length - failed}/${ids.length} job(s), ${failed} failed` : `Deleted ${ids.length} job(s)`, failed > 0);
    loadJobs();
    loadResults();
}

function nextJobsPage() { jobsPage++; loadJobs(); }
function prevJobsPage() { if (jobsPage > 1) { jobsPage--; loadJobs(); } }

/** Ignore clicks on interactive elements (checkbox/buttons) inside the row; otherwise toggle detail. */
function handleJobRowClick(evt, jobId) {
    if (evt.target.closest('input, button, a')) return;
    toggleJobDetail(jobId);
}

function toggleJobDetail(jobId) {
    if (currentJobDetail === jobId) {
        closeJobDetail();
    } else {
        openJobDetail(jobId);
    }
    updateJobRowHighlight();
}

function updateJobRowHighlight() {
    document.querySelectorAll('#jobsTableBody tr.job-row').forEach(tr => {
        tr.classList.remove('job-row-active');
    });
    document.querySelectorAll('.job-select-check').forEach(cb => {
        const id = Number(cb.value);
        if (id === currentJobDetail) {
            cb.closest('tr')?.classList.add('job-row-active');
        }
    });
    document.querySelectorAll('#jobsTableBody tr.job-row .btn-view').forEach(btn => {
        const tr = btn.closest('tr');
        const cb = tr?.querySelector('.job-select-check');
        const id = cb ? Number(cb.value) : null;
        btn.textContent = id === currentJobDetail ? 'Hide' : 'View';
    });
}

async function openJobDetail(jobId) {
    currentJobDetail = jobId;
    jobDiffPage = 1;
    const panel = document.getElementById('jobDetailPanel');
    panel.classList.remove('hidden');
    document.getElementById('jobDetailId').textContent = `#${jobId}`;

    try {
        const res = await fetch(`/api/jobs/${jobId}`);
        const data = await res.json();
        if (!res.ok) { toast(data.error || 'Failed to load job', true); return; }

        const tables = data.tables || [];
        const host = document.getElementById('jobDetailTables');
        host.innerHTML = renderJobDetailTablesHtml(tables);

        initJobDiffGrid();
        loadJobDiffs();
    } catch (e) {
        toast('Failed: ' + e.message, true);
    }
}

/** Build the full tables panel HTML (used on first open). */
function renderJobDetailTablesHtml(tables) {
    if (!tables.length) return '<div class="text-secondary small px-1">No tables recorded for this job.</div>';
    return `<div class="table-responsive">
        <table class="table table-dark table-sm align-middle mb-0">
            <thead><tr>
                <th>Table</th><th>Status</th><th>Progress</th>
                <th>Source</th><th>Dest</th><th>Diffs</th>
                <th>Error</th><th>Started</th><th>Finished</th>
            </tr></thead>
            <tbody>
                ${tables.map(t => `
                <tr data-jtid="${t.id}">
                    <td class="font-monospace" style="max-width:220px;word-break:break-all;">${escapeHtml(t.tableName)}</td>
                    <td>${tableStatusBadgeHtml(t.status)}</td>
                    <td class="jtd-progress">${tableProgressHtml(t)}</td>
                    <td class="jtd-src font-monospace">${Number(t.sourceCount || 0).toLocaleString()}</td>
                    <td class="jtd-dest font-monospace">${Number(t.destCount || 0).toLocaleString()}</td>
                    <td class="jtd-diff text-warning font-monospace">${Number(t.diffCount || 0).toLocaleString()}</td>
                    <td class="small text-danger" style="max-width:180px;">${escapeHtml(t.error || '')}</td>
                    <td class="small text-secondary">${t.startedAt ? t.startedAt.slice(0,19).replace('T',' ') : '—'}</td>
                    <td class="small text-secondary">${t.finishedAt ? t.finishedAt.slice(0,19).replace('T',' ') : '—'}</td>
                </tr>`).join('')}
            </tbody>
        </table>
    </div>`;
}

/**
 * In-place DOM update of the Job Detail tables panel — patches only the
 * cells that change (status badge, progress bar, counts) without rebuilding
 * the whole table, so there's zero flicker and the scroll position is kept.
 */
async function refreshJobDetailTables(jobId) {
    const host = document.getElementById('jobDetailTables');
    if (!host) return;
    try {
        const res = await fetch(`/api/jobs/${jobId}`);
        if (!res.ok) return;
        const data = await res.json();
        const tables = data.tables || [];
        if (!tables.length) return;

        // If table was never rendered yet (e.g. panel was opened before tables were created)
        // do a full render.
        const tbody = host.querySelector('tbody');
        if (!tbody) {
            host.innerHTML = renderJobDetailTablesHtml(tables);
            return;
        }

        tables.forEach(t => {
            const row = tbody.querySelector(`tr[data-jtid="${t.id}"]`);
            if (!row) return;

            // Status badge
            const statusCell = row.querySelector('td:nth-child(2)');
            if (statusCell) statusCell.innerHTML = tableStatusBadgeHtml(t.status);

            // Progress bar (only meaningful while RUNNING; shows nothing otherwise)
            const progressCell = row.querySelector('.jtd-progress');
            if (progressCell) progressCell.innerHTML = tableProgressHtml(t);

            // Counts
            const srcCell = row.querySelector('.jtd-src');
            if (srcCell) srcCell.textContent = Number(t.sourceCount || 0).toLocaleString();
            const destCell = row.querySelector('.jtd-dest');
            if (destCell) destCell.textContent = Number(t.destCount || 0).toLocaleString();
            const diffCell = row.querySelector('.jtd-diff');
            if (diffCell) diffCell.textContent = Number(t.diffCount || 0).toLocaleString();
        });
    } catch (e) {
        console.error('refreshJobDetailTables failed', e);
    }
}

/** Just the status badge pill (no progress bar — that lives in its own cell). */
function tableStatusBadgeHtml(status) {
    return `<span class="diff-badge ${statusBadgeClass(status)}">${status}</span>`;
}

/**
 * The progress bar cell content for a single table row.
 * - RUNNING with known percent: show exact progress.
 * - RUNNING with unknown percent: show indeterminate animation immediately.
 * - COMPLETED: always show a 100% bar so fast-finished tables are still clear.
 */
function tableProgressHtml(t) {
    const pct = t.progressPercent;

    if (t.status === 'COMPLETED') {
        return `<div class="tbl-progress-wrap">
            <div class="tbl-progress-track tbl-progress-track-complete">
                <div class="tbl-progress-fill tbl-progress-fill-complete" style="width:100%"></div>
            </div>
            <div class="tbl-progress-pct">100%</div>
        </div>`;
    }

    if (t.status === 'RUNNING' && (typeof pct !== 'number' || pct < 0)) {
        return `<div class="tbl-progress-wrap">
            <div class="tbl-progress-track tbl-progress-track-running">
                <div class="tbl-progress-fill tbl-progress-fill-indeterminate"></div>
            </div>
            <div class="tbl-progress-pct">running...</div>
        </div>`;
    }

    if (t.status === 'RUNNING') {
        const p = Math.max(0, Math.min(100, pct));
        return `<div class="tbl-progress-wrap">
            <div class="tbl-progress-track tbl-progress-track-running">
                <div class="tbl-progress-fill" style="width:${p}%"></div>
            </div>
            <div class="tbl-progress-pct">${p}%</div>
        </div>`;
    }

    // FAILED (e.g. interrupted by an app restart) with a known last-checkpoint
    // percent: show a "stopped here" bar instead of leaving it blank, so it's
    // clear the scan wasn't a total failure — it got partway through before
    // being interrupted.
    if (t.status === 'FAILED' && typeof pct === 'number' && pct >= 0) {
        const p = Math.max(0, Math.min(100, pct));
        return `<div class="tbl-progress-wrap">
            <div class="tbl-progress-track tbl-progress-track-failed">
                <div class="tbl-progress-fill tbl-progress-fill-failed" style="width:${p}%"></div>
            </div>
            <div class="tbl-progress-pct">stopped at ${p}%</div>
        </div>`;
    }

    return '<span class="text-secondary small">—</span>';
}

/** @deprecated use tableStatusBadgeHtml + tableProgressHtml instead. */
function tableStatusCellHtml(t) {
    return tableStatusBadgeHtml(t.status);
}



function closeJobDetail() {
    currentJobDetail = null;
    document.getElementById('jobDetailPanel').classList.add('hidden');
}

function initJobDiffGrid() {
    const el = document.getElementById('jobDiffGrid');
    if (jobDiffGridApi) return;
    jobDiffGridApi = agGrid.createGrid(el, {
        columnDefs: [
            { field: 'id', headerName: 'ID', width: 70 },
            { field: 'tableName', headerName: 'Table', width: 150 },
            { field: 'pkComposite', headerName: 'Primary Key', flex: 2 },
            {
                field: 'diffType', headerName: 'Diff Type', width: 180,
                cellRenderer: p => {
                    const cls = { HASH_MISMATCH: 'badge-amber', MISSING_IN_DEST: 'badge-red', MISSING_IN_SOURCE: 'badge-blue' }[p.value] || 'badge-slate';
                    return `<span class="diff-badge ${cls}">${p.value}</span>`;
                }
            },
            { field: 'changedColumns', headerName: 'Changed Columns', flex: 1 },
            { field: 'createdAt', headerName: 'Detected At', width: 160 }
        ],
        rowData: [],
        defaultColDef: { sortable: true, resizable: true },
        animateRows: true
    });
}

async function loadJobDiffs() {
    if (!currentJobDetail) return;
    try {
        const res = await fetch(`/api/jobs/${currentJobDetail}/diffs?page=${jobDiffPage}&size=50`);
        const data = await res.json();
        if (jobDiffGridApi) jobDiffGridApi.setGridOption('rowData', data.rows || []);
        document.getElementById('jobDiffPageInfo').textContent = `Page ${data.page} — ${Number(data.totalRows || 0).toLocaleString()} total`;
    } catch (e) {
        console.error('loadJobDiffs failed', e);
    }
}

function nextJobDiffPage() { jobDiffPage++; loadJobDiffs(); }
function prevJobDiffPage() { if (jobDiffPage > 1) { jobDiffPage--; loadJobDiffs(); } }

async function deleteJob(jobId) {
    if (!confirm(`Delete job #${jobId} and all its diffs?`)) return;
    try {
        const res = await fetch(`/api/jobs/${jobId}`, { method: 'DELETE' });
        const data = await res.json();
        if (!res.ok) { toast(data.error || 'Delete failed', true); return; }
        toast(data.message || 'Job deleted');
        selectedJobIds.delete(jobId);
        if (currentJobDetail === jobId) closeJobDetail();
        loadJobs();
        loadResults();
    } catch (e) {
        toast('Delete failed: ' + e.message, true);
    }
}

// Load jobs on startup
loadJobs();

// Expose job handlers
window.loadJobs = loadJobs;
window.nextJobsPage = nextJobsPage;
window.prevJobsPage = prevJobsPage;
window.openJobDetail = openJobDetail;
window.toggleJobDetail = toggleJobDetail;
window.handleJobRowClick = handleJobRowClick;
window.closeJobDetail = closeJobDetail;
window.nextJobDiffPage = nextJobDiffPage;
window.prevJobDiffPage = prevJobDiffPage;
window.deleteJob = deleteJob;
window.onJobCheckToggle = onJobCheckToggle;
window.toggleAllJobsSelected = toggleAllJobsSelected;
window.deleteSelectedJobs = deleteSelectedJobs;

initDiffChart();
updateChartScopeLabel('');
loadSchemaMappings();
loadOracleDatabases();
refreshSaveButtonState();
loadResults();

