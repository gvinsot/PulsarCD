/* Pipeline run history (Stacks view > History).
 *
 * Self-contained: it wires its own listeners on [data-run-history] elements
 * instead of going through app.js's UI_HANDLERS, and only borrows apiGet()
 * (core/api.js) and escapeHtml() (core/utils.js) at call time. Log content is only ever inserted
 * as text, never as markup.
 */
(function () {
    'use strict';

    const PAGE_SIZE = 25;
    const LOG_TAIL = 2000;
    const state = { page: 1, total: 0, filters: {}, reposLoaded: false };

    const modal = () => document.getElementById('run-history-modal');
    const q = (sel) => modal().querySelector(sel);

    function show(el, visible) {
        el.classList.remove('start-hidden');
        el.style.display = visible ? '' : 'none';
    }

    function fmtTime(iso) {
        if (!iso) return '-';
        const d = new Date(iso);
        return isNaN(d) ? '-' : d.toLocaleString();
    }

    function fmtDuration(seconds) {
        if (seconds === null || seconds === undefined) return '-';
        const s = Math.round(seconds);
        if (s < 60) return `${s}s`;
        const m = Math.floor(s / 60);
        if (m < 60) return `${m}m ${s % 60}s`;
        return `${Math.floor(m / 60)}h ${m % 60}m`;
    }

    function statusBadge(status) {
        const known = ['running', 'success', 'failed', 'gate_rejected', 'cancelled', 'interrupted'];
        const cls = known.includes(status) ? status : 'unknown';
        return `<span class="run-status run-status-${cls}">${escapeHtml((status || '?').replace('_', ' '))}</span>`;
    }

    function queryString(params) {
        const qs = new URLSearchParams();
        for (const [k, v] of Object.entries(params)) {
            if (v !== '' && v !== null && v !== undefined) qs.set(k, v);
        }
        return qs.toString();
    }

    async function loadRepos() {
        if (state.reposLoaded) return;
        const data = await apiGet('/pipeline/runs/repos');
        if (!data || !Array.isArray(data.repos)) return;
        const select = q('#rh-repo');
        for (const repo of data.repos) {
            const opt = document.createElement('option');
            opt.value = repo;
            opt.textContent = repo;
            select.appendChild(opt);
        }
        state.reposLoaded = true;
    }

    async function loadRuns() {
        const tbody = q('[data-rh-rows]');
        tbody.innerHTML = '<tr><td colspan="8" class="text-muted">Loading…</td></tr>';
        const data = await apiGet(`/pipeline/runs?${queryString({
            ...state.filters, page: state.page, page_size: PAGE_SIZE,
        })}`);
        if (!data) {
            tbody.innerHTML = '<tr><td colspan="8" class="text-muted">Run history is unavailable.</td></tr>';
            q('[data-rh-page]').textContent = '';
            return;
        }
        state.total = data.total || 0;
        const runs = data.runs || [];
        tbody.innerHTML = runs.length ? runs.map(run => `
            <tr class="run-history-row" data-rh-run="${escapeHtml(run.run_id || '')}" tabindex="0">
                <td>${escapeHtml(fmtTime(run.started_at))}</td>
                <td>${escapeHtml(run.repo || '')}</td>
                <td>${escapeHtml(run.stage || '')}</td>
                <td class="mono">${escapeHtml(run.version || '-')}</td>
                <td class="mono">${escapeHtml((run.commit || '-').slice(0, 10))}</td>
                <td>${escapeHtml(run.trigger || '')}${run.actor ? ` <span class="text-muted">(${escapeHtml(run.actor)})</span>` : ''}</td>
                <td>${escapeHtml(fmtDuration(run.duration_seconds))}</td>
                <td>${statusBadge(run.status)}</td>
            </tr>`).join('')
            : '<tr><td colspan="8" class="text-muted">No runs recorded for these filters.</td></tr>';
        const pages = Math.max(1, Math.ceil(state.total / PAGE_SIZE));
        q('[data-rh-page]').textContent = `Page ${state.page} / ${pages} · ${state.total} runs`;
        q('[data-run-history="prev"]').disabled = state.page <= 1;
        q('[data-run-history="next"]').disabled = state.page >= pages;
    }

    async function openRun(runId) {
        show(q('[data-rh-pane="list"]'), false);
        show(q('[data-rh-pane="detail"]'), true);
        const meta = q('[data-rh-meta]');
        const logEl = q('[data-rh-log]');
        const errEl = q('[data-rh-error]');
        meta.textContent = 'Loading…';
        logEl.textContent = '';
        show(errEl, false);
        const data = await apiGet(`/pipeline/runs/${encodeURIComponent(runId)}?log_offset=-${LOG_TAIL}&log_limit=${LOG_TAIL}`);
        if (!data || !data.run) {
            meta.textContent = 'Run not found.';
            return;
        }
        const run = data.run;
        meta.innerHTML = `
            <strong>${escapeHtml(run.repo || '')}</strong> · ${escapeHtml(run.stage || '')}
            · v${escapeHtml(run.version || '?')} ${statusBadge(run.status)}
            <span class="text-muted">· ${escapeHtml(fmtTime(run.started_at))} · ${escapeHtml(fmtDuration(run.duration_seconds))}
            · ${escapeHtml(run.trigger || '')}${run.actor ? ` by ${escapeHtml(run.actor)}` : ''}
            ${run.commit ? ` · commit <span class="mono">${escapeHtml(run.commit)}</span>` : ''}
            ${run.action_id ? ` · action ${escapeHtml(run.action_id)}` : ''}</span>`;
        if (run.error_excerpt) {
            errEl.textContent = run.error_excerpt;
            show(errEl, true);
        }
        const logs = data.logs || { lines: [], total: 0, offset: 0 };
        const shownFrom = logs.offset + 1;
        const shownTo = logs.offset + logs.lines.length;
        q('[data-rh-lognote]').textContent = logs.total
            ? `Showing lines ${shownFrom}–${shownTo} of ${logs.total}${logs.truncated ? ' (oldest lines were not stored)' : ''}.`
            : 'No log stored for this run.';
        const frag = document.createDocumentFragment();
        for (const line of logs.lines) {
            const div = document.createElement('div');
            div.className = 'log-line';
            if (/\b(error|failed|fatal)\b/i.test(line)) div.classList.add('log-error');
            div.textContent = line;
            frag.appendChild(div);
        }
        logEl.appendChild(frag);
        logEl.scrollTop = logEl.scrollHeight;
    }

    function backToList() {
        show(q('[data-rh-pane="detail"]'), false);
        show(q('[data-rh-pane="list"]'), true);
    }

    async function open() {
        backToList();
        modal().classList.add('active');
        loadRepos();
        await loadRuns();
    }

    function close() {
        modal().classList.remove('active');
    }

    function readFilters(form) {
        const data = new FormData(form);
        state.filters = {
            repo: data.get('repo') || '',
            stage: data.get('stage') || '',
            status: data.get('status') || '',
            q: (data.get('q') || '').trim(),
        };
        state.page = 1;
    }

    const ACTIONS = {
        open,
        close,
        back: backToList,
        prev() { if (state.page > 1) { state.page -= 1; loadRuns(); } },
        next() { state.page += 1; loadRuns(); },
    };

    document.addEventListener('click', (event) => {
        const target = event.target instanceof Element ? event.target : null;
        if (!target) return;
        const action = target.closest('[data-run-history]');
        if (action && Object.hasOwn(ACTIONS, action.dataset.runHistory)) {
            event.preventDefault();
            ACTIONS[action.dataset.runHistory]();
            return;
        }
        const row = target.closest('#run-history-modal [data-rh-run]');
        if (row && row.dataset.rhRun) openRun(row.dataset.rhRun);
    });

    document.addEventListener('keydown', (event) => {
        const target = event.target instanceof Element ? event.target : null;
        if (event.key === 'Enter' && target && target.matches('#run-history-modal [data-rh-run]')) {
            openRun(target.dataset.rhRun);
        } else if (event.key === 'Escape' && modal()?.classList.contains('active')) {
            close();
        }
    });

    document.addEventListener('submit', (event) => {
        const form = event.target;
        if (!(form instanceof HTMLFormElement) || !form.hasAttribute('data-rh-filters')) return;
        event.preventDefault();
        readFilters(form);
        loadRuns();
    });
})();
