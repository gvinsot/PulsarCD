/**
 * PulsarCD frontend: Usage view: requests per endpoint, host and stack.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Usage ==============

// What the platform actually serves, from the same access index the Security
// view reads: requests per endpoint, per host and per stack. A request names
// the Traefik router that served it; the backend resolves the stack behind
// that router from the Swarm labels (see backend/usage_analytics.py).

const USAGE_POLL_INTERVAL = 60000;
let usagePollTimer = null;
let usageLoading = false;
let usageStacksLoaded = false;

function startUsagePolling() {
    stopUsagePolling();
    usagePollTimer = setInterval(() => {
        if (currentView === 'usage' && !document.hidden) loadUsage();
    }, USAGE_POLL_INTERVAL);
}

function stopUsagePolling() {
    if (usagePollTimer) {
        clearInterval(usagePollTimer);
        usagePollTimer = null;
    }
}

function usageWindow() {
    return document.getElementById('usage-window').value;
}

function usageStack() {
    return document.getElementById('usage-stack').value;
}

async function loadUsage() {
    if (usageLoading) return;
    usageLoading = true;
    try {
        const internal = document.getElementById('usage-internal').checked;
        const data = await apiGet(`/usage/overview?minutes=${encodeURIComponent(usageWindow())}`
            + `&include_internal=${internal}&stack=${encodeURIComponent(usageStack())}`);
        if (data) renderUsage(data);
    } finally {
        usageLoading = false;
    }
}

/** Filter the whole view on one stack, or on everything when it is already. */
function selectUsageStack(stack) {
    const select = document.getElementById('usage-stack');
    select.value = select.value === stack ? '' : (stack || '');
    loadUsage();
}

/** The stack selector's options, once the first answer names the stacks. */
function renderUsageStacks(stacks, selected) {
    const select = document.getElementById('usage-stack');
    // Rebuilt only when the deployed stacks change: an open dropdown must not
    // close under the poll.
    const current = Array.from(select.options).slice(1).map(o => o.value);
    if (usageStacksLoaded && String(current) === String(stacks)) {
        select.value = selected || '';
        return;
    }
    select.innerHTML = '<option value="">All stacks</option>'
        + stacks.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
    // A stack that no longer exists leaves the selector on "All stacks",
    // which is what the payload then reports anyway.
    select.value = stacks.includes(selected) ? selected : '';
    usageStacksLoaded = true;
}

function renderUsage(data) {
    const errorEl = document.getElementById('usage-error');
    if (data.error) {
        errorEl.textContent = `${data.error}. The access index is filled by the PulsarCD agent running on the Traefik node, from Traefik's JSON access log.`;
        errorEl.style.display = '';
    } else {
        errorEl.style.display = 'none';
    }

    renderUsageStacks(data.stacks || [], data.stack);

    const t = data.totals;
    document.getElementById('usage-requests').textContent = formatNumber(t.requests);
    document.getElementById('usage-rate').textContent = `${formatNumber(t.requests_per_minute)}/min`;
    document.getElementById('usage-clients').textContent = formatNumber(t.unique_ips);
    document.getElementById('usage-endpoints').textContent = formatNumber(t.endpoints);
    document.getElementById('usage-latency').textContent = formatDuration(t.p50);
    document.getElementById('usage-latency-sub').textContent =
        t.p95 === null ? '' : `p95 ${formatDuration(t.p95)}`;
    document.getElementById('usage-error-rate').textContent = `${formatNumber(t.error_rate)}%`;
    document.getElementById('usage-errors-sub').textContent =
        `${formatNumber(t.client_errors)} × 4xx, ${formatNumber(t.server_errors)} × 5xx`;
    document.getElementById('usage-bytes').textContent = formatBytes(t.bytes);
    document.getElementById('usage-interval').textContent =
        SECURITY_INTERVAL_LABELS[data.timeline_interval] || data.timeline_interval;

    renderUsageChart(data.timeline, data.window_minutes);
    renderUsageStackTable(data.by_stack, data.stack);
    renderUsageEndpoints(data.endpoints);
    renderUsageHosts(data.hosts);
    renderUsageBreakdowns(data.status_codes, data.methods);
    paintUsageBars();
}

function renderUsageChart(timeline, windowMinutes) {
    const labels = timeline.map(p => formatSecurityBucket(p.timestamp, windowMinutes));
    const series = [
        timeline.map(p => p.ok || 0),
        timeline.map(p => p.client_errors || 0),
        timeline.map(p => p.server_errors || 0),
        timeline.map(p => p.avg_duration_ms),
    ];

    if (charts.usage) {
        charts.usage.data.labels = labels;
        charts.usage.data.datasets.forEach((ds, i) => { ds.data = series[i]; });
        charts.usage.update('none');
        return;
    }

    // Volume on the left axis, latency on the right: a slow bucket is rarely
    // the busiest one, and the two together are what "usage" means here.
    const options = getChartOptionsDualAxis('Requests', 'Avg ms');
    options.scales.x.stacked = true;
    options.scales.y.stacked = true;
    options.scales.y.beginAtZero = true;
    options.scales.y.ticks.color = '#6e7681';
    options.scales.y.title.color = '#6e7681';
    options.scales.x.ticks.autoSkip = true;
    options.scales.x.ticks.maxTicksLimit = 12;

    charts.usage = new Chart(document.getElementById('usage-chart').getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: '2xx/3xx', data: series[0], backgroundColor: 'rgba(34, 197, 94, 0.6)' },
                { label: '4xx', data: series[1], backgroundColor: 'rgba(245, 158, 11, 0.75)' },
                { label: '5xx', data: series[2], backgroundColor: 'rgba(239, 68, 68, 0.8)' },
                {
                    label: 'Avg ms', data: series[3], type: 'line', yAxisID: 'y1',
                    borderColor: '#3b82f6', backgroundColor: '#3b82f6',
                    borderWidth: 1.5, pointRadius: 0, tension: 0.3, spanGaps: true,
                },
            ],
        },
        options,
    });
}

/** Milliseconds, in the unit that keeps the number readable. */
function formatDuration(ms) {
    if (ms === null || ms === undefined) return '-';
    if (ms >= 1000) return `${(ms / 1000).toFixed(ms >= 10000 ? 0 : 1)} s`;
    if (ms >= 10) return `${Math.round(ms)} ms`;
    return `${ms} ms`;
}

/** The row's share of the window, drawn behind its request count.
 *
 * The width is carried as data and applied by paintUsageBars once the rows
 * are in the document: a style attribute in markup is refused here, inline
 * styles being what the page's CSP exists to keep out. */
function usageBarCell(requests, share) {
    return `<td class="num usage-bar-cell">
        <span class="usage-bar" data-share="${Math.min(100, share).toFixed(1)}"></span>
        <span>${formatNumber(requests)}</span>
    </td>`;
}

function paintUsageBars() {
    document.querySelectorAll('#usage-view .usage-bar[data-share]').forEach(bar => {
        bar.style.width = `${bar.dataset.share}%`;
    });
}

function usageErrorCell(count, requests) {
    if (!count) return '<td class="num text-muted">&mdash;</td>';
    return `<td class="num">${formatNumber(count)}<span class="text-muted"> (${Math.round(100 * count / requests)}%)</span></td>`;
}

const UNATTRIBUTED_TITLE =
    'Requests no deployed stack claims: a host Traefik serves through its own '
    + 'file configuration, a stack removed since, or a request no router matched at all '
    + '(usually a scan of the bare IP address).';

function usageStackCell(stack) {
    if (!stack) {
        return `<span class="usage-unattributed" title="${escapeHtml(UNATTRIBUTED_TITLE)}">unattributed</span>`;
    }
    return `<span class="usage-stack-tag" data-click="selectUsageStack" data-args="${uiArgs(stack)}"
                  title="Filter this view on ${escapeHtml(stack)}">${escapeHtml(stack)}</span>`;
}

function renderUsageStackTable(rows, selected) {
    const card = document.getElementById('usage-stacks-card');
    const body = document.getElementById('usage-stacks-body');
    // Filtered on one stack, the table would be that single row: the totals
    // above it already say the same thing.
    card.style.display = selected ? 'none' : '';
    if (selected) return;
    if (!rows.length) {
        body.innerHTML = '<tr><td colspan="9" class="table-empty">No request in this window</td></tr>';
        return;
    }
    body.innerHTML = rows.map(row => {
        const routers = row.routers.join(', ');
        const name = row.stack
            ? `<span class="usage-stack-name">${escapeHtml(row.stack)}</span>`
            : `<span class="usage-unattributed" title="${escapeHtml(UNATTRIBUTED_TITLE)}">unattributed</span>`;
        // Only a named stack is clickable: "unattributed" is not a filter.
        const filter = row.stack
            ? ` class="usage-stack-row" data-click="selectUsageStack" data-args="${uiArgs(row.stack)}"`
            : '';
        return `
        <tr${filter}>
            <td>${name}</td>
            <td class="mono cell-ellipsis" title="${escapeHtml(routers)}">${escapeHtml(routers) || '&mdash;'}</td>
            ${usageBarCell(row.requests, row.share)}
            <td class="num">${formatNumber(row.share)}%</td>
            <td class="num">${formatNumber(row.distinct_ips)}</td>
            ${usageErrorCell(row.client_errors, row.requests)}
            ${usageErrorCell(row.server_errors, row.requests)}
            <td class="num">${formatDuration(row.avg_duration_ms)}</td>
            <td class="num">${formatBytes(row.bytes)}</td>
        </tr>`;
    }).join('');
}

function renderUsageEndpoints(endpoints) {
    const body = document.getElementById('usage-endpoints-body');
    if (!endpoints.length) {
        body.innerHTML = '<tr><td colspan="11" class="table-empty">No request in this window</td></tr>';
        return;
    }
    body.innerHTML = endpoints.map(ep => {
        const endpoint = `${ep.host}${ep.path}`;
        const methods = ep.methods.map(m => `<span class="usage-method">${escapeHtml(m)}</span>`).join('');
        return `
        <tr>
            <td class="mono cell-ellipsis" title="${escapeHtml(endpoint)}"><span class="text-muted">${escapeHtml(ep.host)}</span>${escapeHtml(ep.path)}</td>
            <td>${usageStackCell(ep.stack)}</td>
            <td>${methods}</td>
            ${usageBarCell(ep.requests, ep.share)}
            <td class="num">${formatNumber(ep.share)}%</td>
            <td class="num">${formatNumber(ep.distinct_ips)}</td>
            ${usageErrorCell(ep.client_errors, ep.requests)}
            ${usageErrorCell(ep.server_errors, ep.requests)}
            <td class="num">${formatDuration(ep.avg_duration_ms)}</td>
            <td class="num">${formatDuration(ep.max_duration_ms)}</td>
            <td class="security-nowrap">${escapeHtml(formatRelativeTime(ep.last_seen))}</td>
        </tr>`;
    }).join('');
}

function renderUsageHosts(hosts) {
    const body = document.getElementById('usage-hosts-body');
    if (!hosts.length) {
        body.innerHTML = '<tr><td colspan="9" class="table-empty">No request in this window</td></tr>';
        return;
    }
    body.innerHTML = hosts.map(h => `
        <tr>
            <td class="mono cell-ellipsis" title="${escapeHtml(h.host)}">${escapeHtml(h.host)}</td>
            ${usageBarCell(h.requests, h.share)}
            <td class="num">${formatNumber(h.share)}%</td>
            <td class="num">${formatNumber(h.distinct_paths)}</td>
            <td class="num">${formatNumber(h.distinct_ips)}</td>
            ${usageErrorCell(h.client_errors, h.requests)}
            ${usageErrorCell(h.server_errors, h.requests)}
            <td class="num">${formatDuration(h.avg_duration_ms)}</td>
            <td class="num">${formatBytes(h.bytes)}</td>
        </tr>`).join('');
}

function renderUsageBreakdowns(statusCodes, methods) {
    const statusEl = document.getElementById('usage-status-codes');
    statusEl.innerHTML = statusCodes.length
        ? statusCodes.map(s => {
            const klass = s.status >= 500 ? 'usage-chip-5xx' : s.status >= 400 ? 'usage-chip-4xx' : 'usage-chip-2xx';
            return `<span class="usage-chip ${klass}">${escapeHtml(String(s.status))}
                <span class="usage-chip-count">${formatNumber(s.count)}</span></span>`;
        }).join('')
        : '<span class="text-muted">No request in this window</span>';

    const methodsEl = document.getElementById('usage-methods');
    methodsEl.innerHTML = methods.length
        ? methods.map(m => `<span class="usage-chip">${escapeHtml(m.key)}
            <span class="usage-chip-count">${formatNumber(m.count)}</span></span>`).join('')
        : '<span class="text-muted">No request in this window</span>';
}
