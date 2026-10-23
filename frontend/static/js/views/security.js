/**
 * PulsarCD frontend: Security view: client IPs, endpoints, attacks, a request
 * explorer, WAF rules and the edge blocklist.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Security ==============

// Requests per client IP and per endpoint, from the Traefik access logs the
// agents copy into the access index. The detection rules (flags) and the
// attack categories are computed by the backend, see
// backend/security_analytics.py.
//
// The stat cards and the chart always cover the selected window. Clicking a
// bar focuses the tabs (client IPs, endpoints, attacks, requests, WAF rules)
// on that bucket: they are then computed over its time slot only.

const SECURITY_POLL_INTERVAL = 30000;
const SECURITY_INTERVAL_LABELS = { '1m': 'minute', '5m': '5 minutes', '15m': '15 minutes', '1h': 'hour' };
const SECURITY_INTERVAL_SECONDS = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600 };
const SECURITY_TABS = ['ips', 'endpoints', 'attacks', 'requests', 'edge'];
const SECURITY_TAB_KEY = 'pulsarcd.security.tab';
// Request explorer filters typed in its form; the others (path, uri, rule)
// come from a click elsewhere and show as removable chips.
const SECURITY_FORM_FILTERS = { ip: 'security-filter-ip', host: 'security-filter-host', q: 'security-filter-q',
    status: 'security-filter-status', category: 'security-filter-category' };
const SECURITY_CHIP_FILTERS = { path: 'path', uri: 'URI', rule: 'WAF rule' };
const SECURITY_EDGE_ROUTER = 'pulsarcd-blocklist';
let securityPollTimer = null;
let securityLoading = false;
let securityReloadPending = false;
let securityData = null;      // the whole window: cards and chart
let securityDetail = null;    // what the tabs show: the window, or the focused slot
let securitySlice = null;     // { start, end } (ISO) of the bar picked on the chart
let securitySelectedBar = -1;
let securitySliceFailed = false;
let securityTab = 'ips';
let securityRequestFilters = {};
let securityRequestsRequest = 0;
let securityIpRequest = 0;
let securityIpModalIp = null;

function startSecurityPolling() {
    stopSecurityPolling();
    securityPollTimer = setInterval(() => {
        if (currentView === 'security' && !document.hidden) loadSecurity();
    }, SECURITY_POLL_INTERVAL);
}

function stopSecurityPolling() {
    if (securityPollTimer) {
        clearInterval(securityPollTimer);
        securityPollTimer = null;
    }
}

function securityWindow() {
    return document.getElementById('security-window').value;
}

function securityInternal() {
    return document.getElementById('security-internal').checked;
}

/** Access-record timestamps are UTC; one written without a zone is taken as such. */
function parseUtc(iso) {
    if (!iso) return null;
    return new Date(/(Z|[+-]\d\d:?\d\d)$/.test(iso) ? iso : `${iso}Z`);
}

/** Query parameters of the range the tabs cover: the focused slot, or the window. */
function securityRangeParams() {
    const params = new URLSearchParams({ minutes: securityWindow() });
    if (securitySlice) {
        params.set('start', securitySlice.start);
        params.set('end', securitySlice.end);
    }
    return params;
}

async function loadSecurity() {
    // A bar clicked while a load is in flight must not be lost: it is loaded
    // once the current one is done.
    if (securityLoading) {
        securityReloadPending = true;
        return;
    }
    securityLoading = true;
    try {
        const slice = securitySlice;
        const internal = securityInternal();
        const sliceParams = securityRangeParams();
        sliceParams.set('include_internal', internal);
        const [data, blocklist, sliceData] = await Promise.all([
            apiGet(`/security/overview?minutes=${encodeURIComponent(securityWindow())}&include_internal=${internal}`),
            apiGet('/security/blocklist'),
            slice ? apiGet(`/security/overview?${sliceParams}`) : null,
        ]);
        if (blocklist) renderSecurityBlocklist(blocklist);
        if (!data) return;
        securityData = data;
        if (slice !== securitySlice) return;  // refocused meanwhile: the pending load renders
        securitySliceFailed = Boolean(slice) && !sliceData;
        securityDetail = slice ? sliceData : data;
        renderSecurity(data, securityDetail);
        if (securityTab === 'requests') loadSecurityRequests();
    } finally {
        securityLoading = false;
        if (securityReloadPending) {
            securityReloadPending = false;
            loadSecurity();
        }
    }
}

function changeSecurityWindow() {
    // A slot of the previous window may not exist in the new one.
    securitySlice = null;
    loadSecurity();
}

function renderSecurity(data, detail) {
    const errorEl = document.getElementById('security-error');
    const error = data.error || (detail && detail.error);
    if (error) {
        errorEl.textContent = `${error}. The access index is filled by the PulsarCD agent running on the Traefik node, from Traefik's JSON access log.`;
        errorEl.style.display = '';
    } else {
        errorEl.style.display = 'none';
    }

    const t = data.totals;
    document.getElementById('security-requests').textContent = formatNumber(t.requests);
    document.getElementById('security-unique-ips').textContent = formatNumber(t.unique_ips);
    document.getElementById('security-flagged').textContent = formatNumber(t.flagged_ips);
    document.getElementById('security-waf').textContent = formatNumber(t.waf_blocks);
    document.getElementById('security-denied').textContent = formatNumber(t.denied);
    document.getElementById('security-4xx').textContent = formatNumber(t.client_errors);
    document.getElementById('security-429').textContent = formatNumber(t.rate_limited);
    document.getElementById('security-interval').textContent =
        SECURITY_INTERVAL_LABELS[data.timeline_interval] || data.timeline_interval;

    renderSecurityChart(data.timeline, data.window_minutes);
    renderSecurityCategories(data.attack_categories || []);
    renderSecurityFocus();
    renderSecurityDetail(detail);
}

/** The tabs' content, or a placeholder while the focused slot loads (or failed to). */
function renderSecurityDetail(detail) {
    if (!detail) {
        const text = securitySliceFailed ? 'Could not load this time slot' : 'Loading&hellip;';
        document.getElementById('security-ips-body').innerHTML = `<tr><td colspan="11" class="security-empty">${text}</td></tr>`;
        document.getElementById('security-endpoints-body').innerHTML = `<tr><td colspan="8" class="security-empty">${text}</td></tr>`;
        document.getElementById('security-attacks-body').innerHTML = `<tr><td colspan="7" class="security-empty">${text}</td></tr>`;
        document.getElementById('security-waf-rules').innerHTML = '';
        ['ips', 'endpoints', 'attacks'].forEach(tab => setSecurityTabCount(tab, ''));
        return;
    }
    const scope = securitySlice ? 'in this time slot' : 'in this window';
    renderSecurityIps(detail.ips, scope);
    renderSecurityEndpoints(detail.endpoints, scope);
    renderSecurityAttacks(detail.attacks || [], scope);
    renderSecurityWafRules(detail.waf_rules, scope);

    const flagged = detail.totals.flagged_ips;
    setSecurityTabCount('ips', formatNumber(detail.ips.length), flagged > 0,
        flagged ? `${flagged} flagged` : '');
    setSecurityTabCount('endpoints', formatNumber(detail.endpoints.length), detail.totals.flagged_endpoints > 0);
    const answered = detail.totals.attack_answered || 0;
    setSecurityTabCount('attacks', formatNumber(detail.totals.attack_requests || 0), answered > 0,
        answered ? `${answered} answered with a 2xx` : '');
}

function setSecurityTabCount(tab, text, alert = false, title = '') {
    const el = document.getElementById(`security-count-${tab}`);
    el.textContent = text;
    el.title = title;
    el.classList.toggle('security-tab-count-alert', alert);
}

// ---- Tabs ----------------------------------------------------------------

function switchSecurityTab(tab) {
    if (!SECURITY_TABS.includes(tab)) return;
    const changed = tab !== securityTab;
    securityTab = tab;
    try { localStorage.setItem(SECURITY_TAB_KEY, tab); } catch { /* private mode */ }
    document.querySelectorAll('#security-view .security-tab').forEach(btn => {
        const active = btn.dataset.tab === tab;
        btn.classList.toggle('active', active);
        btn.setAttribute('aria-selected', String(active));
    });
    SECURITY_TABS.forEach(name => {
        document.getElementById(`security-panel-${name}`).style.display = name === tab ? '' : 'none';
    });
    if (tab === 'requests' && changed) loadSecurityRequests();
}

function restoreSecurityTab() {
    let saved = null;
    try { saved = localStorage.getItem(SECURITY_TAB_KEY); } catch { /* private mode */ }
    if (saved && SECURITY_TABS.includes(saved)) {
        securityTab = null;
        switchSecurityTab(saved);
    }
}

// ---- Timeline and the focused time slot --------------------------------------

function formatSecurityBucket(iso, windowMinutes) {
    const date = parseUtc(iso);
    if (windowMinutes > 1440) {
        return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', hour12: false });
    }
    return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
}

function formatSecuritySlice(slice) {
    const start = parseUtc(slice.start);
    const end = parseUtc(slice.end);
    const day = start.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    const time = d => d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
    return `${day}, ${time(start)}–${time(end)}`;
}

/** The slot of bar `index`, or null to clear the focus (the focused bar clicked again). */
function securitySliceFor(timeline, interval, index, current) {
    const point = timeline[index];
    if (!point) return current;
    const start = parseUtc(point.timestamp);
    if (current && parseUtc(current.start).getTime() === start.getTime()) return null;
    const seconds = SECURITY_INTERVAL_SECONDS[interval] || 60;
    return { start: start.toISOString(), end: new Date(start.getTime() + seconds * 1000).toISOString() };
}

function focusSecurityBar(index) {
    if (!securityData) return;
    securitySlice = securitySliceFor(securityData.timeline, securityData.timeline_interval, index, securitySlice);
    // The blocklist does not depend on time: show what the slot is about.
    if (securitySlice && securityTab === 'edge') switchSecurityTab('endpoints');
    securityDetail = securitySlice ? null : securityData;
    securitySliceFailed = false;
    renderSecurity(securityData, securityDetail);
    if (securitySlice) {
        loadSecurity();
    } else if (securityTab === 'requests') {
        loadSecurityRequests();
    }
}

function clearSecuritySlice() {
    if (!securitySlice) return;
    securitySlice = null;
    securityDetail = securityData;
    if (securityData) renderSecurity(securityData, securityDetail);
    if (securityTab === 'requests') loadSecurityRequests();
}

function renderSecurityFocus() {
    const banner = document.getElementById('security-focus');
    if (securitySlice) {
        document.getElementById('security-focus-label').textContent = formatSecuritySlice(securitySlice);
        banner.style.display = '';
    } else {
        banner.style.display = 'none';
    }
}

// Colours of the 2xx/3xx, 4xx and 5xx series.
const SECURITY_SERIES_COLORS = [['34, 197, 94', 0.6], ['245, 158, 11', 0.75], ['239, 68, 68', 0.8]];

/** A bar's colour, dimmed when another bar is focused. */
function securityBarColor([rgb, alpha]) {
    return ctx => `rgba(${rgb}, ${securitySelectedBar >= 0 && ctx.dataIndex !== securitySelectedBar ? alpha * 0.3 : alpha})`;
}

function renderSecurityChart(timeline, windowMinutes) {
    const labels = timeline.map(p => formatSecurityBucket(p.timestamp, windowMinutes));
    const series = [
        timeline.map(p => p.ok || 0),
        timeline.map(p => p.client_errors || 0),
        timeline.map(p => p.server_errors || 0),
    ];
    const focused = securitySlice ? parseUtc(securitySlice.start).getTime() : null;
    securitySelectedBar = focused === null ? -1
        : timeline.findIndex(p => parseUtc(p.timestamp).getTime() === focused);

    // Updated in place on refresh: rebuilding the chart every 30 s flickers.
    if (charts.security) {
        charts.security.data.labels = labels;
        charts.security.data.datasets.forEach((ds, i) => { ds.data = series[i]; });
        charts.security.update('none');
        return;
    }

    const options = getChartOptions();
    options.interaction = { mode: 'index', intersect: false };
    options.scales.x.stacked = true;
    options.scales.y.stacked = true;
    options.scales.x.ticks.autoSkip = true;
    options.scales.x.ticks.maxTicksLimit = 12;
    const barAt = (event, chart) =>
        chart.getElementsAtEventForMode(event, 'index', { intersect: false }, true)[0];
    options.onClick = (event, _elements, chart) => {
        const bar = barAt(event, chart);
        if (bar) focusSecurityBar(bar.index);
    };
    options.onHover = (event, _elements, chart) => {
        chart.canvas.style.cursor = barAt(event, chart) ? 'pointer' : 'default';
    };
    // A bar chart's legend takes the colour of its first bar, dimmed when
    // another one is focused: give it the series colour instead.
    options.plugins.legend.labels.generateLabels = chart =>
        Chart.defaults.plugins.legend.labels.generateLabels(chart).map(item => {
            const [rgb, alpha] = SECURITY_SERIES_COLORS[item.datasetIndex];
            return { ...item, fillStyle: `rgba(${rgb}, ${alpha})`, strokeStyle: `rgba(${rgb}, ${alpha})` };
        });

    charts.security = new Chart(document.getElementById('security-chart').getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: '2xx/3xx', data: series[0], backgroundColor: securityBarColor(SECURITY_SERIES_COLORS[0]) },
                { label: '4xx', data: series[1], backgroundColor: securityBarColor(SECURITY_SERIES_COLORS[1]) },
                { label: '5xx', data: series[2], backgroundColor: securityBarColor(SECURITY_SERIES_COLORS[2]) },
            ],
        },
        options,
    });
}

// ---- Client IPs, endpoints, attacks, WAF rules -----------------------------

function securityFlagBadges(flags) {
    if (!flags || !flags.length) return '<span class="text-muted">&mdash;</span>';
    return flags.map(f =>
        `<span class="security-flag security-flag-${escapeHtml(f.severity)}" title="${escapeHtml(f.detail)}">${escapeHtml(f.label)}</span>`
    ).join('');
}

function securityRowClass(score) {
    if (score >= 3) return 'security-row-high';
    if (score > 0) return 'security-row-medium';
    return '';
}

function securityShare(part, total) {
    return total ? ` <span class="text-muted">(${Math.round(100 * part / total)}%)</span>` : '';
}

function securityIpLink(ip) {
    return `<span class="mono security-link" data-click="openSecurityIp" data-args="${uiArgs(ip)}" data-click-stop>${escapeHtml(ip)}</span>`;
}

function renderSecurityIps(ips, scope) {
    const body = document.getElementById('security-ips-body');
    if (!ips.length) {
        body.innerHTML = `<tr><td colspan="11" class="security-empty">No request ${scope}</td></tr>`;
        return;
    }
    body.innerHTML = ips.map(ip => {
        const hosts = ip.top_hosts.map(h => h.key).join(', ');
        return `
        <tr class="security-row ${securityRowClass(ip.score)}" data-click="openSecurityIp" data-args="${uiArgs(ip.ip)}">
            <td class="mono">${escapeHtml(ip.ip)}${ip.internal ? ' <span class="security-internal">internal</span>' : ''}</td>
            <td class="security-flags-cell">${securityFlagBadges(ip.flags)}</td>
            <td class="num">${formatNumber(ip.requests)}</td>
            <td class="num">${formatNumber(ip.peak_per_minute)}</td>
            <td class="num">${formatNumber(ip.client_errors)}${securityShare(ip.client_errors, ip.requests)}</td>
            <td class="num">${formatNumber(ip.waf_blocks)}</td>
            <td class="num">${formatNumber(ip.distinct_paths)}</td>
            <td class="security-ellipsis" title="${escapeHtml(hosts)}">${escapeHtml(hosts) || '&mdash;'}</td>
            <td class="security-ellipsis" title="${escapeHtml(ip.user_agent || '')}">${escapeHtml(ip.user_agent || '') || '&mdash;'}</td>
            <td>${formatRelativeTime(ip.last_seen)}</td>
            <td class="security-action-cell">${securityBlockButton(ip.ip, ip.blocked, ip.internal, true)}</td>
        </tr>`;
    }).join('');
}

function renderSecurityEndpoints(endpoints, scope) {
    const body = document.getElementById('security-endpoints-body');
    if (!endpoints.length) {
        body.innerHTML = `<tr><td colspan="8" class="security-empty">No request ${scope}</td></tr>`;
        return;
    }
    body.innerHTML = endpoints.map(ep => {
        const top = ep.top_ips[0];
        const topClient = top ? `${securityIpLink(top.key)}${securityShare(top.count, ep.requests)}` : '&mdash;';
        const endpoint = `${ep.host}${ep.path}`;
        return `
        <tr class="security-row ${securityRowClass(ep.score)}" data-click="showSecurityRequests" data-args="${uiArgs({ host: ep.host, path: ep.path })}" title="Show the requests of this endpoint">
            <td class="mono security-ellipsis" title="${escapeHtml(endpoint)}"><span class="text-muted">${escapeHtml(ep.host)}</span>${escapeHtml(ep.path)}</td>
            <td class="security-flags-cell">${securityFlagBadges(ep.flags)}</td>
            <td class="num">${formatNumber(ep.requests)}</td>
            <td class="num">${formatNumber(ep.peak_per_minute)}</td>
            <td class="num">${formatNumber(ep.distinct_ips)}</td>
            <td class="num">${formatNumber(ep.client_errors)}${securityShare(ep.client_errors, ep.requests)}</td>
            <td class="num">${formatNumber(ep.server_errors)}</td>
            <td>${topClient}</td>
        </tr>`;
    }).join('');
}

function renderSecurityAttacks(attacks, scope) {
    const body = document.getElementById('security-attacks-body');
    if (!attacks.length) {
        body.innerHTML = `<tr><td colspan="7" class="security-empty">No known attack pattern ${scope}</td></tr>`;
        return;
    }
    body.innerHTML = attacks.map(cat => {
        // A category read from the URI shows the whole URI: its payload is the point.
        const targets = cat.top_targets.map(t => {
            const filters = cat.field === 'uri' ? { uri: t.target } : { path: t.target };
            const title = `${t.target}: ${t.count} request(s)${t.answered ? `, ${t.answered} answered with a 2xx` : ''}`
                + ` on ${t.hosts.join(', ')}`;
            return `<span class="security-chip mono${t.answered ? ' security-chip-alert' : ''}" title="${escapeHtml(title)}" data-click="showSecurityRequests" data-args="${uiArgs(filters)}" data-click-stop>${escapeHtml(t.target)} <span class="text-muted">&times;${formatNumber(t.count)}</span></span>`;
        }).join('');
        const clients = cat.top_ips.map(c =>
            `<span class="security-chip mono" data-click="openSecurityIp" data-args="${uiArgs(c.key)}" data-click-stop>${escapeHtml(c.key)} <span class="text-muted">&times;${formatNumber(c.count)}</span></span>`
        ).join('');
        const answered = cat.answered
            ? `<span class="security-status-error" title="Check these targets did not serve anything sensitive">${formatNumber(cat.answered)}</span>`
            : '<span class="text-muted">0</span>';
        return `
        <tr class="security-row${cat.answered ? ' security-row-high' : ''}" data-click="showSecurityRequests" data-args="${uiArgs({ category: cat.code })}" title="Show these requests">
            <td><strong>${escapeHtml(cat.label)}</strong><div class="security-attack-desc">${escapeHtml(cat.description)}</div></td>
            <td class="num">${formatNumber(cat.requests)}</td>
            <td class="num">${formatNumber(cat.clients)}</td>
            <td class="num">${answered}</td>
            <td><div class="security-chips">${targets || '&mdash;'}</div></td>
            <td><div class="security-chips">${clients || '&mdash;'}</div></td>
            <td class="security-nowrap">${formatRelativeTime(cat.last_seen)}</td>
        </tr>`;
    }).join('');
}

function renderSecurityWafRules(rules, scope) {
    const el = document.getElementById('security-waf-rules');
    if (!rules || !rules.length) {
        el.innerHTML = `<span class="text-muted">No WAF block ${scope}</span>`;
        return;
    }
    el.innerHTML = rules.map(r =>
        `<span class="security-chip" data-click="showSecurityRequests" data-args="${uiArgs({ status: 'waf', rule: r.key })}">${escapeHtml(r.key)} <span class="text-muted">&times;${formatNumber(r.count)}</span></span>`
    ).join('');
}

/** The explorer's attack choices, from the backend's catalogue (filled once). */
function renderSecurityCategories(categories) {
    const select = document.getElementById('security-filter-category');
    if (select.options.length > 1 || !categories.length) return;
    categories.forEach(cat => select.add(new Option(cat.label, cat.code)));
    select.value = securityRequestFilters.category || '';
}

// ---- Request explorer ------------------------------------------------------

/** Open the Requests tab on these filters (from a row, a chip or a rule elsewhere). */
function showSecurityRequests(filters) {
    securityRequestFilters = { ...filters };
    for (const [name, id] of Object.entries(SECURITY_FORM_FILTERS)) {
        document.getElementById(id).value = securityRequestFilters[name] || '';
    }
    if (securityTab === 'requests') {
        loadSecurityRequests();
    } else {
        switchSecurityTab('requests');
    }
}

function applySecurityRequestFilters() {
    const filters = {};
    for (const name of Object.keys(SECURITY_CHIP_FILTERS)) {
        if (securityRequestFilters[name]) filters[name] = securityRequestFilters[name];
    }
    for (const [name, id] of Object.entries(SECURITY_FORM_FILTERS)) {
        const value = document.getElementById(id).value.trim();
        if (value) filters[name] = value;
    }
    securityRequestFilters = filters;
    loadSecurityRequests();
}

function resetSecurityRequestFilters() {
    showSecurityRequests({});
}

function removeSecurityRequestFilter(name) {
    delete securityRequestFilters[name];
    loadSecurityRequests();
}

function renderSecurityRequestChips() {
    document.getElementById('security-request-chips').innerHTML = Object.entries(SECURITY_CHIP_FILTERS)
        .filter(([name]) => securityRequestFilters[name])
        .map(([name, label]) => `
            <span class="security-chip security-chip-filter">
                <span class="text-muted">${label}:</span> <span class="mono">${escapeHtml(securityRequestFilters[name])}</span>
                <button class="security-chip-remove" title="Remove this filter" data-click="removeSecurityRequestFilter" data-args="${uiArgs(name)}">&times;</button>
            </span>`).join('');
}

function securityStatusClass(status) {
    if (status >= 500) return 'security-status-error';
    if (status >= 400) return 'security-status-warning';
    return 'security-status-ok';
}

function securityEventStatus(e) {
    if (e.event === 'waf_block') {
        return `<span class="security-flag security-flag-high" title="${escapeHtml(e.rule_msg || '')}">WAF</span>`;
    }
    const status = `<span class="${securityStatusClass(e.status)}">${escapeHtml(String(e.status ?? '-'))}</span>`;
    return (e.router || '').startsWith(SECURITY_EDGE_ROUTER)
        ? `${status} <span class="security-flag security-flag-high" title="Refused at the edge: this client is blocked">Edge</span>`
        : status;
}

/** One request or WAF block; `withClient` adds the client column (explorer). */
function securityEventRow(e, withClient) {
    const time = parseUtc(e.timestamp).toLocaleString('en-US', {
        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
    });
    const host = e.request_host || e.router || '';
    return `
        <tr>
            <td class="mono security-nowrap">${escapeHtml(time)}</td>
            ${withClient ? `<td>${e.client_ip ? securityIpLink(e.client_ip) : ''}</td>` : ''}
            <td class="security-nowrap">${securityEventStatus(e)}</td>
            <td>${escapeHtml(e.method || '')}</td>
            <td class="security-ellipsis" title="${escapeHtml(host)}">${escapeHtml(host)}</td>
            <td class="mono security-uri" title="${escapeHtml(e.uri || '')}">${escapeHtml(e.uri || '')}</td>
            <td class="security-ellipsis" title="${escapeHtml(e.user_agent || '')}">${escapeHtml(e.user_agent || '')}</td>
        </tr>`;
}

async function loadSecurityRequests() {
    const body = document.getElementById('security-requests-body');
    const status = document.getElementById('security-requests-status');
    renderSecurityRequestChips();
    const params = securityRangeParams();
    params.set('include_internal', securityInternal());
    for (const [name, value] of Object.entries(securityRequestFilters)) {
        if (value) params.set(name, value);
    }
    // A slower answer for earlier filters must not overwrite this one.
    const requestId = ++securityRequestsRequest;
    status.textContent = 'Loading…';
    const { data, error } = await apiGetOrError(`/security/requests?${params}`);
    if (requestId !== securityRequestsRequest) return;
    const scope = securitySlice ? `in ${formatSecuritySlice(securitySlice)}` : 'in this window';
    if (error || data.error) {
        status.textContent = '';
        body.innerHTML = `<tr><td colspan="7" class="security-empty">${escapeHtml(error || data.error)}</td></tr>`;
        return;
    }
    const events = data.events;
    if (!events.length) {
        status.textContent = '';
        body.innerHTML = `<tr><td colspan="7" class="security-empty">No matching request ${escapeHtml(scope)}</td></tr>`;
        return;
    }
    status.textContent = events.length >= data.limit
        ? `The ${formatNumber(data.limit)} most recent matching requests ${scope}: narrow the filters or pick a bar to see older ones.`
        : `${formatNumber(events.length)} matching request(s) ${scope}, newest first.`;
    body.innerHTML = events.map(e => securityEventRow(e, true)).join('');
}

document.getElementById('security-request-filters').addEventListener('submit', (e) => {
    e.preventDefault();
    applySecurityRequestFilters();
});

restoreSecurityTab();

// ---- Blocking a client at the edge ----------------------------------------
// A blocked address is refused by Traefik in front of Coraza, for every host
// it serves. The backend keeps the list and the edge polls it, so a block is
// live within one poll interval; see backend/ip_blocklist.py.

/**
 * The Block / Unblock control for one address.
 *
 * `blocked` is the entry the backend matched, which may be a CIDR range that
 * merely covers this address: releasing that needs the range, not the address,
 * so the row shows what blocks it instead of a button that would fail.
 */
function securityBlockButton(ip, blocked, internal, stopClick) {
    const stop = stopClick ? ' data-click-stop' : '';
    if (blocked && blocked.ip !== ip) {
        return `<span class="security-flag security-flag-high" title="Covered by the blocked range ${escapeHtml(blocked.ip)}">Blocked</span>`;
    }
    if (!isAdmin()) {
        return blocked ? '<span class="security-flag security-flag-high">Blocked</span>' : '';
    }
    if (blocked) {
        return `<button class="btn btn-sm btn-secondary security-action-btn" data-click="unblockIp" data-args="${uiArgs(ip)}"${stop}>Unblock</button>`;
    }
    if (internal) {
        // The backend refuses these outright: blocking a cluster address would
        // cut the fleet's own traffic.
        return '<span class="text-muted" title="Internal addresses cannot be blocked">&mdash;</span>';
    }
    return `<button class="btn btn-sm btn-danger security-action-btn" data-click="blockIp" data-args="${uiArgs(ip)}"${stop}>Block</button>`;
}

function renderSecurityBlocklist(data) {
    const warning = document.getElementById('security-blocklist-warning');
    if (data.edge_configured) {
        warning.style.display = 'none';
    } else {
        warning.textContent = 'The edge cannot fetch this list: PULSARCD_AUTH__EDGE_KEY is not set on the backend, '
            + 'or the matching bearer token is missing from the edge\'s providers.http configuration. '
            + 'Addresses listed here are recorded but nothing is refused.';
        warning.style.display = '';
    }

    document.querySelector('.security-block-form').style.display = isAdmin() ? '' : 'none';

    const body = document.getElementById('security-blocklist-body');
    const entries = data.entries || [];
    setSecurityTabCount('edge', entries.length ? formatNumber(entries.length) : '', false,
        entries.length ? `${entries.length} blocked address(es) or range(s)` : '');
    if (!entries.length) {
        body.innerHTML = '<tr><td colspan="6" class="security-empty">No address is blocked</td></tr>';
        return;
    }
    body.innerHTML = entries.map(e => `
        <tr>
            <td class="mono">${escapeHtml(e.ip)}</td>
            <td class="security-ellipsis" title="${escapeHtml(e.reason || '')}">${escapeHtml(e.reason || '') || '&mdash;'}</td>
            <td class="security-ellipsis" title="${escapeHtml(e.blocked_by || '')}">${escapeHtml(e.blocked_by || '') || '&mdash;'}</td>
            <td>${formatRelativeTime(e.blocked_at)}</td>
            <td>${e.expires_at ? escapeHtml(parseUtc(e.expires_at).toLocaleString()) : 'Permanent'}</td>
            <td class="security-action-cell">${securityBlockButton(e.ip, e, false, false)}</td>
        </tr>`).join('');
}

/**
 * Send one blocklist change. Returns the entry the backend stored, or null
 * when it refused: the reason it gives is what the operator has to read, so it
 * is shown rather than collapsed into a generic failure.
 */
async function _blocklistRequest(method, path, body) {
    try {
        const resp = await fetch(`${API_BASE}${path}`, {
            method,
            headers: body ? { ...authHeaders(), 'Content-Type': 'application/json' } : authHeaders(),
            body: body ? JSON.stringify(body) : undefined,
        });
        if (resp.status === 401) { showLogin(); return null; }
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            showNotification('error', err.detail || `Request failed (HTTP ${resp.status})`);
            return null;
        }
        return await resp.json();
    } catch {
        showNotification('error', 'Connection error');
        return null;
    }
}

/** Returns whether the address ended up blocked, for the form below. */
async function blockIp(ip, reason) {
    const what = ip.includes('/') ? 'range' : 'address';
    if (!confirm(`Block ${ip} at the edge?\n\n`
        + `Traefik will answer 403 to every request from this ${what}, on every host it serves, `
        + 'until it is unblocked here.')) return false;
    const entry = await _blocklistRequest('POST', '/security/blocklist', { ip, reason: reason || '' });
    if (!entry) return false;
    // entry.ip, not what was typed: the backend canonicalises a range, and
    // "103.4.8.17/24 is now blocked" would misname what is actually refused.
    showNotification('success', `${entry.ip} is now blocked at the edge`);
    if (securityIpModalIp === entry.ip) setSecurityIpModalAction(entry.ip, entry, false);
    await loadSecurity();
    return true;
}

async function unblockIp(ip) {
    if (!confirm(`Unblock ${ip}?\n\nThe edge will serve it again within a few seconds.`)) return;
    if (!await _blocklistRequest('DELETE', `/security/blocklist?ip=${encodeURIComponent(ip)}`)) return;
    showNotification('success', `${ip} is no longer blocked`);
    if (securityIpModalIp === ip) setSecurityIpModalAction(ip, null, false);
    await loadSecurity();
}

/** Block the address typed into the card's own form. */
async function blockIpFromForm() {
    const ipInput = document.getElementById('security-block-ip');
    const reasonInput = document.getElementById('security-block-reason');
    const ip = ipInput.value.trim();
    if (!ip) {
        showNotification('warning', 'Enter an address or a CIDR range to block');
        ipInput.focus();
        return;
    }
    // A refused address (internal, too broad, malformed) stays in the field to
    // be corrected, next to the notification saying what was wrong with it.
    if (await blockIp(ip, reasonInput.value.trim())) {
        ipInput.value = '';
        reasonInput.value = '';
    }
}

function setSecurityIpModalAction(ip, blocked, internal) {
    document.getElementById('security-ip-modal-action').innerHTML =
        securityBlockButton(ip, blocked, internal, false);
}


function securityIpSummary(ip) {
    const paths = ip.top_paths.map(p =>
        `<span class="rerr-service-chip mono">${escapeHtml(p.key)} <span class="text-muted">&times;${formatNumber(p.count)}</span></span>`
    ).join('');
    return `
        <div class="security-ip-flags">${securityFlagBadges(ip.flags)}</div>
        <div class="security-ip-facts">
            <span><strong>${formatNumber(ip.requests)}</strong> requests</span>
            <span><strong>${formatNumber(ip.peak_per_minute)}</strong> peak/min</span>
            <span><strong>${formatNumber(ip.client_errors)}</strong> 4xx</span>
            <span><strong>${formatNumber(ip.denied)}</strong> 401/403</span>
            <span><strong>${formatNumber(ip.waf_blocks)}</strong> WAF blocks</span>
            <span><strong>${formatNumber(ip.distinct_paths)}</strong> paths</span>
            <span>first seen ${formatRelativeTime(ip.first_seen)}</span>
        </div>
        ${paths ? `<div class="security-ip-paths">${paths}</div>` : ''}`;
}

async function openSecurityIp(ip) {
    const modal = document.getElementById('security-ip-modal');
    const eventsBody = document.getElementById('security-ip-modal-events');
    const known = securityDetail && securityDetail.ips.find(i => i.ip === ip);
    const scope = securitySlice ? 'in this time slot' : 'in this window';
    document.getElementById('security-ip-modal-ip').textContent = ip;
    document.getElementById('security-ip-modal-summary').innerHTML = known ? securityIpSummary(known) : '';
    eventsBody.innerHTML = '<tr><td colspan="6" class="security-empty">Loading&hellip;</td></tr>';
    securityIpModalIp = ip;
    // Rendered from the list while the detail is on its way, so the footer is
    // never briefly empty for a client that is already blocked.
    setSecurityIpModalAction(ip, known ? known.blocked : null, known ? known.internal : false);
    modal.classList.add('open');

    // A slower answer for a previously opened IP must not overwrite this one.
    const requestId = ++securityIpRequest;
    const data = await apiGet(`/security/ips/${encodeURIComponent(ip)}?${securityRangeParams()}`);
    if (requestId !== securityIpRequest) return;
    if (data) setSecurityIpModalAction(ip, data.blocked, data.internal);
    if (!data) {
        eventsBody.innerHTML = '<tr><td colspan="6" class="security-empty">Could not load the requests of this client</td></tr>';
        return;
    }
    if (!data.events.length) {
        eventsBody.innerHTML = `<tr><td colspan="6" class="security-empty">No request from this client ${scope}</td></tr>`;
        return;
    }
    eventsBody.innerHTML = data.events.map(e => securityEventRow(e, false)).join('');
}

/** Leave the modal for the request explorer, filtered on this client. */
function showSecurityIpRequests() {
    const ip = securityIpModalIp;
    closeSecurityIpModal();
    if (ip) showSecurityRequests({ ip });
}

function closeSecurityIpModal() {
    securityIpRequest++;
    securityIpModalIp = null;
    document.getElementById('security-ip-modal').classList.remove('open');
}

document.getElementById('security-ip-modal').addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) closeSecurityIpModal();
});

document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeSecurityIpModal();
});
