/**
 * PulsarCD frontend: Security view: client IPs, endpoints, WAF rules and the edge blocklist.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Security ==============

// Requests per client IP and per endpoint, from the Traefik access logs the
// agents copy into the access index. The detection rules (flags) are computed
// by the backend, see backend/security_analytics.py.

const SECURITY_POLL_INTERVAL = 30000;
const SECURITY_INTERVAL_LABELS = { '1m': 'minute', '5m': '5 minutes', '15m': '15 minutes', '1h': 'hour' };
let securityPollTimer = null;
let securityLoading = false;
let securityData = null;
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

/** Access-index timestamps are UTC; those read from _source carry no zone. */
function parseUtc(iso) {
    if (!iso) return null;
    return new Date(/(Z|[+-]\d\d:?\d\d)$/.test(iso) ? iso : `${iso}Z`);
}

async function loadSecurity() {
    if (securityLoading) return;
    securityLoading = true;
    try {
        const internal = document.getElementById('security-internal').checked;
        const [data, blocklist] = await Promise.all([
            apiGet(`/security/overview?minutes=${encodeURIComponent(securityWindow())}&include_internal=${internal}`),
            apiGet('/security/blocklist'),
        ]);
        if (blocklist) renderSecurityBlocklist(blocklist);
        if (!data) return;
        securityData = data;
        renderSecurity(data);
    } finally {
        securityLoading = false;
    }
}

function renderSecurity(data) {
    const errorEl = document.getElementById('security-error');
    if (data.error) {
        errorEl.textContent = `${data.error}. The access index is filled by the PulsarCD agent running on the Traefik node, from Traefik's JSON access log.`;
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
    renderSecurityIps(data.ips);
    renderSecurityEndpoints(data.endpoints);
    renderSecurityWafRules(data.waf_rules);
}

function formatSecurityBucket(iso, windowMinutes) {
    const date = parseUtc(iso);
    if (windowMinutes > 1440) {
        return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', hour12: false });
    }
    return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
}

function renderSecurityChart(timeline, windowMinutes) {
    const labels = timeline.map(p => formatSecurityBucket(p.timestamp, windowMinutes));
    const series = [
        timeline.map(p => p.ok || 0),
        timeline.map(p => p.client_errors || 0),
        timeline.map(p => p.server_errors || 0),
    ];

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

    charts.security = new Chart(document.getElementById('security-chart').getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: '2xx/3xx', data: series[0], backgroundColor: 'rgba(34, 197, 94, 0.6)' },
                { label: '4xx', data: series[1], backgroundColor: 'rgba(245, 158, 11, 0.75)' },
                { label: '5xx', data: series[2], backgroundColor: 'rgba(239, 68, 68, 0.8)' },
            ],
        },
        options,
    });
}

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

function renderSecurityIps(ips) {
    const body = document.getElementById('security-ips-body');
    if (!ips.length) {
        body.innerHTML = '<tr><td colspan="11" class="security-empty">No request in this window</td></tr>';
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

function renderSecurityEndpoints(endpoints) {
    const body = document.getElementById('security-endpoints-body');
    if (!endpoints.length) {
        body.innerHTML = '<tr><td colspan="8" class="security-empty">No request in this window</td></tr>';
        return;
    }
    body.innerHTML = endpoints.map(ep => {
        const top = ep.top_ips[0];
        const topClient = top
            ? `<span class="mono security-link" data-click="openSecurityIp" data-args="${uiArgs(top.key)}">${escapeHtml(top.key)}</span>${securityShare(top.count, ep.requests)}`
            : '&mdash;';
        const endpoint = `${ep.host}${ep.path}`;
        return `
        <tr class="${securityRowClass(ep.score)}">
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

function renderSecurityWafRules(rules) {
    const card = document.getElementById('security-waf-card');
    if (!rules || !rules.length) {
        card.style.display = 'none';
        return;
    }
    document.getElementById('security-waf-rules').innerHTML = rules.map(r =>
        `<span class="rerr-service-chip">${escapeHtml(r.key)} <span class="text-muted">&times;${formatNumber(r.count)}</span></span>`
    ).join('');
    card.style.display = '';
}

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

function securityStatusClass(status) {
    if (status >= 500) return 'security-status-error';
    if (status >= 400) return 'security-status-warning';
    return 'security-status-ok';
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
    const known = securityData && securityData.ips.find(i => i.ip === ip);
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
    const data = await apiGet(
        `/security/ips/${encodeURIComponent(ip)}?minutes=${encodeURIComponent(securityWindow())}`);
    if (requestId !== securityIpRequest) return;
    if (data) setSecurityIpModalAction(ip, data.blocked, data.internal);
    if (!data) {
        eventsBody.innerHTML = '<tr><td colspan="6" class="security-empty">Could not load the requests of this client</td></tr>';
        return;
    }
    if (!data.events.length) {
        eventsBody.innerHTML = '<tr><td colspan="6" class="security-empty">No request from this client in this window</td></tr>';
        return;
    }
    eventsBody.innerHTML = data.events.map(e => {
        const status = e.event === 'waf_block'
            ? `<span class="security-flag security-flag-high" title="${escapeHtml(e.rule_msg || '')}">WAF</span>`
            : `<span class="${securityStatusClass(e.status)}">${escapeHtml(String(e.status ?? '-'))}</span>`;
        const time = parseUtc(e.timestamp).toLocaleString('en-US', {
            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
        });
        const host = e.request_host || e.router || '';
        return `
        <tr>
            <td class="mono security-nowrap">${escapeHtml(time)}</td>
            <td>${status}</td>
            <td>${escapeHtml(e.method || '')}</td>
            <td class="security-ellipsis" title="${escapeHtml(host)}">${escapeHtml(host)}</td>
            <td class="mono security-uri" title="${escapeHtml(e.uri || '')}">${escapeHtml(e.uri || '')}</td>
            <td class="security-ellipsis" title="${escapeHtml(e.user_agent || '')}">${escapeHtml(e.user_agent || '')}</td>
        </tr>`;
    }).join('');
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
