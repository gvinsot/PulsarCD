const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/security.js'), 'utf8');

function fakeElement(id) {
    const classes = new Set();
    return {
        id, textContent: '', title: '', value: '', innerHTML: '', style: {}, checked: false,
        dataset: {}, attributes: {}, options: [{}],
        classList: {
            add(name) { classes.add(name); },
            remove(name) { classes.delete(name); },
            toggle(name, on) { if (on) classes.add(name); else classes.delete(name); },
            contains(name) { return classes.has(name); },
        },
        add(option) { this.options.push(option); },
        addEventListener() {},
        setAttribute(name, value) { this.attributes[name] = String(value); },
    };
}

const TIMELINE = ['10:00', '10:15', '10:30'].map(t => ({
    timestamp: `2026-10-23T${t}:00.000Z`, ok: 1, client_errors: 0, server_errors: 0,
}));

function overview(extra = {}) {
    return {
        window_minutes: 1440, timeline_interval: '15m', timeline: TIMELINE,
        totals: { requests: 3, flagged_ips: 0, flagged_endpoints: 0, attack_requests: 0, attack_answered: 0 },
        ips: [], endpoints: [], attacks: [], waf_rules: [], attack_categories: [],
        ...extra,
    };
}

function view({ requests = { events: [], limit: 500 } } = {}) {
    const elements = new Map();
    const get = id => {
        if (!elements.has(id)) elements.set(id, fakeElement(id));
        return elements.get(id);
    };
    get('security-window').value = '1440';
    const tabs = ['ips', 'endpoints', 'attacks', 'requests', 'edge'].map(tab => {
        const btn = fakeElement(`tab-${tab}`);
        btn.dataset.tab = tab;
        return btn;
    });
    const calls = [];
    const chart = { data: { labels: [], datasets: [{}, {}, {}] }, update() {} };
    const context = vm.createContext({
        document: {
            getElementById: get, querySelector: get, querySelectorAll: () => tabs,
            addEventListener() {}, hidden: false,
        },
        localStorage: { getItem: () => null, setItem() {} },
        URLSearchParams, Option: function Option(label, value) { this.label = label; this.value = value; },
        charts: { security: chart }, currentView: 'security',
        apiGet: async url => { calls.push(url); return url.startsWith('/security/blocklist') ? { entries: [] } : overview(); },
        apiGetOrError: async url => { calls.push(url); return { data: requests }; },
        escapeHtml: s => String(s), formatNumber: n => String(n), formatRelativeTime: () => '',
        uiArgs: (...args) => JSON.stringify(args), isAdmin: () => true,
    });
    vm.runInContext(source, context);
    return { context, get, calls, tabs };
}

test('a bar is a time slot; clicking it again clears it', () => {
    const { context } = view();
    const slot = context.securitySliceFor(TIMELINE, '15m', 1, null);
    assert.deepEqual({ ...slot }, { start: '2026-10-23T10:15:00.000Z', end: '2026-10-23T10:30:00.000Z' });
    assert.equal(context.securitySliceFor(TIMELINE, '15m', 1, slot), null);
});

test('clicking a bar focuses the tabs on its slot', async () => {
    const e = view();
    await e.context.loadSecurity();
    e.context.switchSecurityTab('edge');
    e.calls.length = 0;
    e.context.focusSecurityBar(1);
    await new Promise(resolve => setImmediate(resolve));

    // The blocklist does not depend on time: the endpoints are shown instead.
    assert.equal(e.get('security-panel-endpoints').style.display, '');
    assert.equal(e.get('security-panel-edge').style.display, 'none');
    assert.equal(e.get('security-focus').style.display, '');
    assert.match(e.get('security-focus-label').textContent, /Oct 23/);
    const slotCall = e.calls.find(url => url.includes('start='));
    assert.ok(slotCall, e.calls.join('\n'));
    assert.match(slotCall, /start=2026-10-23T10%3A15%3A00.000Z&end=2026-10-23T10%3A30%3A00.000Z/);
    // The cards and the chart keep the whole window.
    assert.ok(e.calls.some(url => url.startsWith('/security/overview?minutes=1440&include_internal')));

    e.context.clearSecuritySlice();
    assert.equal(e.get('security-focus').style.display, 'none');
});

test('an endpoint opens the request explorer on it, within the focused slot', async () => {
    const e = view({ requests: { events: [{ timestamp: '2026-10-23T10:20:00Z', event: 'request',
        client_ip: '203.0.113.9', status: 403, router: 'pulsarcd-blocklist@http', uri: '/.env' }], limit: 500 } });
    await e.context.loadSecurity();
    e.context.focusSecurityBar(0);
    await new Promise(resolve => setImmediate(resolve));
    e.calls.length = 0;

    e.context.showSecurityRequests({ host: 'a.io', path: '/.env' });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(e.get('security-panel-requests').style.display, '');
    const url = e.calls.find(u => u.startsWith('/security/requests?'));
    const params = new URLSearchParams(url.split('?')[1]);
    assert.equal(params.get('host'), 'a.io');
    assert.equal(params.get('path'), '/.env');
    assert.equal(params.get('start'), '2026-10-23T10:00:00.000Z');
    assert.equal(e.get('security-filter-host').value, 'a.io');
    // The path came from a click: a removable chip, not a form field.
    assert.match(e.get('security-request-chips').innerHTML, /removeSecurityRequestFilter/);
    const rows = e.get('security-requests-body').innerHTML;
    assert.match(rows, /203\.0\.113\.9/);
    assert.match(rows, /Edge/);

    e.context.removeSecurityRequestFilter('path');
    await new Promise(resolve => setImmediate(resolve));
    const last = new URLSearchParams(e.calls.at(-1).split('?')[1]);
    assert.equal(last.get('path'), null);
    assert.equal(last.get('host'), 'a.io');
});
