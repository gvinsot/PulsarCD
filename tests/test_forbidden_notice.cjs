const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const read = name => fs.readFileSync(path.join(__dirname, '../frontend/static/js/core', name), 'utf8');

function page(response) {
    const notifications = [];
    const context = vm.createContext({
        API_BASE: '/api', console: { warn() {}, error() {}, log() {} },
        document: { addEventListener() {} }, localStorage: { getItem: () => 'token' },
        fetch: async () => response,
    });
    vm.runInContext(read('auth.js'), context);
    vm.runInContext(read('api.js'), context);
    // After api.js, which defines its own showNotification.
    context.showNotification = (type, message) => notifications.push({ type, message });
    context.authHeaders = () => ({});
    return { context, notifications };
}

const json = body => ({ status: 403, ok: false, json: async () => body });
const text = () => ({ status: 403, ok: false, json: async () => { throw new SyntaxError('not JSON'); } });

test("PulsarCD's own 403 is a role problem", async () => {
    const { context, notifications } = page(json({ detail: 'Admin access required' }));
    const { error } = await context.apiGetOrError('/config');
    assert.equal(error, 'This action is reserved for administrators.');
    assert.match(notifications[0].message, /reserved for administrators/);
});

test('a 403 from the edge or the WAF says so, once a minute', async () => {
    const { context, notifications } = page(text());
    const { error } = await context.apiGetOrError('/security/requests');
    assert.match(error, /Refused at the edge/);
    assert.equal(await context.apiGet('/security/overview'), null);
    assert.equal(notifications.length, 1);
    assert.equal(notifications[0].type, 'error');
    assert.match(notifications[0].message, /blocked, or the WAF/);
    assert.doesNotMatch(notifications[0].message, /reserved for administrators/);
});
