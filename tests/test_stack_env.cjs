const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/stack-env.js'), 'utf8');

function editor(apiGetOrError) {
    const elements = new Map();
    const get = id => {
        if (!elements.has(id)) elements.set(id, {
            textContent: '', title: '', value: '', style: {}, disabled: false,
            classList: { add() {}, remove() {} }, focus() {},
        });
        return elements.get(id);
    };
    const context = vm.createContext({
        document: { getElementById: get, querySelectorAll: () => [] },
        isAdmin: () => true, apiGetOrError,
        apiPut: async () => ({ success: true }), showNotification() {},
    });
    vm.runInContext(source, context);
    return { context, get, status: () => get('stack-env-storage').textContent };
}

function response(node, exists = true) {
    return { data: { content: '', storage: { node, exists, resource: 'repo/devops/.env' } } };
}

test('storage follows each tab and its cached buffer', async () => {
    const e = editor(async url => response(url.endsWith('=qa') ? 'qa-host' : 'prod-host'));
    await e.context.editStackEnv('repo', true);
    assert.match(e.status(), /Stored on: prod-host/);
    e.get('stack-env-content').value = 'UNSAVED=1';
    await e.context.switchStackEnvTab('qa');
    assert.match(e.status(), /Stored on: qa-host/);
    await e.context.switchStackEnvTab('prod');
    assert.match(e.status(), /Stored on: prod-host/);
    assert.equal(e.get('stack-env-content').value, 'UNSAVED=1');
});

test('missing file shows the save target and becomes stored after saving', async () => {
    const e = editor(async () => response('build-host', false));
    await e.context.editStackEnv('repo', true);
    assert.match(e.status(), /File not created yet · Save target: build-host/);
    e.get('stack-env-content').value = 'UNSAVED=1';
    await e.context.switchStackEnvTab('qa');
    await e.context.saveStackEnv(); // Other tab is dirty: modal stays open.
    assert.match(e.status(), /Stored on: build-host/);
});

test('reading error clears previous storage information and disables saving', async () => {
    const e = editor(async url => url.endsWith('=qa')
        ? { error: 'Host unavailable' } : response('prod-host'));
    await e.context.editStackEnv('repo', true);
    await e.context.switchStackEnvTab('qa');
    assert.equal(e.status(), 'Storage node unavailable');
    assert.equal(e.get('stack-env-save').disabled, true);
    assert.equal(e.get('stack-env-storage').title, '');
});

test('late tab response cannot replace the active storage node', async () => {
    let resolveProd;
    const e = editor(url => url.endsWith('=qa') ? Promise.resolve(response('qa-host'))
        : new Promise(resolve => { resolveProd = resolve; }));
    const opening = e.context.editStackEnv('repo', true);
    assert.equal(e.status(), 'Loading storage node…');
    await e.context.switchStackEnvTab('qa');
    resolveProd(response('prod-host'));
    await opening;
    assert.match(e.status(), /Stored on: qa-host/);
});

test('reopening modal ignores previous repository response', async () => {
    let resolveOld;
    const e = editor(url => url.includes('/old/')
        ? new Promise(resolve => { resolveOld = resolve; }) : Promise.resolve(response('new-host')));
    const opening = e.context.editStackEnv('old');
    e.context.closeStackEnvModal();
    await e.context.editStackEnv('new');
    resolveOld(response('old-host'));
    await opening;
    assert.match(e.status(), /Stored on: new-host/);
});

test('hostnames are displayed as text and unavailable metadata is explicit', async () => {
    const hostname = '<img src=x onerror=alert(1)>';
    const e = editor(async () => response(hostname));
    await e.context.editStackEnv('repo');
    assert.ok(e.status().includes(hostname));
    assert.equal(e.get('stack-env-storage').innerHTML, undefined);
    e.context.renderStackEnvStorage({ content: '' });
    assert.equal(e.status(), 'Storage node unavailable');
});
