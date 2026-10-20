const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/stack-env.js'), 'utf8');

function fakeElement() {
    const classes = new Set();
    return {
        textContent: '', title: '', value: '', style: {}, disabled: false, attributes: {},
        children: [],
        classList: {
            add(name) { classes.add(name); },
            remove(name) { classes.delete(name); },
            contains(name) { return classes.has(name); },
        },
        focus() {},
        setAttribute(name, value) { this.attributes[name] = String(value); },
        append(...nodes) { this.children.push(...nodes); },
        replaceChildren(...nodes) { this.children = nodes; },
    };
}

function editor(apiGetOrError, apiPut = async () => ({ success: true })) {
    const elements = new Map();
    const notifications = [];
    const get = id => {
        if (!elements.has(id)) elements.set(id, fakeElement());
        return elements.get(id);
    };
    const context = vm.createContext({
        document: { getElementById: get, querySelectorAll: () => [], createElement: fakeElement },
        isAdmin: () => true, apiGetOrError,
        apiPut, showNotification(type, message) { notifications.push({ type, message }); },
    });
    vm.runInContext(source, context);
    return { context, get, notifications, status: () => get('stack-env-storage').textContent };
}

function response(node, exists = true, replicas = []) {
    return { data: { content: '', storage: {
        node, exists, resource: 'repo/devops/.env', replica_targets: replicas,
    } } };
}

test('storage follows each tab and its cached buffer', async () => {
    const e = editor(async url => response(url.endsWith('=qa') ? 'qa-host' : 'prod-host'));
    await e.context.editStackEnv('repo', true);
    assert.match(e.status(), /Primary storage: prod-host/);
    e.get('stack-env-content').value = 'UNSAVED=1';
    await e.context.switchStackEnvTab('qa');
    assert.match(e.status(), /Primary storage: qa-host/);
    await e.context.switchStackEnvTab('prod');
    assert.match(e.status(), /Primary storage: prod-host/);
    assert.equal(e.get('stack-env-content').value, 'UNSAVED=1');
});

test('missing file shows the save target and becomes stored after saving', async () => {
    const e = editor(async () => response('build-host', false));
    await e.context.editStackEnv('repo', true);
    assert.match(e.status(), /File not created yet · Save target: build-host/);
    e.get('stack-env-content').value = 'UNSAVED=1';
    await e.context.switchStackEnvTab('qa');
    await e.context.saveStackEnv(); // Other tab is dirty: modal stays open.
    assert.match(e.status(), /Primary storage: build-host/);
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
    assert.match(e.status(), /Primary storage: qa-host/);
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
    assert.match(e.status(), /Primary storage: new-host/);
});

test('hostnames are displayed as text and unavailable metadata is explicit', async () => {
    const hostname = '<img src=x onerror=alert(1)>';
    const replica = '<img src=x onerror=alert(2)>';
    const e = editor(async () => response(hostname, true, [replica]));
    await e.context.editStackEnv('repo');
    assert.ok(e.status().includes(hostname));
    assert.ok(e.status().includes(replica));
    assert.equal(e.get('stack-env-storage').innerHTML, undefined);
    e.context.renderStackEnvStorage({ content: '' });
    assert.equal(e.status(), 'Storage node unavailable');
});

test('storage describes replication on save without claiming replicas are current', async () => {
    const e = editor(async () => response('server-b', true, ['server-c', 'server-d']));
    await e.context.editStackEnv('repo');
    assert.equal(e.status(),
        'Primary storage: server-b · Each save replicates to: server-c, server-d.');
});

test('storage explicitly reports when no secondary build servers are configured', async () => {
    const e = editor(async () => response('server-b'));
    await e.context.editStackEnv('repo');
    assert.equal(e.status(),
        'Primary storage: server-b · No secondary build servers configured.');
});

test('successful save displays the server replication result and closes the modal', async () => {
    const message = '.env saved on server-b and replicated to server-c.';
    const e = editor(async () => response('server-b', true, ['server-c']),
        async () => ({ success: true, message }));
    await e.context.editStackEnv('repo');
    await e.context.saveStackEnv();
    assert.deepEqual(e.notifications, [{ type: 'success', message }]);
    assert.equal(e.get('stack-env-modal').classList.contains('active'), false);
});

test('replication result keeps the other tab unsaved and the modal open', async () => {
    const message = '.env.qa saved on server-b and replicated to server-c.';
    const e = editor(async () => response('server-b', true, ['server-c']),
        async () => ({ success: true, message }));
    await e.context.editStackEnv('repo', true);
    e.get('stack-env-content').value = 'UNSAVED=1';
    await e.context.switchStackEnvTab('qa');
    await e.context.saveStackEnv();
    assert.deepEqual(e.notifications, [{ type: 'warning',
        message: `${message} .env still has unsaved changes.` }]);
    assert.equal(e.get('stack-env-modal').classList.contains('active'), true);
    await e.context.switchStackEnvTab('prod');
    assert.equal(e.get('stack-env-content').value, 'UNSAVED=1');
});

const VERSIONS = [
    { id: 'v3', created_at: '2026-10-20T10:00:00+00:00', source: 'editor', actor: 'gildas', state: 'applied' },
    { id: 'v2', created_at: '2026-10-19T10:00:00+00:00', source: 'before-edit', actor: 'gildas', state: 'observed' },
    { id: 'v1', created_at: '2026-10-18T10:00:00+00:00', source: 'scan', actor: 'system', state: 'observed' },
];

function withHistory(versions = VERSIONS) {
    const calls = [];
    const e = editor(async url => {
        calls.push(url);
        if (url.includes('/history')) return { data: { versions } };
        if (url.includes('/versions/')) return { data: { content: `RESTORED=${url.split('/versions/')[1].split('?')[0]}\n` } };
        return { data: { content: 'CURRENT=1\n', storage: { node: 'server-b', exists: true } } };
    });
    return { ...e, calls, panel: () => e.get('stack-env-history') };
}

test('history lists the backed-up versions of the file shown, as text', async () => {
    const e = withHistory();
    await e.context.editStackEnv('repo', true);
    await e.context.toggleStackEnvHistory();
    assert.equal(e.panel().style.display, '');
    assert.ok(e.calls.includes('/admin/recovery/env/repo/history?env=prod'));
    const rows = e.panel().children;
    assert.equal(rows.length, 3);
    const meta = rows.map(r => r.children[1].textContent);
    assert.equal(meta[0], 'Saved from the editor · gildas · latest');
    assert.equal(meta[1], 'Content before an edit · gildas');
    assert.equal(meta[2], 'Changed on the build host · system');
    assert.equal(rows[1].children[2].attributes['data-click'], 'loadStackEnvVersion');
    assert.equal(rows[1].children[2].attributes['data-args'], '["v2"]');

    // The panel follows the tab.
    await e.context.switchStackEnvTab('qa');
    assert.ok(e.calls.includes('/admin/recovery/env/repo/history?env=qa'));
    await e.context.toggleStackEnvHistory();
    assert.equal(e.panel().style.display, 'none');
});

test('loading a version puts it in the editor unsaved; Save restores it', async () => {
    const writes = [];
    const e = withHistory();
    e.context.apiPut = async (url, body) => { writes.push(body.content); return { success: true }; };
    await e.context.editStackEnv('repo');
    await e.context.toggleStackEnvHistory();
    await e.context.loadStackEnvVersion('v2');
    assert.equal(e.get('stack-env-content').value, 'RESTORED=v2\n');
    assert.match(e.notifications.at(-1).message, /Save to restore it as \.env/);
    await e.context.saveStackEnv();
    assert.deepEqual(writes, ['RESTORED=v2\n']);
});

test('history explains an empty or unavailable backup', async () => {
    const empty = withHistory([]);
    await empty.context.editStackEnv('repo');
    await empty.context.toggleStackEnvHistory();
    assert.match(empty.panel().children[0].textContent, /No backup of \.env yet/);

    const disabled = editor(async url => url.includes('/history')
        ? { error: 'Encrypted backup is disabled' }
        : { data: { content: '', storage: { node: 'server-b', exists: true } } });
    await disabled.context.editStackEnv('repo');
    await disabled.context.toggleStackEnvHistory();
    assert.equal(disabled.get('stack-env-history').children[0].textContent,
        'History unavailable: Encrypted backup is disabled');
});

test('partial replication failure preserves the editor and allows the same content to be retried', async () => {
    const failure = 'File saved on the primary, but replication failed on: server-c. Save again to retry.';
    const writes = [];
    const e = editor(async () => response('server-b', true, ['server-c']),
        async (url, body) => {
            writes.push({ url, content: body.content });
            // apiPut throws the HTTP 500 detail returned for partial failure.
            if (writes.length === 1) throw new Error(failure);
            return { success: true, message: '.env saved; replicated to: server-c' };
        });
    await e.context.editStackEnv('repo');
    e.get('stack-env-content').value = 'VALUE=new\n';
    await e.context.saveStackEnv();
    assert.deepEqual(e.notifications, [{ type: 'error', message: failure }]);
    assert.equal(e.get('stack-env-modal').classList.contains('active'), true);
    assert.equal(e.get('stack-env-content').value, 'VALUE=new\n');
    assert.equal(e.get('stack-env-save').disabled, false);
    await e.context.saveStackEnv();
    assert.deepEqual(writes, [
        { url: '/stacks/repo/env?env=prod', content: 'VALUE=new\n' },
        { url: '/stacks/repo/env?env=prod', content: 'VALUE=new\n' },
    ]);
    assert.equal(e.notifications.at(-1).type, 'success');
    assert.equal(e.get('stack-env-modal').classList.contains('active'), false);
});
