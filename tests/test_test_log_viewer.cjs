const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const TestLogModel = require('../frontend/static/js/test-log-model.js');

// Test state transitions and rendered evidence without adding a browser/DOM dependency.
const viewerSource = fs.readFileSync(path.join(__dirname, '../frontend/static/js/test-log-viewer.js'), 'utf8');
const escapeHtml = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));

function element() {
    return {
        innerHTML: '', textContent: '', hidden: false, disabled: false, value: '',
        scrollTop: 0, scrollHeight: 200, clientHeight: 100,
        querySelectorAll: () => [],
        insertAdjacentHTML(_position, html) { this.innerHTML += html; },
    };
}

function fixture(state = {}) {
    const timers = new Map();
    let nextTimer = 0;
    const context = vm.createContext({
        TestLogModel, escapeHtml, AbortController,
        setTimeout(callback, delay) { const id = ++nextTimer; timers.set(id, { callback, delay }); return id; },
        clearTimeout(id) { timers.delete(id); },
        document: { getElementById: () => ({ classList: { remove() {} } }) },
    });
    vm.runInContext(viewerSource + '\nthis.TestLogsViewer = TestLogsViewer;', context);
    const viewer = Object.create(context.TestLogsViewer.prototype);
    const slots = new Map(), controls = new Map();
    const control = selector => {
        if (!controls.has(selector)) controls.set(selector, element());
        return controls.get(selector);
    };
    Object.assign(viewer, {
        actionId: 'test-action', repo: 'example', lines: [], offset: 0, view: 'results',
        groupBy: 'result', query: '', status: '', follow: true, rawStart: 0,
        selectedLine: -1, closed: false, revision: 0, abort: new AbortController(),
        model: TestLogModel.parse([]), rawContent: { hidden: true },
        root: { querySelector: control, querySelectorAll: () => [], remove() { this.removed = true; } },
        slot(name) { if (!slots.has(name)) slots.set(name, element()); return slots.get(name); },
        ...state,
    });
    return { viewer, timers, control };
}

function deferred() {
    let resolve, reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

function response(data) { return { json: async () => data }; }

function priorReport() {
    return { markdown: '# Previous report\nPrevious risk', findings: [{ title: 'Previous risk', severity: 'high' }], report: {} };
}

test('SwiftProof shows the recorded tool version, never an assumed installation version', () => {
    const { viewer } = fixture({ proof: { status: 'passed', tool_version: 'v0.2.0' }, proofEnabled: true });
    viewer.renderProof();
    assert.ok(viewer.slot('proof').innerHTML.includes('SwiftProof v0.2.0'));
    const { viewer: legacy } = fixture({ proof: { status: 'passed' }, proofEnabled: true });
    legacy.renderProof();
    assert.ok(!legacy.slot('proof').innerHTML.includes('v0.2.0'));
});

for (const blocking of [undefined, true, false]) {
    for (const status of ['passed', 'blocked', 'needs_review', 'error']) {
        test(`SwiftProof report remains visible for ${status} with blocking=${blocking}`, async () => {
            const { viewer, control } = fixture({ view: 'report' });
            const report = { markdown: '# Current review\nReview evidence', findings: [{ title: 'Current finding', severity: 'high' }], report: {} };
            const requestedPaths = [];
            viewer.request = async path => {
                requestedPaths.push(path);
                if (path === '/stacks/pipeline/example/transition/build_to_test') {
                    return response({ config: { swiftproof_enabled: true, swiftproof_blocking: blocking }, swiftproof: { id: 'current-review', status } });
                }
                if (path === '/stacks/pipeline/example/swiftproof/current-review/report?format=json') return response(report);
                throw new Error(`Unexpected request: ${path}`);
            };
            await viewer.pollProof();
            assert.equal(requestedPaths.length, 2);
            assert.equal(viewer.proofBlocking, blocking !== false);
            assert.equal(control('.test-proof').hidden, false);
            assert.equal(control('[data-view="report"]').disabled, false);
            assert.equal(viewer.report, report);
            assert.ok(viewer.slot('proof').innerHTML.includes(blocking === false ? '>Non-blocking<' : '>Blocking<'));
            assert.ok(viewer.slot('proof').innerHTML.includes('Current finding'));
            assert.ok(viewer.slot('proof').innerHTML.includes('Open full report'));
            assert.ok(viewer.slot('proof').innerHTML.includes('Download evidence'));
            assert.ok(viewer.slot('report').innerHTML.includes('Review evidence'));
        });
    }
}

test('changing SwiftProof blocking mode updates the label without discarding the report', async () => {
    const { viewer, control } = fixture({ proofEnabled: true, proofBlocking: true,
        proof: { id: 'current-review', status: 'needs_review' }, reportId: 'current-review', report: priorReport() });
    viewer.renderProof();
    assert.ok(viewer.slot('proof').innerHTML.includes('>Blocking<'));
    viewer.request = async () => response({ config: { swiftproof_enabled: true, swiftproof_blocking: false }, swiftproof: viewer.proof });
    await viewer.pollProof();
    assert.ok(viewer.slot('proof').innerHTML.includes('>Non-blocking<'));
    assert.ok(viewer.slot('proof').innerHTML.includes('does not change the automated test result'));
    assert.ok(viewer.slot('proof').innerHTML.includes('Previous risk'));
    assert.equal(control('[data-view="report"]').disabled, false);
});

test('finding source uses its old/new side when another renamed file shares the path', () => {
    const { viewer } = fixture({ selectedFinding: 0 });
    viewer.report = {
        findings: [{ title: 'Removed authorization', severity: 'high', side: 'old', path: 'auth.js', line: 12 }],
        report: { change: { files: [
            { path: 'auth.js', old_path: 'legacy.js', hunks: [{ lines: [{ old_line: 12, new_line: 15, kind: '+', content: 'wrong file content' }] }] },
            { path: 'renamed-auth.js', old_path: 'auth.js', hunks: [{ lines: [{ old_line: 12, new_line: null, kind: '-', content: 'requirePermission("<admin>")' }] }] },
        ] } },
        markdown: '# Evidence\n<script>reportMarkup()</script>',
    };
    viewer.renderReport();
    const html = viewer.slot('report').innerHTML;
    assert.ok(html.includes('requirePermission(&quot;&lt;admin&gt;&quot;)'));
    assert.ok(!html.includes('wrong file content'));
    assert.ok(html.includes('test-log-selected'));
    assert.ok(html.includes('Source changes · old side'));
    assert.ok(html.includes('&lt;script&gt;reportMarkup()&lt;/script&gt;'));
    assert.ok(!html.includes('<script>'));

    viewer.report.findings[0] = { title: 'Candidate change', severity: 'medium', side: 'new', path: 'auth.js', line: 15 };
    viewer.renderReport();
    assert.ok(viewer.slot('report').innerHTML.includes('wrong file content'));
    assert.ok(!viewer.slot('report').innerHTML.includes('requirePermission'));
});

test('linked baseline/candidate check outputs are rendered and escaped without evidence.output', () => {
    const { viewer } = fixture({ selectedFinding: 0 });
    viewer.report = {
        findings: [{ title: '<img src=x>', severity: 'critical', path: 'auth.js', line: 9,
            evidence: [{ description: 'Differential check', base_check_id: 'base', check_id: 'candidate', test_names: ['denies <admin>'] }] }],
        report: { checks: [
            { id: 'base', status: 'PASS', command: ['node', 'test', '--name=<admin>'], output: 'baseline passed\n<safe>', duration_ms: 18 },
            { id: 'candidate', status: 'FAIL', command: ['node', 'test'], output: 'candidate failed\n<script>bad()</script>', duration_ms: 22, truncated: true },
            { id: 'unrelated', status: 'FAIL', output: 'UNRELATED_OUTPUT' },
        ] }, markdown: '# Results',
    };
    viewer.renderReport();
    const html = viewer.slot('report').innerHTML;
    for (const expected of ['Baseline check', 'Candidate check', 'baseline passed', 'candidate failed',
        'node test --name=&lt;admin&gt;', '18 ms', '22 ms', '[Output truncated by SwiftProof]',
        'denies &lt;admin&gt;', '&lt;script&gt;bad()&lt;/script&gt;', '&lt;img src=x&gt;']) assert.ok(html.includes(expected), expected);
    assert.ok(!html.includes('UNRELATED_OUTPUT'));
    assert.ok(!html.includes('<script>'));
    assert.ok(!html.includes('<img src=x>'));
});

test('ingestion retains only 20k lines and keeps raw timestamps/container prefixes', () => {
    const { viewer, timers } = fixture();
    const lines = Array.from({ length: 20005 }, (_, index) => `2026-09-20T10:00:00Z tests-1 | output ${index}`);
    lines[20004] = '\x1b[31m2026-09-20T10:00:00Z tests-1 | tests/test_api.py::test_read FAILED [100%]\x1b[0m';
    viewer.ingest(lines);
    assert.equal(viewer.lines.length, 20000);
    assert.equal(viewer.offset, 5);
    assert.equal(viewer.lines[0], lines[5]);
    assert.equal(viewer.lines.at(-1), '2026-09-20T10:00:00Z tests-1 | tests/test_api.py::test_read FAILED [100%]');
    viewer.ingest(['--- COMPLETED ---'], 'log-success');
    assert.equal(viewer.lines.length, 20000);
    assert.equal(viewer.offset, 6);
    assert.equal(viewer.done, true);
    assert.equal(timers.size, 1, 'a burst schedules only one render');
    viewer.refresh();
    assert.equal(viewer.model.entries[0].line, 20004);
    assert.ok(viewer.slot('live').textContent.includes('20006 lines'));
    viewer.rawStart = 20004;
    viewer.renderRaw();
    assert.ok(viewer.slot('raw').innerHTML.includes('2026-09-20T10:00:00Z tests-1 |'));
    assert.equal(viewer.slot('range').textContent, 'Lines 20005–20006 of 20006');
});

test('late LLM success cannot replace groups after a log stream reset', async () => {
    const { viewer } = fixture({ model: TestLogModel.parse(['tests/test_api.py::test_read PASSED']) });
    const pending = deferred();
    viewer.request = () => pending.promise;
    const operation = viewer.groupWithLLM();
    viewer.reset();
    pending.resolve(response({ groups: [{ label: 'Old stream', entry_ids: ['test-0'] }] }));
    await operation;
    assert.equal(viewer.llmGroups, null);
    assert.equal(viewer.groupBy, 'result');
});

test('late LLM error cannot overwrite the status of a reset stream', async () => {
    const { viewer } = fixture({ model: TestLogModel.parse(['tests/test_api.py::test_read PASSED']) });
    const pending = deferred();
    viewer.request = () => pending.promise;
    const operation = viewer.groupWithLLM();
    viewer.reset();
    viewer.slot('themes').textContent = 'Current stream status';
    pending.reject(new Error('Old request failed'));
    await operation;
    assert.equal(viewer.slot('themes').textContent, 'Current stream status');
});

test('an old LLM request settling cannot re-enable a newer request button', async () => {
    const { viewer, control } = fixture({ model: TestLogModel.parse(['tests/test_api.py::test_old PASSED']) });
    const oldRequest = deferred(), newRequest = deferred();
    viewer.request = () => oldRequest.promise;
    const oldOperation = viewer.groupWithLLM();
    viewer.reset();
    assert.equal(control('[data-action="themes"]').disabled, false);
    viewer.model = TestLogModel.parse(['tests/test_api.py::test_current PASSED']);
    viewer.request = () => newRequest.promise;
    const newOperation = viewer.groupWithLLM();
    oldRequest.resolve(response({ groups: [{ label: 'Old', entry_ids: ['test-0'] }] }));
    await oldOperation;
    assert.equal(control('[data-action="themes"]').disabled, true);
    assert.equal(viewer.groupBy, 'result');
    newRequest.resolve(response({ groups: [{ label: 'Current', entry_ids: ['test-0'] }] }));
    await newOperation;
    assert.equal(control('[data-action="themes"]').disabled, false);
    assert.equal(viewer.groupBy, 'llm');
    assert.equal(viewer.llmGroups[0].label, 'Current');
});

test('closing aborts requests and ignores a late LLM response', async () => {
    const { viewer, timers } = fixture({ model: TestLogModel.parse(['tests/test_api.py::test_read PASSED']) });
    const pending = deferred();
    viewer.request = () => pending.promise;
    const operation = viewer.groupWithLLM();
    viewer.ingest(['pending output']);
    viewer.dispose();
    const note = viewer.slot('themes').textContent;
    pending.resolve(response({ groups: [{ label: 'Stale', entry_ids: ['test-0'] }] }));
    await operation;
    assert.equal(viewer.abort.signal.aborted, true);
    assert.equal(viewer.root.removed, true);
    assert.equal(viewer.rawContent.hidden, false);
    assert.equal(viewer.llmGroups, undefined);
    assert.equal(viewer.slot('themes').textContent, note);
    assert.equal(timers.size, 0);
});

test('a new running SwiftProof review clears old findings and the selected report', async () => {
    const { viewer, timers, control } = fixture({ view: 'report', proofEnabled: true,
        proof: { id: 'old-review', status: 'blocked' }, reportId: 'old-review', report: priorReport(), selectedFinding: 0 });
    viewer.renderProof();
    assert.ok(viewer.slot('proof').innerHTML.includes('Previous risk'));
    viewer.request = async () => response({ config: { swiftproof_enabled: true }, swiftproof: { id: 'new-review', status: 'running' } });
    await viewer.pollProof();
    assert.equal(viewer.report, null);
    assert.equal(viewer.reportId, null);
    assert.equal(viewer.selectedFinding, undefined);
    assert.equal(control('[data-view="report"]').disabled, true);
    assert.ok(!viewer.slot('proof').innerHTML.includes('Previous risk'));
    assert.ok(viewer.slot('report').innerHTML.includes('Waiting for a SwiftProof report'));
    assert.equal([...timers.values()][0].delay, 4000);
});

test('a failed report fetch for a new review cannot revive previous evidence', async () => {
    const { viewer, timers } = fixture({ view: 'report', proofEnabled: true,
        proof: { id: 'old-review', status: 'blocked' }, reportId: 'old-review', report: priorReport(), selectedFinding: 0 });
    viewer.renderProof();
    let calls = 0;
    viewer.request = async () => {
        if (++calls === 1) return response({ config: { swiftproof_enabled: true }, swiftproof: { id: 'new-review', status: 'error', reason: 'Review failed' } });
        throw new Error('HTTP 404');
    };
    await viewer.pollProof();
    assert.equal(viewer.report, null);
    assert.equal(viewer.reportId, null);
    assert.equal(viewer.selectedFinding, undefined);
    assert.ok(viewer.slot('proof').innerHTML.includes('Review failed'));
    assert.ok(viewer.slot('proof').innerHTML.includes('Retrying automatically'));
    assert.ok(!viewer.slot('report').innerHTML.includes('Previous risk'));
    assert.equal(timers.size, 1);
});

test('a late SwiftProof report after dispose is ignored and does not restart polling', async () => {
    const { viewer, timers } = fixture();
    const pending = deferred();
    let calls = 0;
    viewer.request = async () => {
        if (++calls === 1) return response({ config: { swiftproof_enabled: true }, swiftproof: { id: 'new-review', status: 'passed' } });
        return pending.promise;
    };
    const operation = viewer.pollProof();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(calls, 2);
    viewer.dispose();
    pending.resolve(response(priorReport()));
    await operation;
    assert.equal(viewer.report, null);
    assert.equal(viewer.reportId, null);
    assert.equal(timers.size, 0);
});
