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
