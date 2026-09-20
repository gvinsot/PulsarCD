const test = require('node:test');
const assert = require('node:assert/strict');
const model = require('../frontend/static/js/test-log-model.js');

test('pytest results retain exact anchors and deduplicate failure summaries', () => {
    const parsed = model.parse([
        '\x1b[32m================ test session starts ================\x1b[0m',
        'tests/unit/test_auth.py::test_login PASSED [ 50%]',
        'tests/unit/test_auth.py::test_expired_token FAILED [100%]',
        '________________ test_expired_token ________________',
        'E AssertionError: expected 401, got 200',
        '============= short test summary info =============',
        'FAILED tests/unit/test_auth.py::test_expired_token - AssertionError',
        '============= 1 failed, 1 passed in 0.42s =============',
    ], { lineOffset: 20 });
    assert.equal(parsed.entries.length, 2);
    assert.equal(parsed.entries[1].line, 22);
    assert.equal(parsed.entries[1].detailLine, 26);
    assert.equal(parsed.entries[1].diagnostic, 'AssertionError');
    assert.equal(parsed.entries[1].endLine, 27);
    assert.equal(parsed.entries[0].type, 'Unit');
    assert.equal(parsed.entries[0].theme, 'Authentication & security');
    assert.equal(parsed.counts.failed, 1);
    assert.deepEqual(parsed.summaries[0].counts, { failed: 1, passed: 1 });
    assert.ok(parsed.sections.some(section => section.name === 'test_expired_token' && section.line === 23));
});

test('quiet pytest and reporter totals do not invent individual test cases', () => {
    const parsed = model.parse('tests/test_api.py ..Fss [100%]\n====== 2 passed, 1 failed, 2 skipped in 1.2s ======');
    assert.equal(parsed.entries.length, 1);
    assert.equal(parsed.entries[0].kind, 'suite');
    assert.equal(parsed.entries[0].status, 'failed');
    assert.equal(parsed.counts.total, 0);
    assert.equal(parsed.counts.suites, 1);
    assert.deepEqual(parsed.summaries[0].counts, { passed: 2, failed: 1, skipped: 2 });
});

test('Jest suites and leaf tests remain distinct with durations and describe headings', () => {
    const parsed = model.parse([
        'PASS tests/unit/login.test.ts (1.2 s)',
        '  Session authentication',
        '    ✓ accepts valid credentials (5 ms)',
        '    ○ skipped expired session',
        'FAIL tests/integration/api.test.ts',
        '  ✕ rejects invalid request (8 ms)',
        'Tests:       1 failed, 1 skipped, 1 passed, 3 total',
    ]);
    assert.equal(parsed.counts.total, 3);
    assert.equal(parsed.counts.suites, 2);
    assert.equal(parsed.entries[1].duration, '5 ms');
    assert.equal(parsed.entries[1].name, 'Session authentication › accepts valid credentials');
    assert.equal(parsed.entries[2].status, 'skipped');
    assert.equal(parsed.entries[4].type, 'Integration');
    assert.equal(parsed.entries[4].file, 'tests/integration/api.test.ts');
});

test('Vitest file results and verbose tests including failure indicators', () => {
    const parsed = model.parse([
        ' RUN  v3.0.0 /workspace',
        ' ✓ tests/unit/auth.test.ts (2 tests) 6ms',
        '   ✓ authentication > logs in 2ms',
        '   ↓ authentication > refreshes tokens',
        ' ❯ tests/integration/api.test.ts (2 tests | 1 failed) 9ms',
        '   × API > handles timeout 3ms',
        ' Test Files  1 failed | 1 passed (2)',
    ]);
    assert.equal(parsed.counts.passed, 1);
    assert.equal(parsed.counts.skipped, 1);
    assert.equal(parsed.counts.failed, 1);
    assert.equal(parsed.entries[1].framework, 'Vitest');
    assert.equal(parsed.entries[1].duration, '2ms');
    assert.equal(parsed.entries[3].kind, 'suite');
    assert.equal(parsed.entries[3].status, 'failed');
    assert.equal(parsed.entries[4].file, 'tests/integration/api.test.ts');
    assert.equal(parsed.summaries.length, 1);
});

test('node --test spec results keep nested names, suites, and the failing recap', () => {
    const parsed = model.parse([
        '✔ loads the session token (1.0585ms)',
        '✖ rejects an expired token (0.8758ms)',
        '﹣ refreshes in the background (0.2188ms) # not ready',
        '✔ deletes the account (0.0614ms) # TODO',
        '▶ database migrations',
        '  ✔ applies the schema (0.0646ms)',
        '  ✖ rolls back on failure (0.062ms)',
        '✖ database migrations (0.6879ms)',
        'ℹ tests 7',
        'ℹ suites 0',
        'ℹ pass 2',
        'ℹ fail 3',
        'ℹ skipped 1',
        'ℹ todo 1',
        'ℹ duration_ms 66.8708',
        '',
        '✖ failing tests:',
        '',
        'test at tests/unit/auth.test.js:4:1',
        '✖ rejects an expired token (0.8758ms)',
        '  AssertionError [ERR_ASSERTION]: Expected values to be strictly equal:',
        '      at TestContext.<anonymous> (tests/unit/auth.test.js:4:35)',
    ]);
    assert.equal(parsed.counts.total, 6);
    assert.equal(parsed.counts.suites, 1);
    assert.deepEqual([parsed.counts.passed, parsed.counts.failed, parsed.counts.skipped], [2, 2, 2]);
    assert.equal(parsed.entries[0].framework, 'node:test');
    assert.equal(parsed.entries[0].duration, '1.0585ms');
    assert.equal(parsed.entries[5].name, 'database migrations › rolls back on failure');
    assert.equal(parsed.entries[6].kind, 'suite');
    // The recap repeats a failure: it carries its file and error, it is not a second test.
    assert.equal(parsed.entries[1].file, 'tests/unit/auth.test.js');
    assert.equal(parsed.entries[1].detailLine, 19);
    assert.ok(parsed.entries[1].diagnostic.startsWith('AssertionError'));
    assert.equal(parsed.summaries.length, 1);
    assert.deepEqual(parsed.summaries[0].counts, { passed: 2, failed: 3, skipped: 2 });
    assert.ok(parsed.sections.some(section => section.name === '✖ failing tests:' && section.line === 16));
});

test('nested TAP counts leaf results and honours SKIP / TODO directives', () => {
    const parsed = model.parse([
        'TAP version 13',
        '# Subtest: database',
        '    # Subtest: saves record',
        '    ok 1 - saves record',
        '    # Subtest: deletes record',
        '    not ok 2 - deletes record',
        '      ---',
        '      error: expected a deleted record',
        '      ...',
        'not ok 1 - database',
        'ok 2 - future feature # SKIP unavailable',
        'not ok 3 - planned feature # TODO implement later',
        '1..3',
        '# tests 4',
    ]);
    assert.equal(parsed.counts.total, 4);
    assert.equal(parsed.counts.suites, 1);
    assert.equal(parsed.counts.failed, 1);
    assert.equal(parsed.counts.skipped, 2);
    assert.equal(parsed.entries[1].endLine, 8);
    assert.equal(parsed.entries[3].name, 'future feature');
    assert.equal(parsed.entries[4].name, 'planned feature');
});

test('dotnet, Go, and container prefixes preserve source positions', () => {
    const parsed = model.parse([
        '#12 4.561   Passed Project.Unit.AuthTests.Login [23 ms]',
        'tests-1  |   Failed Project.Integration.ApiTests.Get [2 s]',
        '2026-09-20T08:01:02Z --- PASS: TestDatabaseSave (0.01s)',
        '--- SKIP: TestExternalAPI (0.00s)',
    ]);
    assert.deepEqual(parsed.entries.map(entry => entry.status), ['passed', 'failed', 'passed', 'skipped']);
    assert.deepEqual(parsed.entries.map(entry => entry.line), [0, 1, 2, 3]);
    assert.equal(parsed.entries[0].framework, '.NET');
    assert.equal(parsed.entries[2].framework, 'Go');
    assert.equal(model.group(parsed.entries, 'framework').length, 2);
});

test('unknown logs remain browsable without interpreting application text as tests', () => {
    const lines = ['[INFO] Building the test service...', ...Array(110).fill('some output'), '[ERROR] Test build failed!'];
    const parsed = model.parse(lines);
    assert.equal(parsed.entries.length, 0);
    assert.ok(parsed.sections.some(section => section.name.includes('line 101')));
    assert.equal(parsed.sections.at(-1).line, 111);
    assert.equal(parsed.sections.at(-1).endLine, 111);
    assert.equal(parsed.lineCount, 112);
    assert.equal(model.cleanLine('\x1b]0;title\x07\x1b[31merror\x1b[0m'), 'error');
});

test('grouping is deterministic and input remains unchanged', () => {
    const input = [{ line: 'tests/security/test_permissions.py::test_admin PASSED [100%]' }];
    const before = JSON.stringify(input);
    const parsed = model.parse(input);
    assert.equal(JSON.stringify(input), before);
    assert.equal(model.group(parsed.entries, 'theme')[0].label, 'Authentication & security');
    assert.equal(model.group(parsed.entries, 'type')[0].label, 'Security');
    assert.equal(model.group(parsed.entries, 'file')[0].key, 'tests/security/test_permissions.py');
});

test('empty streams and pytest parametrized identifiers with spaces', () => {
    assert.equal(model.parse('').lineCount, 0);
    assert.deepEqual(model.parse([]).sections, []);
    const parsed = model.parse([
        'tests/test_api.py::test_request[invalid body] FAILED [100%]',
        'FAILED tests/test_api.py::test_request[invalid body] - Invalid input',
    ]);
    assert.equal(parsed.counts.total, 1);
    assert.equal(parsed.entries[0].name, 'tests/test_api.py::test_request[invalid body]');
    assert.equal(parsed.entries[0].detailLine, 1);
});

test('aggregate-only outputs keep totals separate from test entries', () => {
    const parsed = model.parse([
        'Passed! - Failed: 0, Passed: 11, Skipped: 2, Total: 13, Duration: 1 s',
        '# pass 7',
        '# fail 2',
    ]);
    assert.equal(parsed.counts.total, 0);
    assert.deepEqual(parsed.summaries[0].counts, { failed: 0, passed: 11, skipped: 2 });
    assert.deepEqual(parsed.summaries[1].counts, { passed: 7 });
    assert.deepEqual(parsed.summaries[2].counts, { failed: 2 });
});

test('Go subtest results do not double count the parent test', () => {
    const parsed = model.parse([
        '--- PASS: TestDatabase/save (0.00s)',
        '--- SKIP: TestDatabase/delete (0.00s)',
        '--- PASS: TestDatabase (0.01s)',
    ]);
    assert.equal(parsed.counts.total, 2);
    assert.equal(parsed.counts.suites, 1);
    assert.equal(parsed.entries[2].kind, 'suite');
});

test('result grouping puts ERROR before SUCCESS and combines errors with failures', () => {
    const parsed = model.parse([
        'tests/test_api.py::test_ok PASSED [ 20%]',
        'tests/test_api.py::test_skipped SKIPPED [ 40%]',
        'tests/test_api.py::test_failed FAILED [ 60%]',
        'tests/test_api.py::test_setup ERROR [100%]',
    ]);
    const groups = model.group(parsed.entries, 'result');
    assert.deepEqual(groups.map(group => group.label), ['ERROR', 'SUCCESS', 'SKIPPED']);
    assert.equal(groups[0].entries.length, 2);
    assert.equal(groups[0].counts.failed, 1);
    assert.equal(groups[0].counts.error, 1);
    assert.equal(model.group([{ name: 'pending', status: 'unknown', kind: 'test' }], 'result')[0].label, 'UNKNOWN');
});
