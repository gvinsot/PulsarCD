const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/stacks.js'), 'utf8');

function view(stored = {}) {
    const elements = {};
    const storage = { ...stored };
    const context = vm.createContext({
        console,
        document: { getElementById: id => elements[id] ||= {
            setAttribute() {}, classList: { toggle() {} },
        } },
        localStorage: {
            getItem: key => (key in storage ? storage[key] : null),
            setItem: (key, value) => { storage[key] = String(value); },
        },
        escapeHtml: String,
        uiArgs: (...args) => args.join(','),
        normalizeVersion: value => String(value).replace(/^v/, ''),
        repoToStackName: value => value.toLowerCase(),
        adoptStartHidden: () => {},
        formatTimeAgo: () => 'ago',
    });
    vm.runInContext(source, context);
    vm.runInContext(`
        stacksRepos = ['charlie', 'Alpha', 'bravo', 'delta', 'echo']
            .map(name => ({name, owner: 'o', ssh_url: 'git@x/' + name + '.git'}));
        stacksPipelineState = {
            charlie: {status: 'success', stage: 'done', deployed_version: '1.0.0',
                      last_deployed_at: '2026-11-01T10:00:00Z'},
            echo: {status: 'success', stage: 'done', deployed_version: '2.0.0',
                   last_deployed_at: '2026-11-09T10:00:00Z'},
        };
        stacksDeployedTags = {delta: '0.1.0'};  // deployed, no known date
    `, context);
    return { context, elements, storage };
}

const order = (context, mode) =>
    Array.from(context.sortStacksRepos(vm.runInContext('stacksRepos', context), mode), r => r.name);

test('default mode keeps the starred order', () => {
    const { context } = view();
    assert.deepEqual(order(context, 'default'), ['charlie', 'Alpha', 'bravo', 'delta', 'echo']);
});

test('name mode sorts case-insensitively', () => {
    const { context } = view();
    assert.deepEqual(order(context, 'name'), ['Alpha', 'bravo', 'charlie', 'delta', 'echo']);
});

test('last_deployed mode: newest first, then undated deployed, then undeployed', () => {
    const { context } = view();
    assert.deepEqual(order(context, 'last_deployed'), ['echo', 'charlie', 'delta', 'Alpha', 'bravo']);
});

test('changing the select persists the mode and re-renders in that order', () => {
    const { context, elements, storage } = view();
    context.changeStacksSort.call({ value: 'name' });
    assert.equal(storage.pulsarcd_stacks_sort, 'name');
    assert.equal(elements['stacks-sort'].value, 'name');
    const html = elements['stacks-list'].innerHTML;
    // Each stack header carries one Activity button whose args name the repo.
    const positions = ['Alpha', 'bravo', 'charlie', 'delta', 'echo']
        .map(name => html.indexOf(`data-click="showStackActivity" data-args="o,${name}"`));
    assert.ok(positions.every(p => p >= 0), `missing stack: ${positions}`);
    assert.deepEqual([...positions].sort((a, b) => a - b), positions);
});

test('an unknown stored mode falls back to the starred order', () => {
    const { context } = view({ pulsarcd_stacks_sort: 'bogus' });
    assert.equal(context.getStacksSort(), 'default');
    context.changeStacksSort.call({ value: 'bogus' });
    assert.equal(context.getStacksSort(), 'default');
});

test('sorting applies to the stacks left visible by "Hide undeployed"', () => {
    const { context, elements } = view({
        pulsarcd_stacks_sort: 'last_deployed', pulsarcd_stacks_hide_undeployed: '1',
    });
    vm.runInContext('stacksQaDeployedTags = {}; stacksContainers = {};', context);
    context.renderStacksList();
    const html = elements['stacks-list'].innerHTML;
    const positions = ['echo', 'charlie', 'delta']
        .map(name => html.indexOf(`data-click="showStackActivity" data-args="o,${name}"`));
    assert.ok(positions.every(p => p >= 0), `missing stack: ${positions}`);
    assert.deepEqual([...positions].sort((a, b) => a - b), positions);
    for (const hidden of ['Alpha', 'bravo']) {
        assert.ok(!html.includes(`data-args="o,${hidden}"`), `${hidden} should be hidden`);
    }
});
