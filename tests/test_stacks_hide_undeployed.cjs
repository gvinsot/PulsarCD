const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// "Hide undeployed" filter of the Stacks view: filtering, button state and persistence.
const source = fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/stacks.js'), 'utf8');

function fakeElement() {
    const classes = new Set();
    return {
        innerHTML: '', textContent: '', title: '', attributes: {},
        classList: {
            toggle(name, on) { if (on) classes.add(name); else classes.delete(name); },
            contains(name) { return classes.has(name); },
        },
        setAttribute(name, value) { this.attributes[name] = String(value); },
    };
}

function view(stored = {}) {
    const store = new Map(Object.entries(stored));
    const elements = {};
    const context = vm.createContext({
        console,
        document: { getElementById: id => elements[id] ||= fakeElement() },
        localStorage: {
            getItem: key => (store.has(key) ? store.get(key) : null),
            setItem: (key, value) => store.set(key, String(value)),
        },
        escapeHtml: String,
        uiArgs: (...args) => args.join(','),
        normalizeVersion: value => value.replace(/^v/, ''),
        repoToStackName: value => value.toLowerCase(),
        adoptStartHidden: () => {},
        formatTimeAgo: () => 'now',
    });
    vm.runInContext(source, context);
    vm.runInContext(`
        stacksRepos = [
            {name: 'prod-app', owner: 'o', ssh_url: 'git@example/prod-app.git'},
            {name: 'qa-only', owner: 'o', ssh_url: 'git@example/qa-only.git'},
            {name: 'idle-app', owner: 'o', ssh_url: 'git@example/idle-app.git'},
        ];
        stacksDeployedTags = {'prod-app': 'v1.0.0'};
        stacksQaDeployedTags = {'qa-only': 'v0.2.0'};
    `, context);
    return { context, elements, store };
}

const repoNames = html => [...html.matchAll(/data-repo="([^"]+)"/g)].map(m => m[1]);

test('every stack is listed by default', () => {
    const { context, elements } = view();
    context.renderStacksList();
    const names = new Set(repoNames(elements['stacks-list'].innerHTML));
    assert.deepEqual([...names].sort(), ['idle-app', 'prod-app', 'qa-only']);
    assert.equal(elements['stacks-hide-undeployed'].attributes['aria-pressed'], 'false');
    assert.equal(elements['stacks-hide-undeployed'].textContent, 'Hide undeployed');
});

test('toggling hides stacks deployed neither in prod nor QA and persists the choice', () => {
    const { context, elements, store } = view();
    context.toggleHideUndeployedStacks();
    const names = new Set(repoNames(elements['stacks-list'].innerHTML));
    assert.deepEqual([...names].sort(), ['prod-app', 'qa-only']);
    assert.equal(store.get('pulsarcd_stacks_hide_undeployed'), '1');
    assert.equal(elements['stacks-hide-undeployed'].attributes['aria-pressed'], 'true');
    assert.equal(elements['stacks-hide-undeployed'].textContent, 'Show undeployed');

    context.toggleHideUndeployedStacks();
    assert.ok(repoNames(elements['stacks-list'].innerHTML).includes('idle-app'));
    assert.equal(store.get('pulsarcd_stacks_hide_undeployed'), '0');
});

test('the saved filter is reapplied on the next load', () => {
    const { context, elements } = view({ pulsarcd_stacks_hide_undeployed: '1' });
    context.renderStacksList();
    assert.ok(!repoNames(elements['stacks-list'].innerHTML).includes('idle-app'));
    assert.equal(elements['stacks-hide-undeployed'].attributes['aria-pressed'], 'true');
});

test('a stack with running containers but no tag counts as deployed', () => {
    const { context } = view();
    vm.runInContext(`stacksContainers = {'idle-app': {svc: [{}]}};`, context);
    assert.equal(context.isRepoDeployed({name: 'idle-app'}), true);
    assert.equal(context.isRepoDeployed({name: 'missing'}), false);
});

test('an empty filtered list explains how to show every stack', () => {
    const { context, elements } = view({ pulsarcd_stacks_hide_undeployed: '1' });
    vm.runInContext(`stacksDeployedTags = {}; stacksQaDeployedTags = {};`, context);
    context.renderStacksList();
    assert.match(elements['stacks-list'].innerHTML, /No deployed stacks/);
});
