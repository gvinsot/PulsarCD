const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Folding the Logs query panel: state, accessible labels and persistence.
const source = fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/logs.js'), 'utf8');

function fakeElement() {
    const classes = new Set();
    return {
        title: '', textContent: '', attributes: {}, focused: false,
        classList: {
            toggle(name, on) { if (on) classes.add(name); else classes.delete(name); },
            contains(name) { return classes.has(name); },
        },
        setAttribute(name, value) { this.attributes[name] = String(value); },
        focus() { this.focused = true; },
    };
}

function load(stored = {}) {
    const store = new Map(Object.entries(stored));
    const label = fakeElement();
    const elements = {
        'ai-search-panel': fakeElement(),
        'logs-query-toggle': Object.assign(fakeElement(), { querySelector: () => label }),
        'ai-query': fakeElement(),
    };
    const context = vm.createContext({
        console,
        localStorage: {
            getItem: key => (store.has(key) ? store.get(key) : null),
            setItem: (key, value) => store.set(key, String(value)),
            removeItem: key => store.delete(key),
        },
        document: { getElementById: id => elements[id] || null },
    });
    vm.runInContext(source, context);
    return { context, elements, label, store };
}

test('the query panel starts expanded by default', () => {
    const { context, elements, label } = load();
    context.applyLogsQueryCollapsed(context.isLogsQueryCollapsed());
    assert.equal(elements['ai-search-panel'].classList.contains('collapsed'), false);
    assert.equal(elements['logs-query-toggle'].attributes['aria-expanded'], 'true');
    assert.equal(label.textContent, 'Hide query');
});

test('toggling folds the panel, then restores it and focuses the query', () => {
    const { context, elements, label, store } = load();
    context.toggleLogsQueryPanel();
    assert.equal(elements['ai-search-panel'].classList.contains('collapsed'), true);
    assert.equal(elements['logs-query-toggle'].attributes['aria-expanded'], 'false');
    assert.equal(label.textContent, 'Show query');
    assert.equal(store.get('pulsarcd_logs_query_collapsed'), '1');

    context.toggleLogsQueryPanel();
    assert.equal(elements['ai-search-panel'].classList.contains('collapsed'), false);
    assert.equal(store.get('pulsarcd_logs_query_collapsed'), '0');
    assert.equal(elements['ai-query'].focused, true);
});

test('a folded panel stays folded after a reload', () => {
    const { context, elements } = load({ pulsarcd_logs_query_collapsed: '1' });
    context.applyLogsQueryCollapsed(context.isLogsQueryCollapsed());
    assert.equal(elements['ai-search-panel'].classList.contains('collapsed'), true);
});
