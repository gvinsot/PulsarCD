const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Load app.js against a tiny DOM stub. app.js names functions from every other
// script (UI_HANDLERS), so unknown globals resolve to recording stubs instead of
// loading the whole frontend.
const appSource = fs.readFileSync(path.join(__dirname, '../frontend/static/js/app.js'), 'utf8');

function loadApp() {
    const calls = new Map();
    const elements = new Map();
    const element = () => {
        const listeners = {};
        return {
            listeners,
            addEventListener(type, fn) { (listeners[type] ||= []).push(fn); },
            dispatch(type, event) { for (const fn of listeners[type] || []) fn(event); },
        };
    };
    const document = element();
    document.getElementById = id => {
        if (!elements.has(id)) elements.set(id, element());
        return elements.get(id);
    };
    const context = vm.createContext({ document, console });
    const contextGlobal = vm.runInContext('globalThis', context);
    const stubs = new Proxy({}, {
        has: (_target, key) => typeof key === 'string' && !(key in contextGlobal),
        get: (_target, key) => {
            if (typeof key !== 'string') return undefined;
            return (...args) => {
                if (!calls.has(key)) calls.set(key, []);
                calls.get(key).push(args);
            };
        },
    });
    context.__stubs = stubs;
    vm.runInContext(`with (__stubs) {\n${appSource}\n}`, context);
    return { calls, document, getElementById: document.getElementById };
}

test('Enter in the AI search field runs exactly one AI search', () => {
    const { calls, getElementById } = loadApp();
    getElementById('ai-query').dispatch('keypress', { key: 'Enter' });
    assert.equal((calls.get('aiSearchLogs') || []).length, 1);
});

test('other keys in the AI search field do not search', () => {
    const { calls, getElementById } = loadApp();
    getElementById('ai-query').dispatch('keypress', { key: 'a' });
    assert.equal(calls.has('aiSearchLogs'), false);
});

test('Enter in the agent chat input sends exactly one message', () => {
    const { calls, getElementById } = loadApp();
    getElementById('llm-test-input').dispatch('keydown', { key: 'Enter' });
    assert.equal((calls.get('sendLLMTest') || []).length, 1);
});

test('Escape closes the modal exactly once', () => {
    const { calls, document } = loadApp();
    document.dispatch('keydown', { key: 'Escape' });
    assert.equal((calls.get('closeModal') || []).length, 1);
});
