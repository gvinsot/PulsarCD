const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function view() {
    const elements = {};
    const element = () => {
        const classes = new Set();
        return {
            innerHTML: '', textContent: '', dataset: {}, value: '',
            classList: { add: c => classes.add(c), remove: c => classes.delete(c), contains: c => classes.has(c) },
            setAttribute() {}, focus() {}, querySelector: () => ({ focus() {} }),
            appendChild(child) { this.innerHTML += child.innerHTML; },
        };
    };
    const context = vm.createContext({
        document: {getElementById: id => elements[id] ||= element(), createElement: element},
        localStorage: {getItem: () => null}, console,
        isAdmin: () => true, uiArgs: (...args) => JSON.stringify(args).replaceAll('"', '&quot;'),
        formatMemory: mb => `${mb} MB`,
        apiGetOrError: async () => ({data: {hostname: 'worker'}}),
    });
    vm.runInContext(fs.readFileSync(path.join(__dirname, '../frontend/static/js/core/utils.js'), 'utf8'), context);
    vm.runInContext(fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/containers.js'), 'utf8'), context);
    return {context, elements};
}

test('inventory displays hardware with escaped text and unavailable values', () => {
    const {context} = view();
    const html = context.renderHostInfo({hostname: '<img src=x>', cpu_cores: 4, cpu_threads: 8,
        memory_total_mb: 16384, ip_addresses: ['2001:db8::1'], gpus: ['<script>alert(1)</script>']});
    assert.match(html, /&lt;img/);
    assert.match(html, /&lt;script/);
    assert.doesNotMatch(html, /<script|<img/);
    assert.match(html, /Physical cores<\/dt><dd>4/);
    assert.match(html, /Logical CPUs \/ threads<\/dt><dd>8/);
    assert.match(html, /2001:db8::1/);
    assert.match(html, /Unavailable/);
});

test('opening loads information and renders errors as text', async () => {
    const {context, elements} = view();
    context.apiGetOrError = async endpoint => {
        assert.equal(endpoint, '/hosts/host%20%26%201/info');
        assert.equal(elements['host-info-body'].textContent, 'Loading host information…');
        return {error: '<b>Host unavailable</b>'};
    };
    await context.openHostInfo('host & 1');
    assert.equal(elements['host-info-body'].textContent, '<b>Host unavailable</b>');
    assert.equal(elements['host-info-body'].innerHTML, '');
});

test('late response cannot replace another host or update a closed modal', async () => {
    const {context, elements} = view();
    let resolveOld;
    context.apiGetOrError = () => new Promise(resolve => {resolveOld = resolve;});
    const old = context.openHostInfo('old');
    context.apiGetOrError = async () => ({data: {hostname: 'new'}});
    await context.openHostInfo('new');
    resolveOld({data: {hostname: 'old'}});
    await old;
    assert.match(elements['host-info-body'].innerHTML, /<dd>new<\/dd>/);
    const trigger = {isConnected: true, focus() {this.focused = true;}};
    context.closeHostInfoModal();
    context.document.activeElement = trigger;
    context.apiGetOrError = () => new Promise(resolve => {resolveOld = resolve;});
    const pending = context.openHostInfo('closed');
    context.closeHostInfoModal();
    resolveOld({data: {hostname: 'closed'}});
    await pending;
    assert.equal(elements['host-info-modal'].classList.contains('open'), false);
    assert.equal(trigger.focused, true);
    assert.doesNotMatch(elements['host-info-body'].innerHTML, /<dd>closed<\/dd>/);
});

test('empty computers still have Info buttons; viewers do not get an inaccessible action', async () => {
    const {context, elements} = view();
    context.apiGet = async endpoint => endpoint === '/hosts' ? [{name: 'empty'}] : {};
    await context.loadContainers();
    assert.match(elements['containers-list'].innerHTML, /data-click="openHostInfo"/);
    assert.match(elements['containers-list'].innerHTML, /0 containers/);
    context.isAdmin = () => false;
    await context.loadContainers();
    assert.doesNotMatch(elements['containers-list'].innerHTML, /data-click="openHostInfo"/);
});
