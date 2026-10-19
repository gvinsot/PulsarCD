const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../frontend/static/js/views/stacks.js'), 'utf8');

function view() {
    const elements = {};
    const context = vm.createContext({
        console,
        document: { getElementById: id => elements[id] ||= {} },
        localStorage: { getItem: () => null },
        escapeHtml: String,
        uiArgs: (...args) => args.join(','),
        normalizeVersion: value => value.replace(/^v/, ''),
        repoToStackName: value => value.toLowerCase(),
        adoptStartHidden: () => {},
    });
    vm.runInContext(source, context);
    vm.runInContext(`
        stacksRepos = [{name: 'app', owner: 'owner', ssh_url: 'git@example/app.git'}];
        stacksLatestBuilt = {app: 'v1.2.3'};
        stacksAutoBuildState = {app: {untagged_commits: 2}};
        stacksPipelineState = {app: {
            stage: 'idle', status: 'idle', version: '1.2.3',
            build_action_id: 'build-id',
            transition_configs: {test_to_deploy: {mode: 'manual', qa_enabled: true}},
            stages: {
                build: {status: 'success', current_version: '1.2.3'},
                test: {status: 'idle'}, qa: {status: 'idle'}, deploy: {status: 'idle'},
            },
        }};
    `, context);
    return { context, elements };
}

test('removed stack retains build but clears test, QA and deploy status/version', () => {
    const { context, elements } = view();
    context.renderStacksList();
    const html = elements['stacks-list'].innerHTML;
    assert.match(html, /Not deployed/);
    assert.equal(elements['stacks-count'].textContent, '0 stacks deployed');
    assert.match(html, /pipeline-step step-success[^>]*title="Build/);
    for (const title of ['Test', 'QA deploy', 'Deploy \\(prod\\)']) {
        assert.match(html, new RegExp(`pipeline-step step-idle[^>]*title="${title}`));
    }
    assert.doesNotMatch(html, /Prod deployed:|QA deployed:|stack-deployed-ago|View test logs|View deploy logs/);
    assert.match(html, /Last built: 1.2.3/);
});

test('successful removal reloads state before resuming polling', async () => {
    const { context } = view();
    const calls = [];
    Object.assign(context, {
        API_BASE: '/api',
        confirm: () => true,
        authHeaders: () => ({}),
        fetch: async () => ({ok: true, json: async () => ({success: true, message: 'Removed'})}),
        showNotification: () => {},
        refreshStacks: async () => calls.push('refresh'),
        scheduleStacksStateUpdate: () => calls.push('poll'),
    });
    await context.removeDeployedStack('app');
    assert.deepEqual(calls, ['refresh', 'poll']);
});

test('failed removal leaves the current display intact', async () => {
    const { context } = view();
    const calls = [];
    Object.assign(context, {
        API_BASE: '/api',
        confirm: () => true,
        authHeaders: () => ({}),
        fetch: async () => ({ok: true, json: async () => ({success: false, message: 'Failed'})}),
        showNotification: type => calls.push(type),
        refreshStacks: async () => calls.push('refresh'),
        scheduleStacksStateUpdate: () => calls.push('poll'),
    });
    await context.removeDeployedStack('app');
    assert.deepEqual(calls, ['error']);
});
