/**
 * PulsarCD frontend: Stack output modal and the stack env editor.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Stack Output ==============

function showStackOutput(action, repoName, result) {
    const modal = document.getElementById('stack-output-modal');
    const title = document.getElementById('stack-output-title');
    const status = document.getElementById('stack-output-status');
    const content = document.getElementById('stack-output-content');
    
    title.textContent = `${action}: ${repoName}`;
    
    if (result.success) {
        status.innerHTML = `
            <span class="status-success">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20">
                    <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
                    <polyline points="22 4 12 14.01 9 11.01"/>
                </svg>
                Success
            </span>
            ${result.duration_seconds ? `<span class="status-duration">${result.duration_seconds.toFixed(1)}s</span>` : ''}
            ${result.host ? `<span class="status-host">on ${result.host}</span>` : ''}
        `;
    } else {
        status.innerHTML = `
            <span class="status-error">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20">
                    <circle cx="12" cy="12" r="10"/>
                    <line x1="15" y1="9" x2="9" y2="15"/>
                    <line x1="9" y1="9" x2="15" y2="15"/>
                </svg>
                Failed
            </span>
            ${result.duration_seconds ? `<span class="status-duration">${result.duration_seconds.toFixed(1)}s</span>` : ''}
        `;
    }
    
    content.textContent = result.output || 'No output';
    modal.classList.add('active');
}

function closeStackOutputModal() {
    document.getElementById('stack-output-modal').classList.remove('active');
}

// ============== Stack Env Editor ==============

// The modal edits devops/.env and, when the stack's QA stage is enabled,
// devops/.env.qa in a second tab: the variables it sets override .env for the
// QA deployment only. Each tab keeps its own buffer, so switching tabs does not
// lose unsaved edits.
const STACK_ENV_FILES = {
    prod: {
        name: '.env',
        help: 'Edit the environment variables for this stack. Each line should be in KEY=VALUE format.',
    },
    qa: {
        name: '.env.qa',
        help: 'Overrides for the QA deployment only. Each KEY=VALUE here replaces the .env value '
            + 'in the QA stack, verbatim: no automatic "qa." domain prefix. KEY= with no value '
            + 'blanks it. Variables not listed keep their .env value.',
    },
};
let stackEnvState = null;

async function editStackEnv(repoName, qaEnabled = false) {
    // .env contents are secrets: the backend serves them to admins only.
    if (!isAdmin()) {
        _notifyForbidden(`/stacks/${repoName}/env`);
        return;
    }
    stackEnvState = { repo: repoName, active: null, files: {} };
    document.getElementById('stack-env-tabs').style.display = qaEnabled ? '' : 'none';
    document.getElementById('stack-env-modal').classList.add('active');
    await switchStackEnvTab('prod');
}

async function switchStackEnvTab(env) {
    const state = stackEnvState;
    if (!state || !STACK_ENV_FILES[env] || state.active === env) return;
    const textarea = document.getElementById('stack-env-content');
    const saveBtn = document.getElementById('stack-env-save');

    const leaving = state.files[state.active];
    if (leaving && !leaving.error) leaving.content = textarea.value;
    state.active = env;

    document.querySelectorAll('#stack-env-tabs .env-tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.env === env);
    });
    document.getElementById('stack-env-title').textContent =
        `Edit ${STACK_ENV_FILES[env].name}: ${state.repo}`;
    document.getElementById('stack-env-help').textContent = STACK_ENV_FILES[env].help;

    if (!state.files[env]) {
        textarea.value = 'Loading...';
        textarea.disabled = true;
        saveBtn.disabled = true;
        // An empty .env and a backend that could not reach the build host look
        // identical in the editor, and saving over the second one would wipe the
        // real file, so the failure is shown verbatim and editing stays disabled.
        const { data, error } = await apiGetOrError(
            `/stacks/${encodeURIComponent(state.repo)}/env?env=${env}`);
        // The modal was reopened, or another tab picked, while this loaded.
        if (stackEnvState !== state) return;
        state.files[env] = error
            ? { error }
            : { content: data.content || '', saved: data.content || '' };
        if (error) showNotification('error', error);
        if (state.active !== env) return;
    }

    const file = state.files[env];
    if (file.error) {
        textarea.value = `# Could not read the ${STACK_ENV_FILES[env].name} file:\n# ${file.error}\n`;
        textarea.disabled = true;
        saveBtn.disabled = true;
        return;
    }
    textarea.value = file.content;
    textarea.disabled = false;
    saveBtn.disabled = false;
    textarea.focus();
}

async function saveStackEnv() {
    const state = stackEnvState;
    const textarea = document.getElementById('stack-env-content');
    const saveBtn = document.getElementById('stack-env-save');
    if (!state || !state.files[state.active] || state.files[state.active].error) return;
    const env = state.active;
    const content = textarea.value;

    saveBtn.disabled = true;
    saveBtn.innerHTML = '<span class="btn-loading"></span> Saving...';

    try {
        const result = await apiPut(
            `/stacks/${encodeURIComponent(state.repo)}/env?env=${env}`, { content });

        // apiPut returns null when the request was refused (403 already
        // reported to the user) or failed to parse.
        if (result && result.success) {
            const file = state.files[env];
            file.content = file.saved = content;
            // Stay open while the other tab still holds unsaved edits.
            const unsaved = Object.entries(state.files).find(
                ([name, f]) => name !== env && !f.error && f.content !== f.saved);
            if (unsaved) {
                showNotification('warning', `${STACK_ENV_FILES[env].name} saved; `
                    + `${STACK_ENV_FILES[unsaved[0]].name} still has unsaved changes`);
            } else {
                showNotification('success', `${STACK_ENV_FILES[env].name} saved successfully`);
                closeStackEnvModal();
            }
        } else if (result) {
            showNotification('error', result.message || 'Failed to save file');
        }
    } catch (e) {
        showNotification('error', e.message || 'Failed to save file');
    } finally {
        saveBtn.disabled = false;
        saveBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/>
                <polyline points="17 21 17 13 7 13 7 21"/>
                <polyline points="7 3 7 8 15 8"/>
            </svg>
            Save
        `;
    }
}

function closeStackEnvModal() {
    stackEnvState = null;
    document.getElementById('stack-env-modal').classList.remove('active');
}
