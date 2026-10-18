/**
 * PulsarCD frontend: Settings modal: LLM provider, test chat and allowed Google accounts.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Settings Modal ==============

let _settingsConfig = null;

function switchSettingsTab(tabName) {
    document.querySelectorAll('.settings-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.settings-panel').forEach(p => p.classList.remove('active'));
    document.querySelector(`.settings-tab[data-tab="${tabName}"]`).classList.add('active');
    document.getElementById(`settings-panel-${tabName}`).classList.add('active');

    if (tabName === 'users') loadUsersList();
}

async function openSettingsModal() {
    const modal = document.getElementById('settings-modal');
    const status = document.getElementById('settings-status');
    status.textContent = '';
    status.className = 'settings-status';

    // Load config
    const config = await apiGet('/admin/config');
    if (!config) {
        status.textContent = 'Failed to load configuration';
        status.className = 'settings-status error';
        modal.classList.add('active');
        return;
    }
    _settingsConfig = config;

    // Populate LLM fields
    const llm = config.llm || {};
    document.getElementById('settings-llm-url').value = llm.url || '';
    document.getElementById('settings-llm-model').value = llm.model || '';
    document.getElementById('settings-llm-apikey').value = llm.api_key || '';
    document.getElementById('settings-llm-context-tokens').value = llm.context_tokens || 128000;
    document.getElementById('settings-llm-max-output-tokens').value = llm.max_output_tokens || 16384;

    // Populate MCP servers
    renderMCPServers(config.mcp_servers || []);

    // Reset to LLM tab
    switchSettingsTab('llm');
    modal.classList.add('active');
}

function closeSettingsModal() {
    document.getElementById('settings-modal').classList.remove('active');
}

// ============== LLM Provider Presets ==============

const LLM_PRESETS = {
    openai:     { url: 'https://api.openai.com',        model: 'gpt-5.4' },
    anthropic:  { url: 'https://api.anthropic.com',      model: 'claude-sonnet-4-20250514' },
    gemini:     { url: 'https://generativelanguage.googleapis.com/v1beta/openai', model: 'gemini-2.5-flash' },
    mistral:    { url: 'https://api.mistral.ai',         model: 'mistral-small-latest' },
    ollama:     { url: 'http://localhost:11434',          model: 'qwen3:8b' },
    vllm:       { url: 'http://vllm-dev-service:8000',   model: '' },
};

function applyLLMPreset() {
    const preset = LLM_PRESETS[document.getElementById('settings-llm-preset').value];
    if (!preset) return;
    document.getElementById('settings-llm-url').value = preset.url;
    document.getElementById('settings-llm-model').value = preset.model;
}

async function testLLMConnection() {
    const result = document.getElementById('settings-llm-test-result');
    result.textContent = 'Testing...';
    result.className = 'settings-status';

    try {
        const resp = await fetch(`${API_BASE}/admin/llm-connection-test`, {
            method: 'POST',
            headers: { ...authHeaders(), 'Content-Type': 'application/json' },
            body: JSON.stringify({
                url: document.getElementById('settings-llm-url').value,
                model: document.getElementById('settings-llm-model').value,
                api_key: document.getElementById('settings-llm-apikey').value,
            }),
        });
        const data = await resp.json();
        if (data.ok) {
            result.textContent = `Connected — model: ${data.model}`;
            result.className = 'settings-status success';
        } else {
            result.textContent = data.error || 'Connection failed';
            result.className = 'settings-status error';
        }
    } catch (e) {
        result.textContent = 'Network error: ' + e.message;
        result.className = 'settings-status error';
    }
}

// ============== LLM Test Chat ==============

let _llmTestBusy = false;

function clearLLMChat() {
    document.getElementById('llm-test-messages').innerHTML = '';
}

async function sendLLMTest() {
    if (_llmTestBusy) return;
    const input = document.getElementById('llm-test-input');
    const messages = document.getElementById('llm-test-messages');
    const sendBtn = document.getElementById('llm-test-send');
    const text = input.value.trim();
    if (!text) return;

    // Add user message
    const userMsg = document.createElement('div');
    userMsg.className = 'llm-test-msg user';
    userMsg.textContent = text;
    messages.appendChild(userMsg);
    input.value = '';

    // Add thinking indicator
    const thinking = document.createElement('div');
    thinking.className = 'llm-test-msg thinking';
    thinking.textContent = 'Thinking';
    messages.appendChild(thinking);
    messages.scrollTop = messages.scrollHeight;

    _llmTestBusy = true;
    sendBtn.disabled = true;
    input.disabled = true;

    try {
        const response = await fetch(`${API_BASE}/admin/llm-test`, {
            method: 'POST',
            headers: { ...authHeaders(), 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: text })
        });
        thinking.remove();

        if (!response.ok) {
            const err = await response.json().catch(() => ({ detail: `HTTP ${response.status}` }));
            const errMsg = document.createElement('div');
            errMsg.className = 'llm-test-msg error';
            errMsg.textContent = err.detail || `Error ${response.status}`;
            messages.appendChild(errMsg);
        } else {
            const data = await response.json();
            const reply = document.createElement('div');
            reply.className = 'llm-test-msg assistant';
            reply.innerHTML = simpleMarkdown(data.response || '(empty response)');
            messages.appendChild(reply);
        }
    } catch (e) {
        thinking.remove();
        const errMsg = document.createElement('div');
        errMsg.className = 'llm-test-msg error';
        errMsg.textContent = `Connection error: ${e.message}`;
        messages.appendChild(errMsg);
    }

    _llmTestBusy = false;
    sendBtn.disabled = false;
    input.disabled = false;
    input.focus();
    messages.scrollTop = messages.scrollHeight;
}

// ============== Allowed Google Accounts (Settings) ==============
//
// "Users" are Google addresses: there is no local account to create and no
// password to set. An address listed here may sign in with Google; removing it
// ends its open sessions. Addresses marked `managed` come from
// PULSARCD_AUTH__GOOGLE_ADMINS/VIEWERS and can only be changed by redeploying,
// so the UI shows them as locked rather than offering an edit that would be
// undone on the next restart.

async function loadUsersList() {
    const container = document.getElementById('users-list');
    container.innerHTML = '<div class="loading-placeholder">Loading...</div>';

    const data = await apiGet('/admin/users');
    if (!data || !data.users) {
        container.innerHTML = '<div class="empty-state">Failed to load allowed accounts</div>';
        return;
    }

    const rows = data.users.map(u => {
        const email = escapeHtml(u.email);
        const other = u.role === 'admin' ? 'viewer' : 'admin';
        const actions = u.managed
            ? '<span class="user-row-managed" title="Set through PULSARCD_AUTH__GOOGLE_ADMINS / _VIEWERS">from environment</span>'
            : `<button class="btn btn-xs btn-secondary" data-click="setUserRole" data-args="${uiArgs(u.email, other)}">Make ${other}</button>
               <button class="btn btn-xs btn-secondary" data-click="deleteUser" data-args="${uiArgs(u.email)}">Remove</button>`;
        return `
        <div class="user-row">
            <div class="user-row-info">
                <span class="user-row-name">${email}</span>
                <span class="user-row-role">${escapeHtml(u.role)}</span>
            </div>
            <div class="user-row-actions">${actions}</div>
        </div>`;
    }).join('');

    const notices = [];
    if (!data.google_enabled) {
        notices.push('Google sign-in is not configured (PULSARCD_AUTH__GOOGLE_CLIENT_ID), so none of these addresses can sign in yet.');
    }
    if (data.local_admin) {
        notices.push(`Break-glass administrator "${escapeHtml(data.local_admin.username)}" is enabled; its password comes from PULSARCD_AUTH__PASSWORD.`);
    }

    container.innerHTML =
        (rows || '<div class="empty-state">No address is allowed to sign in yet</div>') +
        notices.map(n => `<p class="settings-hint">${n}</p>`).join('');
}

async function createUser() {
    const emailEl = document.getElementById('new-user-email');
    const email = emailEl.value.trim();
    const role = document.getElementById('new-user-role').value;
    if (!email) return;

    const result = await apiPost('/admin/users', { email, role });
    if (result) {
        emailEl.value = '';
        loadUsersList();
    }
}

async function setUserRole(email, role) {
    if (!confirm(`Make "${email}" a ${role}? This ends their current session.`)) return;
    await _userRequest(`/admin/users/${encodeURIComponent(email)}`, 'PUT', { role });
}

async function deleteUser(email) {
    if (!confirm(`Revoke access for "${email}"?`)) return;
    await _userRequest(`/admin/users/${encodeURIComponent(email)}`, 'DELETE');
}

/** Send one allowlist change and refresh the list, reporting what went wrong. */
async function _userRequest(path, method, body) {
    try {
        const resp = await fetch(`${API_BASE}${path}`, {
            method,
            headers: body
                ? { ...authHeaders(), 'Content-Type': 'application/json' }
                : authHeaders(),
            body: body ? JSON.stringify(body) : undefined,
        });
        if (resp.ok) loadUsersList();
        else {
            const err = await resp.json().catch(() => ({}));
            alert(err.detail || `Request failed (HTTP ${resp.status})`);
        }
    } catch { alert('Connection error'); }
}
