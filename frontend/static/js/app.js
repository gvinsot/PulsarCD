/**
 * PulsarCD Frontend Application
 * Professional Docker Log Analytics Dashboard
 */

// API Base URL
const API_BASE = '/api';

// State
let currentView = 'dashboard';

// Hash ↔ view mapping
const HASH_TO_VIEW = { dashboard: 'dashboard', stacks: 'stacks', computers: 'containers', logs: 'logs', terminal: 'terminal' };
const VIEW_TO_HASH = { dashboard: 'Dashboard', stacks: 'Stacks', containers: 'Computers', logs: 'Logs', terminal: 'Terminal' };

function getViewFromHash() {
    const h = location.hash.replace('#', '').toLowerCase();
    return HASH_TO_VIEW[h] || null;
}
let currentContainer = null;
let charts = {};
let logsPage = 0;
let logsPageSize = 100;
let totalLogs = 0;

// ============== Authentication ==============

function getAuthToken() { return localStorage.getItem('pulsarcd_token'); }
function setAuthToken(token) { localStorage.setItem('pulsarcd_token', token); }
function clearAuthToken() { localStorage.removeItem('pulsarcd_token'); }

function authHeaders() {
    const t = getAuthToken();
    return t ? { 'Authorization': `Bearer ${t}` } : {};
}

function showLogin() {
    document.getElementById('login-overlay').style.display = 'flex';
    document.querySelector('.app').style.display = 'none';
    document.getElementById('login-error').textContent = '';
    document.getElementById('login-error').style.display = 'none';
}

function hideLogin() {
    document.getElementById('login-overlay').style.display = 'none';
    document.querySelector('.app').style.display = '';
}

function logout() {
    clearAuthToken();
    showLogin();
}

let _currentUserRole = 'viewer';

async function checkAuth() {
    const token = getAuthToken();
    if (!token) {
        showLogin();
        return;
    }
    try {
        const response = await fetch(`${API_BASE}/auth/me`, { headers: authHeaders() });
        if (response.status === 401) {
            showLogin();
            return;
        }
        const data = await response.json();
        _currentUserRole = data.role || 'viewer';
        updateUserMenu(data.username || 'user', data.role || 'viewer');
        hideLogin();
        switchView(getViewFromHash() || 'dashboard');
    } catch {
        showLogin();
    }
}

function initLoginForm() {
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = document.getElementById('login-username').value;
        const password = document.getElementById('login-password').value;
        const errorEl = document.getElementById('login-error');

        try {
            const response = await fetch(`${API_BASE}/auth/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password })
            });
            if (response.ok) {
                const data = await response.json();
                setAuthToken(data.token);
                errorEl.style.display = 'none';
                await checkAuth();
            } else {
                errorEl.textContent = 'Invalid username or password';
                errorEl.style.display = 'block';
            }
        } catch {
            errorEl.textContent = 'Connection error';
            errorEl.style.display = 'block';
        }
    });
}

// ============== User Menu ==============

function updateUserMenu(username, role) {
    const nameEl = document.getElementById('user-menu-name');
    const usernameEl = document.getElementById('user-menu-username');
    const roleEl = document.getElementById('user-menu-role');
    if (nameEl) nameEl.textContent = username;
    if (usernameEl) usernameEl.textContent = username;
    if (roleEl) roleEl.textContent = role;

    // Only show settings for admins
    const settingsItem = document.getElementById('settings-menu-item');
    if (settingsItem) settingsItem.style.display = role === 'admin' ? '' : 'none';

    // Load GitHub status in user menu
    apiGet('/stacks/status').then(s => _updateGitHubBadge(s));
}

function _updateGitHubBadge(status) {
    const textEl = document.getElementById('user-menu-github-text');
    const dotEl = document.getElementById('user-menu-github-dot');
    if (!textEl || !dotEl) return;

    if (status && status.configured) {
        textEl.textContent = status.username ? `@${status.username}` : 'Connected';
        dotEl.className = 'user-menu-github-dot connected';
    } else {
        textEl.textContent = 'Not configured';
        dotEl.className = 'user-menu-github-dot disconnected';
    }
}

function toggleUserMenu() {
    const menu = document.getElementById('user-menu');
    menu.classList.toggle('open');
}

function closeUserMenu() {
    const menu = document.getElementById('user-menu');
    menu.classList.remove('open');
}

// Close user menu when clicking outside
document.addEventListener('click', (e) => {
    const menu = document.getElementById('user-menu');
    if (menu && !menu.contains(e.target)) {
        menu.classList.remove('open');
    }
});


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

// ============== Agent Modal ==============

function openAgentModal() {
    const modal = document.getElementById('agent-modal');
    switchAgentTab('history');
    modal.classList.add('active');
}

function closeAgentModal() {
    document.getElementById('agent-modal').classList.remove('active');
}

function switchAgentTab(tabName) {
    document.querySelectorAll('.agent-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.agent-panel').forEach(p => p.classList.remove('active'));
    document.querySelector(`.agent-tab[data-tab="${tabName}"]`).classList.add('active');
    document.getElementById(`agent-panel-${tabName}`).classList.add('active');

    if (tabName === 'history') loadAgentHistory();
    if (tabName === 'instructions') loadInstructionsTab();
}

async function loadInstructionsTab() {
    // Reuse the settings load to populate the instruction fields
    const config = await apiGet('/admin/config');
    if (!config) return;
    const eh = config.error_handling || {};
    document.getElementById('settings-error-enabled').checked = eh.enabled !== false;
    document.getElementById('settings-error-instructions').value = eh.instructions || '';
    document.getElementById('settings-error-build').value = eh.on_build_failure || '';
    document.getElementById('settings-error-test').value = eh.on_test_failure || '';
    document.getElementById('settings-error-deploy').value = eh.on_deploy_failure || '';
    document.getElementById('settings-error-recurring').value = eh.on_recurring_error || '';

    const gates = config.pipeline_gates || {};
    document.getElementById('settings-gate-build-test').checked = !!gates.build_to_test;
    document.getElementById('settings-gate-test-deploy').checked = !!gates.test_to_deploy;
    document.getElementById('settings-gate-build-test-instructions').value = gates.on_build_to_test || '';
    document.getElementById('settings-gate-test-deploy-instructions').value = gates.on_test_to_deploy || '';
    document.getElementById('settings-gate-instructions').value = gates.instructions || '';
}

async function saveSettingsFromAgent() {
    // Load current config, overlay instruction changes, save
    const config = await apiGet('/admin/config');
    if (!config) { showNotification('error', 'Failed to load config'); return; }

    config.error_handling = config.error_handling || {};
    config.error_handling.enabled = document.getElementById('settings-error-enabled').checked;
    config.error_handling.instructions = document.getElementById('settings-error-instructions').value;
    config.error_handling.on_build_failure = document.getElementById('settings-error-build').value;
    config.error_handling.on_test_failure = document.getElementById('settings-error-test').value;
    config.error_handling.on_deploy_failure = document.getElementById('settings-error-deploy').value;
    config.error_handling.on_recurring_error = document.getElementById('settings-error-recurring').value;

    config.pipeline_gates = config.pipeline_gates || {};
    config.pipeline_gates.build_to_test = document.getElementById('settings-gate-build-test').checked;
    config.pipeline_gates.test_to_deploy = document.getElementById('settings-gate-test-deploy').checked;
    config.pipeline_gates.on_build_to_test = document.getElementById('settings-gate-build-test-instructions').value;
    config.pipeline_gates.on_test_to_deploy = document.getElementById('settings-gate-test-deploy-instructions').value;
    config.pipeline_gates.instructions = document.getElementById('settings-gate-instructions').value;

    try {
        const resp = await fetch(`${API_BASE}/admin/config`, {
            method: 'PUT',
            headers: { ...authHeaders(), 'Content-Type': 'application/json' },
            body: JSON.stringify(config),
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        showNotification('success', 'Instructions saved');
    } catch (e) {
        showNotification('error', 'Failed to save: ' + (e.message || e));
    }
}

let _agentHistoryPage = 1;
const _AGENT_HISTORY_PAGE_SIZE = 15;

async function loadAgentHistory(page) {
    if (page !== undefined) _agentHistoryPage = page;
    const container = document.getElementById('agent-history-list');
    container.innerHTML = '<div class="loading-placeholder">Loading...</div>';

    const data = await apiGet(`/admin/agent-history?page=${_agentHistoryPage}&page_size=${_AGENT_HISTORY_PAGE_SIZE}`);
    if (!data || !data.history || data.history.length === 0) {
        container.innerHTML = '<div class="agent-history-empty">No agent activity yet.</div>';
        _renderHistoryPagination(container, data);
        return;
    }

    const entries = data.history
        .filter(entry => {
            // Hide chat entries unless MCP tools were called
            if (entry.type === 'chat') {
                return entry.tools_called && entry.tools_called.length > 0;
            }
            return true;
        })
        .map(entry => {
            const typeClass = `type-${entry.type}${entry.approved === false ? ' rejected' : ''}`;
            const icon = _agentHistoryIcon(entry.type);
            const title = _agentHistoryTitle(entry);
            const detail = _agentHistoryDetail(entry);
            const time = _agentHistoryTime(entry.timestamp);
            return `
                <div class="agent-history-entry ${typeClass}">
                    <div class="agent-history-icon">${icon}</div>
                    <div class="agent-history-content">
                        <div class="agent-history-title">${escapeHtml(title)}</div>
                        <div class="agent-history-detail" onclick="this.classList.toggle('expanded')">${simpleMarkdown(detail)}</div>
                    </div>
                    <div class="agent-history-time">${time}</div>
                </div>
            `;
        }).join('');

    container.innerHTML = entries;
    // Mark details that overflow their max-height so "click to expand" only shows when needed
    container.querySelectorAll('.agent-history-detail').forEach(el => {
        if (el.scrollHeight > el.clientHeight) el.classList.add('overflows');
    });
    _renderHistoryPagination(container, data);
}

function _renderHistoryPagination(container, data) {
    if (!data || data.total_pages <= 1) return;
    const page = data.page;
    const total = data.total_pages;
    const totalEntries = data.total;

    const nav = document.createElement('div');
    nav.className = 'agent-history-pagination';
    nav.innerHTML = `
        <button class="pagination-btn" ${page <= 1 ? 'disabled' : ''} onclick="loadAgentHistory(${page - 1})" title="Previous">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><polyline points="15 18 9 12 15 6"/></svg>
        </button>
        <span class="pagination-info">${page} / ${total} <span class="pagination-total">(${totalEntries})</span></span>
        <button class="pagination-btn" ${page >= total ? 'disabled' : ''} onclick="loadAgentHistory(${page + 1})" title="Next">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><polyline points="9 18 15 12 9 6"/></svg>
        </button>
    `;
    container.appendChild(nav);
}

function _agentHistoryIcon(type) {
    const icons = {
        gate_decision: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>`,
        gate_error: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
        failure_handled: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
        failure_error: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
        recurring_handled: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>`,
        recurring_error: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
        chat: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>`,
    };
    return icons[type] || icons.chat;
}

function _agentHistoryTitle(entry) {
    switch (entry.type) {
        case 'gate_decision':
            return `Gate ${entry.transition || ''}: ${entry.approved ? 'Approved' : 'Rejected'} — ${entry.repo || ''}`;
        case 'gate_error':
            return `Gate ${entry.transition || ''}: Error — ${entry.repo || ''}`;
        case 'failure_handled':
            return `${(entry.stage || '').toUpperCase()} failure handled — ${entry.repo || ''}`;
        case 'failure_error':
            return `${(entry.stage || '').toUpperCase()} failure error — ${entry.repo || ''}`;
        case 'recurring_handled':
            return `Recurring error handled (${entry.count || 0}x) — ${entry.label || entry.services || entry.projects || ''}`;
        case 'recurring_error':
            return `Recurring error handling failed — ${entry.label || entry.services || entry.projects || ''}`;
        case 'chat':
            return `Chat: ${(entry.message || '').substring(0, 60)}`;
        default:
            return entry.type;
    }
}

function _agentHistoryDetail(entry) {
    const parts = [];
    // Show parameters for investigation entries
    if (entry.type === 'failure_handled' || entry.type === 'failure_error') {
        const params = [entry.repo, entry.stage, entry.version].filter(Boolean);
        if (params.length) parts.push('Params: ' + params.join(' / '));
    }
    if (entry.type === 'recurring_handled' || entry.type === 'recurring_error') {
        const params = [];
        if (entry.projects) params.push('Stack: ' + entry.projects);
        if (entry.services) params.push('Services: ' + entry.services);
        if (entry.count) params.push(entry.count + 'x');
        if (params.length) parts.push(params.join(' / '));
    }
    if (entry.type === 'gate_decision' || entry.type === 'gate_error') {
        const params = [entry.repo, entry.transition, entry.version].filter(Boolean);
        if (params.length) parts.push('Params: ' + params.join(' / '));
    }
    const detail = entry.response || entry.reason || entry.error || '';
    if (detail) parts.push(detail);
    return parts.join('\n');
}

function _agentHistoryTime(ts) {
    if (!ts) return '';
    const d = new Date(ts);
    const now = new Date();
    const diffMs = now - d;
    if (diffMs < 60000) return 'just now';
    if (diffMs < 3600000) return `${Math.floor(diffMs / 60000)}m ago`;
    if (diffMs < 86400000) return `${Math.floor(diffMs / 3600000)}h ago`;
    return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
}

function renderMCPServers(servers) {
    const container = document.getElementById('mcp-servers-list');
    container.innerHTML = servers.map((s, i) => `
        <div class="mcp-server-card" data-index="${i}">
            <button class="mcp-server-remove" onclick="removeMCPServer(${i})" title="Remove">&times;</button>
            <div class="settings-field">
                <label>Name</label>
                <input type="text" class="mcp-name" value="${escapeHtml(s.name || '')}" />
            </div>
            <div class="settings-field">
                <label>URL</label>
                <input type="text" class="mcp-url" value="${escapeHtml(s.url || '')}" />
            </div>
            <div class="settings-field">
                <label>API Key</label>
                <input type="password" class="mcp-apikey" value="${escapeHtml(s.api_key || '')}" autocomplete="off" data-lpignore="true" data-1p-ignore />
            </div>
            <div class="settings-field">
                <button class="btn btn-sm btn-secondary" onclick="testMCPServer(${i})" id="mcp-test-btn-${i}">Test Connection</button>
                <span class="mcp-test-result" id="mcp-test-result-${i}"></span>
            </div>
        </div>
    `).join('');
}

async function testMCPServer(index) {
    const cards = document.querySelectorAll('.mcp-server-card');
    const card = cards[index];
    if (!card) return;

    const url = card.querySelector('.mcp-url').value.trim();
    const apiKey = card.querySelector('.mcp-apikey').value.trim();
    const btn = document.getElementById(`mcp-test-btn-${index}`);
    const resultEl = document.getElementById(`mcp-test-result-${index}`);

    if (!url) {
        resultEl.textContent = 'URL is required';
        resultEl.className = 'mcp-test-result mcp-test-fail';
        return;
    }

    btn.disabled = true;
    btn.textContent = 'Testing...';
    resultEl.textContent = '';

    try {
        const response = await fetch(`${API_BASE}/admin/mcp-test`, {
            method: 'POST',
            headers: { ...authHeaders(), 'Content-Type': 'application/json' },
            body: JSON.stringify({ url, api_key: apiKey }),
        });
        const data = await response.json();

        if (data.ok) {
            resultEl.textContent = `OK — ${data.count} tool(s): ${data.tools.join(', ')}`;
            resultEl.className = 'mcp-test-result mcp-test-ok';
        } else {
            resultEl.textContent = `Failed: ${data.error}`;
            resultEl.className = 'mcp-test-result mcp-test-fail';
        }
    } catch (e) {
        resultEl.textContent = `Error: ${e.message}`;
        resultEl.className = 'mcp-test-result mcp-test-fail';
    } finally {
        btn.disabled = false;
        btn.textContent = 'Test Connection';
    }
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

/** Lightweight markdown→HTML: escapes HTML first, then applies common markdown patterns. */
function simpleMarkdown(str) {
    if (!str) return '';
    let h = escapeHtml(str);
    // Fenced code blocks: ```lang\n...\n```
    h = h.replace(/```(\w*)\n([\s\S]*?)```/g, '<pre><code>$2</code></pre>');
    // Inline code: `code`
    h = h.replace(/`([^`]+)`/g, '<code>$1</code>');
    // Headers: ### h3, ## h2, # h1
    h = h.replace(/^### (.+)$/gm, '<strong>$1</strong>');
    h = h.replace(/^## (.+)$/gm, '<strong style="font-size:1.05em">$1</strong>');
    h = h.replace(/^# (.+)$/gm, '<strong style="font-size:1.1em">$1</strong>');
    // Bold: **text**
    h = h.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    // Italic: *text*
    h = h.replace(/\*(.+?)\*/g, '<em>$1</em>');
    // Unordered list items: - item or * item
    h = h.replace(/^[\-\*] (.+)$/gm, '• $1');
    // Numbered list items: 1. item
    h = h.replace(/^\d+\. (.+)$/gm, '‣ $1');
    // Line breaks
    h = h.replace(/\n/g, '<br>');
    return h;
}

function addMCPServer() {
    const servers = collectMCPServers();
    servers.push({ name: '', url: '', api_key: '' });
    renderMCPServers(servers);
}

function removeMCPServer(index) {
    const servers = collectMCPServers();
    servers.splice(index, 1);
    renderMCPServers(servers);
}

function collectMCPServers() {
    const cards = document.querySelectorAll('.mcp-server-card');
    const servers = [];
    cards.forEach(card => {
        servers.push({
            name: card.querySelector('.mcp-name').value,
            url: card.querySelector('.mcp-url').value,
            api_key: card.querySelector('.mcp-apikey').value,
        });
    });
    return servers;
}

async function saveSettings() {
    const status = document.getElementById('settings-status');
    status.textContent = 'Saving...';
    status.className = 'settings-status';

    // Re-fetch full config to avoid overwriting fields managed elsewhere
    const config = await apiGet('/admin/config');
    if (!config) {
        status.textContent = 'Failed to load current config';
        status.className = 'settings-status error';
        return;
    }

    // Overlay only the fields from this modal
    config.llm = config.llm || {};
    config.llm.url = document.getElementById('settings-llm-url').value;
    config.llm.model = document.getElementById('settings-llm-model').value;
    config.llm.api_key = document.getElementById('settings-llm-apikey').value;
    config.llm.context_tokens = parseInt(document.getElementById('settings-llm-context-tokens').value, 10) || 128000;
    config.llm.max_output_tokens = parseInt(document.getElementById('settings-llm-max-output-tokens').value, 10) || 16384;
    config.mcp_servers = collectMCPServers();

    try {
        const resp = await fetch(`${API_BASE}/admin/config`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json', ...authHeaders() },
            body: JSON.stringify(config),
        });
        if (resp.ok) {
            _settingsConfig = config;
            status.textContent = 'Saved';
            status.className = 'settings-status';
            setTimeout(() => { status.textContent = ''; }, 2000);
        } else {
            const err = await resp.json().catch(() => ({}));
            status.textContent = err.detail || 'Save failed';
            status.className = 'settings-status error';
        }
    } catch (e) {
        status.textContent = 'Connection error';
        status.className = 'settings-status error';
    }
}

// ============== Users Management (Settings) ==============

async function loadUsersList() {
    const container = document.getElementById('users-list');
    container.innerHTML = '<div class="loading-placeholder">Loading...</div>';

    const data = await apiGet('/admin/users');
    if (!data || !data.users) {
        container.innerHTML = '<div class="empty-state">Failed to load users</div>';
        return;
    }

    container.innerHTML = data.users.map(u => `
        <div class="user-row">
            <div class="user-row-info">
                <span class="user-row-name">${escapeHtml(u.username)}</span>
                <span class="user-row-role">${escapeHtml(u.role)}</span>
            </div>
            <div class="user-row-actions">
                <button class="btn btn-xs btn-secondary" onclick="deleteUser('${escapeHtml(u.username)}')">Delete</button>
            </div>
        </div>
    `).join('');
}

async function createUser() {
    const username = document.getElementById('new-user-username').value.trim();
    const password = document.getElementById('new-user-password').value;
    const role = document.getElementById('new-user-role').value;

    if (!username || !password) return;

    const result = await apiPost('/admin/users', { username, password, role });
    if (result) {
        document.getElementById('new-user-username').value = '';
        document.getElementById('new-user-password').value = '';
        loadUsersList();
    }
}

async function deleteUser(username) {
    if (!confirm(`Delete user "${username}"?`)) return;
    try {
        const resp = await fetch(`${API_BASE}/admin/users/${username}`, {
            method: 'DELETE',
            headers: authHeaders(),
        });
        if (resp.ok) loadUsersList();
        else {
            const err = await resp.json().catch(() => ({}));
            alert(err.detail || 'Delete failed');
        }
    } catch { alert('Connection error'); }
}


// ============== Initialization ==============

document.addEventListener('DOMContentLoaded', () => {
    initLoginForm();
    initNavigation();
    initModalTabs();
    window.addEventListener('hashchange', () => {
        const view = getViewFromHash();
        if (view && view !== currentView) switchView(view, true);
    });
    checkAuth();
});

// ============== Mobile Menu ==============

function toggleMobileMenu() {
    const dropdown = document.getElementById('mobile-nav-dropdown');
    const logoIcon = document.getElementById('logo-icon');
    dropdown.classList.toggle('open');
    if (logoIcon) logoIcon.classList.toggle('menu-open', dropdown.classList.contains('open'));
}

function closeMobileMenu() {
    const dropdown = document.getElementById('mobile-nav-dropdown');
    const logoIcon = document.getElementById('logo-icon');
    if (dropdown) dropdown.classList.remove('open');
    if (logoIcon) logoIcon.classList.remove('menu-open');
}

function initNavigation() {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const view = item.dataset.view;
            switchView(view);
            // Close mobile menu after navigation
            closeMobileMenu();
        });
    });
}

function switchView(view, skipHash) {
    currentView = view;
    
    // Update URL hash
    if (!skipHash && VIEW_TO_HASH[view]) {
        history.replaceState(null, '', '#' + VIEW_TO_HASH[view]);
    }
    
    // Update nav
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.view === view);
    });
    
    // Update views
    document.querySelectorAll('.view').forEach(v => {
        v.classList.toggle('active', v.id === `${view}-view`);
    });
    
    // Stop stacks polling when leaving the view
    if (view !== 'stacks') {
        stopStacksPolling();
    }

    // Load view data
    switch (view) {
        case 'dashboard':
            loadDashboard();
            break;
        case 'containers':
            loadContainers();
            break;
        case 'logs':
            // Load last 100 logs by default on first visit
            if (totalLogs === 0) {
                loadDefaultLogs();
            }
            checkAIStatus();
            renderRecentQueries();
            break;
        case 'stacks':
            loadStacks();
            break;
        case 'terminal':
            initTerminal();
            setTimeout(() => {
                if (terminalFitAddon) terminalFitAddon.fit();
                if (terminalInstance) terminalInstance.focus();
            }, 100);
            break;
    }
}

// ============== AI Search ==============

let aiAvailable = false;

async function checkAIStatus() {
    try {
        const status = await apiGet('/ai/status');
        aiAvailable = status && status.available;
        
        const indicator = document.getElementById('ai-status-indicator');
        if (indicator) {
            indicator.classList.toggle('available', aiAvailable);
            indicator.classList.toggle('unavailable', !aiAvailable);
            indicator.title = aiAvailable ? 'AI Ready' : 'AI Unavailable';
        }
    } catch (e) {
        aiAvailable = false;
    }
}

// ============== Recent Queries ==============

const RECENT_QUERIES_KEY = 'pulsarcd_recent_queries';
const MAX_RECENT_QUERIES = 10;

function getRecentQueries() {
    try {
        const stored = localStorage.getItem(RECENT_QUERIES_KEY);
        return stored ? JSON.parse(stored) : [];
    } catch {
        return [];
    }
}

function saveRecentQuery(question, queryParams) {
    const queries = getRecentQueries();
    
    // Remove duplicates
    const filtered = queries.filter(q => q.question.toLowerCase() !== question.toLowerCase());
    
    // Add new query at the beginning
    filtered.unshift({
        question: question,
        params: queryParams,
        timestamp: Date.now()
    });
    
    // Keep only MAX_RECENT_QUERIES
    const trimmed = filtered.slice(0, MAX_RECENT_QUERIES);
    
    localStorage.setItem(RECENT_QUERIES_KEY, JSON.stringify(trimmed));
    renderRecentQueries();
}

function renderRecentQueries() {
    const queries = getRecentQueries();
    const container = document.getElementById('recent-queries');
    const list = document.getElementById('recent-queries-list');
    
    if (!queries.length) {
        container.style.display = 'none';
        return;
    }
    
    list.innerHTML = queries.map((q, idx) => `
        <span class="recent-query-item" onclick="useRecentQuery(${idx})" title="${escapeHtml(q.question)}">
            ${escapeHtml(q.question.length > 40 ? q.question.substring(0, 40) + '...' : q.question)}
            <span class="delete-query" onclick="event.stopPropagation(); deleteRecentQuery(${idx})">✕</span>
        </span>
    `).join('');
    
    container.style.display = 'block';
}

function showRecentQueries() {
    renderRecentQueries();
}

function hideRecentQueries() {
    // Delay hiding to allow click on items
    setTimeout(() => {
        const container = document.getElementById('recent-queries');
        // Don't hide if mouse is over the container
        if (!container.matches(':hover')) {
            // container.style.display = 'none';
        }
    }, 200);
}

function useRecentQuery(index) {
    const queries = getRecentQueries();
    if (index >= 0 && index < queries.length) {
        const query = queries[index];
        document.getElementById('ai-query').value = query.question;
        
        // If we have saved params, use them directly (skip AI call)
        if (query.params) {
            populateGeneratedQuery(query.params);
            
            // Execute search with saved params
            const paginationParams = { ...query.params, sort_order: query.params.sort_order || 'desc' };
            if (query.params.time_range) {
                const now = new Date();
                let start = new Date(now);
                if (query.params.time_range.endsWith('m')) {
                    start.setMinutes(start.getMinutes() - parseInt(query.params.time_range));
                } else if (query.params.time_range.endsWith('h')) {
                    start.setHours(start.getHours() - parseInt(query.params.time_range));
                } else if (query.params.time_range.endsWith('d')) {
                    start.setDate(start.getDate() - parseInt(query.params.time_range));
                }
                paginationParams.start_time = start.toISOString();
                delete paginationParams.time_range;
            }
            lastSearchParams = paginationParams;
            logsPage = 0;
            executeSearchWithParams(paginationParams);
        } else {
            // No saved params, run AI search
            aiSearchLogs();
        }
    }
}

function deleteRecentQuery(index) {
    const queries = getRecentQueries();
    queries.splice(index, 1);
    localStorage.setItem(RECENT_QUERIES_KEY, JSON.stringify(queries));
    renderRecentQueries();
}

function clearRecentQueries() {
    localStorage.removeItem(RECENT_QUERIES_KEY);
    renderRecentQueries();
}

async function aiSearchLogs() {
    const question = document.getElementById('ai-query').value.trim();
    if (!question) return;
    
    const btn = document.getElementById('ai-search-btn');
    const btnText = btn.querySelector('.btn-text');
    const btnLoading = btn.querySelector('.btn-loading');
    
    // Hide recent queries panel
    document.getElementById('recent-queries').style.display = 'none';
    
    // Show loading
    btnText.style.display = 'none';
    btnLoading.style.display = 'inline';
    btn.disabled = true;
    
    try {
        const result = await apiPost('/logs/ai-search', { question });
        
        if (result) {
            const params = result.query_params;
            
            // Save to recent queries (with params for reuse)
            saveRecentQuery(question, params);
            
            // Populate the generated query fields
            populateGeneratedQuery(params);
            
            // Store params for pagination (convert time_range to start_time)
            const paginationParams = { ...params, sort_order: params.sort_order || 'desc' };
            if (params.time_range) {
                const now = new Date();
                let start = new Date(now);
                if (params.time_range.endsWith('m')) {
                    start.setMinutes(start.getMinutes() - parseInt(params.time_range));
                } else if (params.time_range.endsWith('h')) {
                    start.setHours(start.getHours() - parseInt(params.time_range));
                } else if (params.time_range.endsWith('d')) {
                    start.setDate(start.getDate() - parseInt(params.time_range));
                }
                paginationParams.start_time = start.toISOString();
                delete paginationParams.time_range;
            }
            lastSearchParams = paginationParams;
            logsPage = 0;
            
            // Display results
            const searchResult = result.result;
            totalLogs = searchResult.total;
            displayLogsResults(searchResult.hits);
            updatePagination();
        }
    } catch (e) {
        console.error('AI search failed', e);
        alert('AI search failed. Please try again.');
    } finally {
        btnText.style.display = 'inline';
        btnLoading.style.display = 'none';
        btn.disabled = false;
    }
}

function populateGeneratedQuery(params) {
    // Show the generated query panel
    document.getElementById('ai-generated-query').style.display = 'block';
    
    // Build a clean JSON object for display
    const displayParams = {};
    if (params.query) displayParams.query = params.query;
    if (params.levels && params.levels.length) displayParams.levels = params.levels;
    if (params.time_range) displayParams.time_range = params.time_range;
    if (params.http_status_min) displayParams.http_status_min = params.http_status_min;
    if (params.http_status_max) displayParams.http_status_max = params.http_status_max;
    if (params.hosts && params.hosts.length) displayParams.hosts = params.hosts;
    if (params.containers && params.containers.length) displayParams.containers = params.containers;
    displayParams.sort_order = params.sort_order || 'desc';
    
    // Display as formatted JSON
    document.getElementById('gen-query-json').value = JSON.stringify(displayParams, null, 2);
}

async function executeGeneratedQuery() {
    const jsonText = document.getElementById('gen-query-json').value.trim();
    
    let params;
    try {
        params = JSON.parse(jsonText);
    } catch (e) {
        alert('Invalid JSON format. Please check the query syntax.');
        return;
    }
    
    // Convert time_range to start_time if present
    if (params.time_range) {
        const now = new Date();
        let start = new Date(now);
        const tr = params.time_range;
        
        if (tr.endsWith('m')) {
            start.setMinutes(start.getMinutes() - parseInt(tr));
        } else if (tr.endsWith('h')) {
            start.setHours(start.getHours() - parseInt(tr));
        } else if (tr.endsWith('d')) {
            start.setDate(start.getDate() - parseInt(tr));
        }
        
        params.start_time = start.toISOString();
        delete params.time_range;
    }
    
    // Store for pagination and reset page
    lastSearchParams = params;
    logsPage = 0;
    
    try {
        await executeSearchWithParams(params);
    } catch (e) {
        console.error('Query failed', e);
        alert('Search query failed.');
    }
}

async function loadDefaultLogs() {
    // Load the last 100 logs with no filters
    const params = {
        sort_order: 'desc'
    };
    
    // Store for pagination
    lastSearchParams = params;
    logsPage = 0;
    
    await executeSearchWithParams(params);
    document.getElementById('results-count').textContent = `${formatNumber(totalLogs)} total logs (showing latest)`;
}

async function refreshLogsSearch() {
    // If we have a previous search, re-execute it
    if (lastSearchParams) {
        logsPage = 0; // Reset to first page
        await executeSearchWithParams(lastSearchParams);
    } else {
        // Otherwise, load default logs
        await loadDefaultLogs();
    }
}

async function searchHttpErrors(minStatus, maxStatus) {
    // Switch to logs view
    switchView('logs');
    
    // Calculate time range for last 24 hours
    const now = new Date();
    const startTime = new Date(now);
    startTime.setHours(startTime.getHours() - 24);
    
    // Build search params
    const params = {
        http_status_min: minStatus,
        http_status_max: maxStatus,
        start_time: startTime.toISOString(),
        sort_order: 'desc'
    };
    
    // Store for pagination
    lastSearchParams = params;
    logsPage = 0;
    
    // Execute search
    await executeSearchWithParams(params);
    
    // Update results count message
    const statusRange = minStatus === 400 ? '4xx' : '5xx';
    document.getElementById('results-count').textContent = `${formatNumber(totalLogs)} HTTP ${statusRange} errors (last 24h)`;
}

function displayLogsResults(logs) {
    const tbody = document.getElementById('logs-table-body');
    tbody.innerHTML = logs.map((log, index) => `
        <tr class="${getLogRowClass(log)} log-row" data-log-index="${index}" onclick="toggleLogExpand(this, ${index})">
            <td class="col-time">${formatTime(log.timestamp)}</td>
            <td class="col-source" title="${escapeHtml(log.host)} / ${escapeHtml(log.container_name)}">
                <span class="source-host">${escapeHtml(log.host)}</span>
                <span class="source-container">${escapeHtml(log.container_name)}</span>
            </td>
            <td class="col-level">${log.level ? `<span class="log-level ${log.level.toLowerCase()}">${escapeHtml(log.level)}</span>` : ''}</td>
            <td class="col-message"><div class="message-truncate">${escapeHtml(log.message)}</div></td>
        </tr>
        <tr class="log-expand-row" id="log-expand-${index}" style="display: none;">
            <td colspan="4">
                <div class="log-expand-content">
                    <div class="log-full-message">
                        <pre>${escapeHtml(log.message)}</pre>
                    </div>
                    <div class="log-analysis">
                        <div class="analysis-item similar-count">
                            <span class="analysis-label">📊 Similar logs (24h):</span>
                            <span class="analysis-value loading" id="search-similar-${index}">Loading...</span>
                        </div>
                        <div class="analysis-item create-task-item">
                            <button class="btn btn-task-create" onclick="event.stopPropagation(); openCreateTaskModal('search', ${index})">🤖 Create Agent Task</button>
                        </div>
                    </div>
                </div>
            </td>
        </tr>
    `).join('');
    
    // Store logs for later reference
    window.currentLogResults = logs;
    
    document.getElementById('results-count').textContent = `${formatNumber(totalLogs)} results`;
    updatePagination();
}

async function toggleLogExpand(row, index) {
    console.log('toggleLogExpand called', index);
    
    const expandRow = document.getElementById(`log-expand-${index}`);
    if (!expandRow) {
        console.error('Expand row not found for index', index);
        return;
    }
    
    const isExpanded = expandRow.style.display !== 'none';
    
    // Close all other expanded rows
    document.querySelectorAll('.log-expand-row').forEach(r => r.style.display = 'none');
    document.querySelectorAll('.log-row').forEach(r => r.classList.remove('expanded'));
    
    if (!isExpanded) {
        expandRow.style.display = 'table-row';
        row.classList.add('expanded');
        
        // Load analysis if not already loaded - use getElementById for reliability
        const similarEl = document.getElementById(`search-similar-${index}`);
        
        console.log('Similar element:', similarEl);
        
        if (similarEl && similarEl.classList.contains('loading')) {
            console.log('Loading similar count for index', index);
            loadSimilarCount(index, similarEl);
        }
    }
}

async function loadSimilarCount(index, element) {
    try {
        const log = window.currentLogResults[index];
        const result = await apiPost('/logs/similar-count', {
            message: log.message,
            container_name: log.container_name,
            hours: 24
        });
        
        element.classList.remove('loading');
        if (result && result.count !== undefined) {
            const count = result.count;
            element.textContent = count;
            element.classList.add(count > 100 ? 'high' : count > 10 ? 'medium' : 'low');
        } else {
            element.textContent = 'N/A';
        }
    } catch (e) {
        element.classList.remove('loading');
        element.textContent = 'Error';
    }
}

async function loadAIAssessment(index, element) {
    // Deprecated — replaced by Create Task button
}

// ============== Create Agent Task Modal ==============

function openCreateTaskModal(source, index) {
    let log, containerName;
    if (source === 'search') {
        log = window.currentLogResults[index];
        containerName = log.container_name || '';
    } else {
        log = currentContainerLogs[index];
        containerName = currentContainer ? currentContainer.data.name : '';
    }

    const message = log.message || '';
    const level = log.level || '';
    const host = log.host || '';
    const timestamp = log.timestamp || '';

    // Derive project name: prefer compose_project from log/container,
    // then extract stack name from Swarm container name (stack_service.slot.taskid)
    let project = log.compose_project || '';
    if (!project && currentContainer) {
        project = currentContainer.data.compose_project || '';
    }
    if (!project && containerName) {
        const m = containerName.match(/^(.+?)_[^.]+\.\d+\.\w+$/);
        project = m ? m[1] : containerName.replace(/\.\d+\..+$/, '');
    }

    const taskDesc = [
        `ERROR LOG detected in container '${containerName}'`,
        host ? `on host '${host}'` : '',
        timestamp ? `at ${timestamp}` : '',
        level ? `\nLevel: ${level}` : '',
        `\nLog message:\n\`\`\`\n${message}\n\`\`\``,
        `\nInvestigate and fix this error.`
    ].filter(Boolean).join(' ');

    document.getElementById('task-project').value = project;
    document.getElementById('task-source').value = `${containerName}${host ? ' @ ' + host : ''}`;
    document.getElementById('task-description').value = taskDesc;
    document.getElementById('task-submit-btn').disabled = false;
    document.getElementById('task-submit-btn').textContent = 'Send Task';

    document.getElementById('create-task-modal').classList.add('active');
}

function closeCreateTaskModal() {
    document.getElementById('create-task-modal').classList.remove('active');
    // Reset to edit mode
    const ta = document.getElementById('task-description');
    const preview = document.getElementById('task-description-preview');
    const btn = document.getElementById('task-toggle-preview');
    ta.style.display = '';
    preview.style.display = 'none';
    btn.textContent = 'Preview';
}

function toggleTaskPreview() {
    const ta = document.getElementById('task-description');
    const preview = document.getElementById('task-description-preview');
    const btn = document.getElementById('task-toggle-preview');
    if (preview.style.display === 'none') {
        preview.innerHTML = simpleMarkdown(ta.value);
        preview.style.display = '';
        ta.style.display = 'none';
        btn.textContent = 'Edit';
    } else {
        preview.style.display = 'none';
        ta.style.display = '';
        btn.textContent = 'Preview';
    }
}

function updateTaskPreview() {
    const preview = document.getElementById('task-description-preview');
    if (preview.style.display !== 'none') {
        preview.innerHTML = simpleMarkdown(document.getElementById('task-description').value);
    }
}

async function submitCreateTask() {
    const project = document.getElementById('task-project').value.trim();
    const task = document.getElementById('task-description').value.trim();
    const btn = document.getElementById('task-submit-btn');

    if (!project || !task) {
        showNotification('error', 'Project and task description are required');
        return;
    }

    btn.disabled = true;
    btn.textContent = 'Analyzing...';

    try {
        const resp = await fetch(`${API_BASE}/tasks/create`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', ...authHeaders() },
            body: JSON.stringify({ task, project }),
            signal: AbortSignal.timeout(180000),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        showNotification('success', 'Task created by AI agent');
        closeCreateTaskModal();
    } catch (e) {
        showNotification('error', 'Failed to create task: ' + (e.message || e));
        btn.disabled = false;
        btn.textContent = 'Send Task';
    }
}

function getLogRowClass(log) {
    const msg = (log.message || '').toLowerCase();
    // Use the log level if explicitly set
    if (log.level === 'ERROR' || log.level === 'FATAL' || log.level === 'CRITICAL') return 'log-row-error';
    if (log.level === 'WARN' || log.level === 'WARNING') return 'log-row-warning';
    // Otherwise detect from message content (excluding URL paths)
    if (isErrorLog(msg)) return 'log-row-error';
    if (isWarningLog(msg)) return 'log-row-warning';
    return '';
}

function initModalTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;

            // Update buttons
            document.querySelectorAll('.tab-btn').forEach(b => {
                b.classList.toggle('active', b.dataset.tab === tab);
            });

            // Update content
            document.querySelectorAll('.tab-content').forEach(c => {
                c.classList.toggle('active', c.id === `tab-${tab}`);
            });

            // Load env vars on demand when switching to env tab
            if (tab === 'env' && currentContainer && Object.keys(currentContainerEnv).length === 0) {
                refreshContainerEnv();
            }
            // Load metrics charts when switching to stats tab
            if (tab === 'stats' && currentContainer) {
                loadContainerMetrics();
            }
        });
    });
}

// ============== API Helpers ==============

async function apiGet(endpoint) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, { headers: authHeaders() });
        if (response.status === 401) { showLogin(); return null; }
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
    } catch (error) {
        console.error(`API Error (${endpoint}):`, error);
        return null;
    }
}

/**
 * API GET with 404 retry support. If the endpoint returns 404,
 * refreshes the container list and retries once.
 */
async function apiGetWithRetry(endpoint, retryCallback = null) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, { headers: authHeaders() });
        if (response.status === 401) { showLogin(); return null; }
        if (response.status === 404 && retryCallback) {
            console.log(`Container not found (404), refreshing and retrying...`);
            await retryCallback();
            // Retry once after refresh
            const retryResponse = await fetch(`${API_BASE}${endpoint}`, { headers: authHeaders() });
            if (!retryResponse.ok) throw new Error(`HTTP ${retryResponse.status}`);
            return await retryResponse.json();
        }
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
    } catch (error) {
        console.error(`API Error (${endpoint}):`, error);
        return null;
    }
}

async function apiPost(endpoint, data) {
    try {
        const opts = { method: 'POST', headers: { ...authHeaders() } };
        if (data !== undefined) {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(data);
        }
        const response = await fetch(`${API_BASE}${endpoint}`, opts);
        if (response.status === 401) { showLogin(); return null; }
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`API Error (${endpoint}):`, error);
        return null;
    }
}

async function apiPut(endpoint, data) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json', ...authHeaders() },
            body: JSON.stringify(data)
        });
        if (response.status === 401) { showLogin(); return null; }
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`API Error (${endpoint}):`, error);
        throw error;
    }
}

function showNotification(type, message) {
    // Use the shared action-toast-container for consistent positioning
    let container = document.getElementById('action-toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'action-toast-container';
        document.body.appendChild(container);
    }
    
    // Remove any existing notification toast (keep action toasts)
    const existing = container.querySelector('.action-toast-notification');
    if (existing) existing.remove();
    
    const iconMap = {
        success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
        error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
        warning: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>'
    };
    
    const toast = document.createElement('div');
    toast.className = `action-toast action-toast-notification action-toast-${type}`;
    toast.innerHTML = `
        <div class="action-toast-content">
            <div class="action-toast-info">
                <span class="action-toast-icon action-toast-icon-${type}">${iconMap[type] || ''}</span>
                <span class="action-toast-text">${escapeHtml(message)}</span>
                <button class="action-toast-close" onclick="this.closest('.action-toast').classList.add('toast-exit'); setTimeout(() => this.closest('.action-toast')?.remove(), 300)">&times;</button>
            </div>
        </div>
    `;
    
    container.appendChild(toast);
    requestAnimationFrame(() => toast.classList.add('toast-visible'));
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (toast.parentElement) {
            toast.classList.add('toast-exit');
            setTimeout(() => toast.remove(), 300);
        }
    }, 5000);
}

// ============== Dashboard ==============

async function loadDashboard() {
    // Load stats
    const stats = await apiGet('/dashboard/stats');
    if (stats) {
        document.getElementById('running-containers').textContent = stats.running_containers;
        document.getElementById('total-hosts').textContent = stats.total_hosts;
        document.getElementById('http-4xx').textContent = formatNumber(stats.http_4xx_24h);
        document.getElementById('http-5xx').textContent = formatNumber(stats.http_5xx_24h);
        document.getElementById('avg-cpu').textContent = `${stats.avg_cpu_percent.toFixed(1)}%`;
        document.getElementById('avg-memory').textContent = `${stats.avg_memory_percent.toFixed(1)}%`;
        
        // Show GPU stats if available
        if (stats.avg_gpu_percent != null) {
            document.getElementById('gpu-stat-card').style.display = '';
            document.getElementById('avg-gpu').textContent = `${stats.avg_gpu_percent.toFixed(1)}%`;
        } else {
            document.getElementById('gpu-stat-card').style.display = 'none';
        }

        // Show VRAM stats if available
        if (stats.avg_vram_used_mb != null && stats.avg_vram_total_mb != null) {
            document.getElementById('vram-stat-card').style.display = '';
            document.getElementById('avg-vram').textContent = `${formatMemory(stats.avg_vram_used_mb)} / ${formatMemory(stats.avg_vram_total_mb)}`;
        } else {
            document.getElementById('vram-stat-card').style.display = 'none';
        }
    }
    
    // Load charts and recurring errors in parallel
    await Promise.all([
        loadErrorsChart().catch(e => console.error('Failed to load errors chart:', e)),
        loadHttpChart().catch(e => console.error('Failed to load http chart:', e)),
        loadCpuChart().catch(e => console.error('Failed to load cpu chart:', e)),
        loadGpuChart().catch(e => console.error('Failed to load gpu chart:', e)),
        loadMemoryChart().catch(e => console.error('Failed to load memory chart:', e)),
        loadVramChart().catch(e => console.error('Failed to load vram chart:', e)),
        loadRecurringErrors().catch(e => console.error('Failed to load recurring errors:', e)),
    ]);
}

let recurringErrorsData = [];

async function loadRecurringErrors() {
    const data = await apiGet('/dashboard/recurring-errors?limit=5');
    const el = document.getElementById('recurring-errors-list');
    if (!el) return;

    const card = el.closest('.recurring-errors-card');
    if (!data || data.length === 0) {
        if (card) card.style.display = 'none';
        recurringErrorsData = [];
        return;
    }
    recurringErrorsData = data;

    // Build HTML first, then reveal card (avoids showing empty card if render fails)
    el.innerHTML = data.map((p, i) => {
        const stacks = p.stacks || [];
        const svcs = p.services || [];
        const stackLabel = stacks.length ? stacks.join(', ') + ' / ' : '';
        const services = stackLabel + svcs.slice(0, 3).join(', ') + (svcs.length > 3 ? ` +${svcs.length - 3}` : '');
        const age = formatRelativeTime(p.notified_at || p.last_seen);
        // Delivery status icon
        let deliveryIcon = '';
        if (p.delivered === true) {
            deliveryIcon = '<span class="rerr-delivery delivered" title="Agent task delivered successfully">&#10003;</span>';
        } else if (p.delivered === false) {
            deliveryIcon = '<span class="rerr-delivery failed" title="Agent task delivery failed">&#10007;</span>';
        } else {
            deliveryIcon = '<span class="rerr-delivery pending" title="Not yet sent to agent">&#8943;</span>';
        }
        return `
        <div class="recurring-error-item" onclick="showRecurringErrorDetail(${i})">
            <div class="recurring-error-header">
                <span class="recurring-error-count">${p.count}×</span>
                <span class="recurring-error-services">${escapeHtml(services)}</span>
                ${deliveryIcon}
                <span class="recurring-error-age">${age}</span>
            </div>
            <div class="recurring-error-message">${escapeHtml(p.sample_message)}</div>
        </div>`;
    }).join('');
    if (card) card.style.display = '';
}

function showRecurringErrorDetail(index) {
    const p = recurringErrorsData[index];
    if (!p) return;

    const svcs = p.services || [];
    const stacks = p.stacks || [];
    document.getElementById('recurring-error-modal-count').textContent =
        `${p.count} occurrence${p.count !== 1 ? 's' : ''}`;
    document.getElementById('recurring-error-modal-stacks').innerHTML =
        stacks.length
            ? stacks.map(s => `<span class="rerr-service-chip">${escapeHtml(s)}</span>`).join('')
            : '<span style="color:var(--text-muted)">Unknown</span>';
    document.getElementById('recurring-error-modal-services').innerHTML =
        svcs.length
            ? svcs.map(s => `<span class="rerr-service-chip">${escapeHtml(s)}</span>`).join('')
            : '<span style="color:var(--text-muted)">Unknown</span>';
    document.getElementById('recurring-error-modal-first-seen').textContent =
        formatRelativeTime(p.first_seen);
    document.getElementById('recurring-error-modal-last-seen').textContent =
        formatRelativeTime(p.last_seen);
    const deliveredEl = document.getElementById('recurring-error-modal-delivered');
    if (p.delivered === true) {
        deliveredEl.innerHTML = '<span class="rerr-delivery delivered">&#10003;</span> Delivered successfully';
    } else if (p.delivered === false) {
        deliveredEl.innerHTML = '<span class="rerr-delivery failed">&#10007;</span> Delivery failed' +
            (p.delivery_error ? ': ' + escapeHtml(p.delivery_error) : '');
    } else {
        deliveredEl.innerHTML = '<span class="rerr-delivery pending">&#8943;</span> Not sent (no agent configured)';
    }
    document.getElementById('recurring-error-modal-message').textContent = p.sample_message;

    const modal = document.getElementById('recurring-error-modal');
    modal.dataset.message = p.sample_message;
    modal.classList.add('open');
}

function closeRecurringErrorModal() {
    document.getElementById('recurring-error-modal').classList.remove('open');
}

function searchRecurringError() {
    const message = document.getElementById('recurring-error-modal').dataset.message || '';
    closeRecurringErrorModal();
    const trimmed = message.trim().substring(0, 60);
    showView('logs');
    setTimeout(() => {
        const searchInput = document.getElementById('log-search');
        if (searchInput) {
            searchInput.value = trimmed;
            searchLogs();
        }
    }, 100);
}

async function refreshDashboard() {
    await loadDashboard();
}

async function loadErrorsChart() {
    const [errorsData, requestsData] = await Promise.all([
        apiGet('/dashboard/errors-timeseries?hours=24&interval=1h'),
        apiGet('/dashboard/http-requests-timeseries?hours=24&interval=1h')
    ]);
    
    if (!errorsData) return;
    
    const ctx = document.getElementById('errors-chart').getContext('2d');
    
    if (charts.errors) charts.errors.destroy();
    
    const datasets = [
        {
            label: 'Errors',
            data: errorsData.map(p => p.value),
            borderColor: '#ef4444',
            backgroundColor: 'rgba(239, 68, 68, 0.1)',
            fill: true,
            tension: 0.3,
            yAxisID: 'y'
        }
    ];
    
    // Add HTTP requests on secondary axis if available
    if (requestsData && requestsData.length) {
        datasets.push({
            label: 'HTTP Requests',
            data: requestsData.map(p => p.value),
            borderColor: '#3b82f6',
            backgroundColor: 'rgba(59, 130, 246, 0.05)',
            fill: true,
            tension: 0.3,
            yAxisID: 'y1'
        });
    }
    
    charts.errors = new Chart(ctx, {
        type: 'line',
        data: {
            labels: errorsData.map(p => formatTime(p.timestamp)),
            datasets: datasets
        },
        options: getChartOptionsDualAxis('Errors', 'Requests')
    });
}

async function loadHttpChart() {
    const [data4xx, data5xx] = await Promise.all([
        apiGet('/dashboard/http-4xx-timeseries?hours=24&interval=1h'),
        apiGet('/dashboard/http-5xx-timeseries?hours=24&interval=1h')
    ]);
    
    if (!data4xx || !data5xx) return;
    
    const ctx = document.getElementById('http-chart').getContext('2d');
    
    if (charts.http) charts.http.destroy();
    
    charts.http = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data4xx.map(p => formatTime(p.timestamp)),
            datasets: [
                {
                    label: '4xx',
                    data: data4xx.map(p => p.value),
                    borderColor: '#f59e0b',
                    backgroundColor: 'rgba(245, 158, 11, 0.1)',
                    fill: true,
                    tension: 0.3
                },
                {
                    label: '5xx',
                    data: data5xx.map(p => p.value),
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    fill: true,
                    tension: 0.3
                }
            ]
        },
        options: getChartOptions()
    });
}

// Color palette for different hosts
const hostColors = [
    '#00d4aa', // teal
    '#f59e0b', // amber
    '#8b5cf6', // purple
    '#ef4444', // red
    '#3b82f6', // blue
    '#ec4899', // pink
    '#14b8a6', // cyan
    '#84cc16', // lime
];

function getHostColor(index) {
    return hostColors[index % hostColors.length];
}

async function loadCpuChart() {
    const data = await apiGet('/dashboard/cpu-timeseries-by-host?hours=24&interval=15m');
    if (!data || !data.length || !data[0].data) return;
    
    const ctx = document.getElementById('cpu-chart').getContext('2d');
    if (charts.cpu) charts.cpu.destroy();
    
    // Get all unique timestamps from first host (they should be aligned)
    const labels = data[0].data.map(p => formatTime(p.timestamp));
    
    // Create a dataset for each host
    const datasets = data.map((hostData, idx) => ({
        label: hostData.host,
        data: hostData.data.map(p => p.value),
        borderColor: getHostColor(idx),
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        borderWidth: 2
    }));
    
    charts.cpu = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: getChartOptions(true)
    });
}

async function loadGpuChart() {
    const data = await apiGet('/dashboard/gpu-timeseries-by-host?hours=24&interval=15m');
    if (!data || !data.length || !data[0]?.data) {
        // Show empty chart if no GPU data
        const ctx = document.getElementById('gpu-chart').getContext('2d');
        if (charts.gpu) charts.gpu.destroy();
        charts.gpu = new Chart(ctx, {
            type: 'line',
            data: { labels: [], datasets: [] },
            options: { ...getChartOptions(true), plugins: { ...getChartOptions(true).plugins, title: { display: true, text: 'No GPU data available', color: '#6e7681' } } }
        });
        return;
    }
    
    const ctx = document.getElementById('gpu-chart').getContext('2d');
    if (charts.gpu) charts.gpu.destroy();
    
    const labels = data[0].data.map(p => formatTime(p.timestamp));
    
    const datasets = data.map((hostData, idx) => ({
        label: hostData.host,
        data: hostData.data.map(p => p.value),
        borderColor: getHostColor(idx),
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        borderWidth: 2
    }));
    
    charts.gpu = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: getChartOptions(true)
    });
}

async function loadMemoryChart() {
    const data = await apiGet('/dashboard/memory-timeseries-by-host?hours=24&interval=15m');
    if (!data || !data.length || !data[0].data) return;
    
    const ctx = document.getElementById('memory-chart').getContext('2d');
    if (charts.memory) charts.memory.destroy();
    
    const labels = data[0].data.map(p => formatTime(p.timestamp));
    
    const datasets = data.map((hostData, idx) => ({
        label: hostData.host,
        data: hostData.data.map(p => p.value),
        borderColor: getHostColor(idx),
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        borderWidth: 2
    }));
    
    charts.memory = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: getChartOptions(true)
    });
}

async function loadVramChart() {
    const data = await apiGet('/dashboard/vram-timeseries-by-host?hours=24&interval=15m');
    if (!data || !data.length || !data[0]?.data) {
        // Hide chart if no VRAM data
        const ctx = document.getElementById('vram-chart').getContext('2d');
        if (charts.vram) charts.vram.destroy();
        charts.vram = new Chart(ctx, {
            type: 'line',
            data: { labels: [], datasets: [] },
            options: { ...getChartOptions(true), plugins: { ...getChartOptions(true).plugins, title: { display: true, text: 'No VRAM data available', color: '#6e7681' } } }
        });
        return;
    }
    
    const ctx = document.getElementById('vram-chart').getContext('2d');
    if (charts.vram) charts.vram.destroy();
    
    const labels = data[0].data.map(p => formatTime(p.timestamp));
    
    const datasets = data.map((hostData, idx) => ({
        label: hostData.host,
        data: hostData.data.map(p => p.value),
        borderColor: getHostColor(idx),
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        borderWidth: 2
    }));
    
    charts.vram = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: getChartOptions(true)
    });
}

function getChartOptionsDualAxis(leftLabel = 'Left', rightLabel = 'Right') {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            mode: 'index',
            intersect: false,
        },
        plugins: {
            legend: {
                display: true,
                position: 'top',
                labels: {
                    color: '#8b949e',
                    font: { family: 'Outfit' }
                }
            }
        },
        scales: {
            x: {
                grid: { color: '#21262d' },
                ticks: { color: '#6e7681', maxRotation: 0 }
            },
            y: {
                type: 'linear',
                display: true,
                position: 'left',
                grid: { color: '#21262d' },
                ticks: { color: '#ef4444' },
                title: {
                    display: true,
                    text: leftLabel,
                    color: '#ef4444'
                }
            },
            y1: {
                type: 'linear',
                display: true,
                position: 'right',
                grid: { drawOnChartArea: false },
                ticks: { color: '#3b82f6' },
                title: {
                    display: true,
                    text: rightLabel,
                    color: '#3b82f6'
                }
            }
        }
    };
}

function getChartOptions(isPercent = false) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                position: 'top',
                labels: {
                    color: '#8b949e',
                    font: { family: 'Outfit' }
                }
            }
        },
        scales: {
            x: {
                grid: { color: '#21262d' },
                ticks: { color: '#6e7681', maxRotation: 0 }
            },
            y: {
                grid: { color: '#21262d' },
                ticks: { 
                    color: '#6e7681',
                    callback: isPercent ? (v) => `${v}%` : undefined
                },
                beginAtZero: true,
                suggestedMax: isPercent ? 100 : undefined
            }
        }
    };
}

// ============== Containers ==============

// Local storage keys
const CONTAINERS_FILTER_KEY = 'pulsarcd_containers_filter';
const CONTAINERS_GROUPS_KEY = 'pulsarcd_containers_groups';

function getStoredFilter() {
    try {
        return localStorage.getItem(CONTAINERS_FILTER_KEY);
    } catch {
        return null;
    }
}

function saveFilter(value) {
    try {
        localStorage.setItem(CONTAINERS_FILTER_KEY, value);
    } catch (e) {
        console.error('Failed to save filter:', e);
    }
}

function getStoredGroups() {
    try {
        const stored = localStorage.getItem(CONTAINERS_GROUPS_KEY);
        return stored ? JSON.parse(stored) : {};
    } catch {
        return {};
    }
}

function saveGroups(groups) {
    try {
        localStorage.setItem(CONTAINERS_GROUPS_KEY, JSON.stringify(groups));
    } catch (e) {
        console.error('Failed to save groups:', e);
    }
}

function getGroupKey(host, project) {
    return `${host}::${project}`;
}

async function loadContainers(forceRefresh = false) {
    // Restore filters from localStorage
    const storedFilter = getStoredFilter();
    const statusFilter = storedFilter !== null ? storedFilter : document.getElementById('status-filter').value;
    if (storedFilter !== null) {
        document.getElementById('status-filter').value = storedFilter;
    }
    
    // Always use host grouping for Computers view
    const groupBy = 'host';
    
    // forceRefresh ensures backend cache is invalidated
    let endpoint = `/containers/grouped?refresh=true&group_by=${groupBy}`;
    if (statusFilter) {
        endpoint += `&status=${statusFilter}`;
    }
    
    // Fetch containers and host metrics in parallel
    const [grouped, hostMetrics] = await Promise.all([
        apiGet(endpoint),
        apiGet('/hosts/metrics')
    ]);
    if (!grouped) return;
    
    // Get stored group states
    const storedGroups = getStoredGroups();
    
    const container = document.getElementById('containers-list');
    container.innerHTML = '';
    
    const topLevelIcon = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <rect x="2" y="2" width="20" height="8" rx="2" ry="2"/>
            <rect x="2" y="14" width="20" height="8" rx="2" ry="2"/>
            <line x1="6" y1="6" x2="6.01" y2="6"/>
            <line x1="6" y1="18" x2="6.01" y2="18"/>
        </svg>`;
    
    for (const [topLevel, services] of Object.entries(grouped)) {
        const topLevelDiv = document.createElement('div');
        topLevelDiv.className = 'host-group';
        topLevelDiv.dataset.host = topLevel;
        topLevelDiv.dataset.stack = topLevel;
        
        // Check if top level group should be collapsed
        const topLevelKey = getGroupKey(topLevel, '');
        const isTopLevelCollapsed = storedGroups[topLevelKey] === false;
        if (isTopLevelCollapsed) {
            topLevelDiv.classList.add('collapsed');
        }
        
        const containerCount = Object.values(services).reduce((sum, containers) => sum + containers.length, 0);
        
        // Calculate group stats: total memory and max CPU
        let topLevelTotalMemory = 0;
        let topLevelMaxCpu = 0;
        for (const containers of Object.values(services)) {
            for (const c of containers) {
                if (c.memory_usage_mb != null) topLevelTotalMemory += c.memory_usage_mb;
                if (c.cpu_percent != null && c.cpu_percent > topLevelMaxCpu) topLevelMaxCpu = c.cpu_percent;
            }
        }
        const topLevelMemoryDisplay = topLevelTotalMemory > 0 ? formatMemory(topLevelTotalMemory) : '';
        const topLevelCpuClass = topLevelMaxCpu >= 80 ? 'cpu-critical' : (topLevelMaxCpu >= 50 ? 'cpu-warning' : '');
        const topLevelCpuDisplay = topLevelMaxCpu > 0 ? `${topLevelMaxCpu.toFixed(1)}%` : '';
        
        // Get GPU usage from host metrics
        let topLevelGpuDisplay = '';
        let topLevelGpuClass = '';
        let topLevelVramDisplay = '';
        let topLevelDiskDisplay = '';
        if (hostMetrics && hostMetrics[topLevel]) {
            const gpuPercent = hostMetrics[topLevel].gpu_percent;
            const gpuMemUsed = hostMetrics[topLevel].gpu_memory_used_mb;
            const gpuMemTotal = hostMetrics[topLevel].gpu_memory_total_mb;
            const diskUsedGb = hostMetrics[topLevel].disk_used_gb;
            const diskTotalGb = hostMetrics[topLevel].disk_total_gb;
            const diskPercent = hostMetrics[topLevel].disk_percent;
            if (gpuPercent != null) {
                topLevelGpuClass = gpuPercent >= 80 ? 'gpu-critical' : (gpuPercent >= 50 ? 'gpu-warning' : '');
                topLevelGpuDisplay = `${gpuPercent.toFixed(1)}%`;
            }
            if (gpuMemUsed != null && gpuMemTotal != null && gpuMemTotal > 0) {
                const vramPercent = (gpuMemUsed / gpuMemTotal) * 100;
                const vramClass = vramPercent >= 80 ? 'gpu-critical' : (vramPercent >= 50 ? 'gpu-warning' : '');
                topLevelVramDisplay = `<span class="group-stat group-gpu ${vramClass}" title="VRAM usage">🖼️ ${formatMemory(gpuMemUsed)} / ${formatMemory(gpuMemTotal)}</span>`;
            }
            if (diskUsedGb != null && diskTotalGb != null && diskTotalGb > 0) {
                const diskClass = diskPercent >= 90 ? 'disk-critical' : (diskPercent >= 75 ? 'disk-warning' : '');
                topLevelDiskDisplay = `<span class="group-stat group-disk ${diskClass}" title="Disk usage">💿 ${diskUsedGb.toFixed(1)} / ${diskTotalGb.toFixed(1)} GB</span>`;
            }
        }
        
        let topLevelHtml = `
            <div class="host-header" onclick="toggleHostGroup(event, this)">
                <span class="host-name">
                    <svg class="chevron-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="6 9 12 15 18 9"/>
                    </svg>
                    ${topLevelIcon}
                    ${escapeHtml(topLevel)}
                    <span class="group-count">${containerCount} containers</span>
                    ${topLevelMemoryDisplay ? `<span class="group-stat group-memory" title="RAM - Total memory usage">💾 ${topLevelMemoryDisplay}</span>` : ''}
                    ${topLevelCpuDisplay ? `<span class="group-stat group-cpu ${topLevelCpuClass}" title="CPU - Max usage">⚡ ${topLevelCpuDisplay}</span>` : ''}
                    ${topLevelDiskDisplay}
                    ${topLevelGpuDisplay ? `<span class="group-stat group-gpu ${topLevelGpuClass}" title="GPU - Compute usage">🎮 ${topLevelGpuDisplay}</span>` : ''}
                    ${topLevelVramDisplay}
                </span>
                <div class="host-header-actions" onclick="event.stopPropagation();">
                    <button class="btn btn-sm btn-warning" onclick="hostAction('${escapeHtml(topLevel)}', 'reboot')" title="Reboot this computer">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <polyline points="23 4 23 10 17 10"/>
                            <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
                        </svg>
                        <span>Reboot</span>
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="hostAction('${escapeHtml(topLevel)}', 'shutdown')" title="Shutdown this computer">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <path d="M18.36 6.64a9 9 0 1 1-12.73 0"/>
                            <line x1="12" y1="2" x2="12" y2="12"/>
                        </svg>
                        <span>Shutdown</span>
                    </button>
                </div>
            </div>
            <div class="host-content">
        `;
        
        for (const [service, containers] of Object.entries(services)) {
            const serviceName = service === '_standalone' ? 'Standalone Containers' : service;
            const groupKey = getGroupKey(topLevel, service);
            const isServiceCollapsed = storedGroups[groupKey] === false;
            const serviceGroupClass = isServiceCollapsed ? 'compose-group collapsed' : 'compose-group';
            
            // Calculate service stats: total memory and max CPU
            let serviceTotalMemory = 0;
            let serviceMaxCpu = 0;
            for (const c of containers) {
                if (c.memory_usage_mb != null) serviceTotalMemory += c.memory_usage_mb;
                if (c.cpu_percent != null && c.cpu_percent > serviceMaxCpu) serviceMaxCpu = c.cpu_percent;
            }
            const serviceMemoryDisplay = serviceTotalMemory > 0 ? formatMemory(serviceTotalMemory) : '';
            const serviceCpuClass = serviceMaxCpu >= 80 ? 'cpu-critical' : (serviceMaxCpu >= 50 ? 'cpu-warning' : '');
            const serviceCpuDisplay = serviceMaxCpu > 0 ? `${serviceMaxCpu.toFixed(1)}%` : '';
            
            // Calculate service GPU stats from host metrics
            let serviceGpuDisplay = '';
            let serviceGpuClass = '';
            let serviceVramDisplay = '';
            if (hostMetrics) {
                // Collect unique hosts for this service's containers
                const serviceHosts = new Set();
                for (const c of containers) {
                    if (c.host) serviceHosts.add(c.host);
                }
                
                // Aggregate GPU metrics: max GPU%, sum VRAM
                let maxGpuPercent = null;
                let totalVramUsed = 0;
                let totalVramTotal = 0;
                let hasVramData = false;
                
                for (const host of serviceHosts) {
                    if (hostMetrics[host]) {
                        const gpuPercent = hostMetrics[host].gpu_percent;
                        const gpuMemUsed = hostMetrics[host].gpu_memory_used_mb;
                        const gpuMemTotal = hostMetrics[host].gpu_memory_total_mb;
                        
                        if (gpuPercent != null) {
                            maxGpuPercent = maxGpuPercent != null ? Math.max(maxGpuPercent, gpuPercent) : gpuPercent;
                        }
                        if (gpuMemUsed != null && gpuMemTotal != null) {
                            totalVramUsed += gpuMemUsed;
                            totalVramTotal += gpuMemTotal;
                            hasVramData = true;
                        }
                    }
                }
                
                if (maxGpuPercent != null) {
                    serviceGpuClass = maxGpuPercent >= 80 ? 'gpu-critical' : (maxGpuPercent >= 50 ? 'gpu-warning' : '');
                    serviceGpuDisplay = `${maxGpuPercent.toFixed(1)}%`;
                }
                if (hasVramData && totalVramTotal > 0) {
                    const vramPercent = (totalVramUsed / totalVramTotal) * 100;
                    const vramClass = vramPercent >= 80 ? 'gpu-critical' : (vramPercent >= 50 ? 'gpu-warning' : '');
                    serviceVramDisplay = `<span class="group-stat group-gpu ${vramClass}" title="VRAM usage">🖼️ ${formatMemory(totalVramUsed)} / ${formatMemory(totalVramTotal)}</span>`;
                }
            }
            
            topLevelHtml += `
                <div class="${serviceGroupClass}" data-host="${escapeHtml(topLevel)}" data-project="${escapeHtml(service)}">
                    <div class="compose-header" onclick="toggleComposeGroup(event, this)">
                        <svg class="chevron-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="6 9 12 15 18 9"/>
                        </svg>
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                        </svg>
                        ${escapeHtml(serviceName)}
                        <span class="group-count">${containers.length}</span>
                        ${serviceMemoryDisplay ? `<span class="group-stat group-memory" title="Total memory usage">💾 ${serviceMemoryDisplay}</span>` : ''}
                        ${serviceCpuDisplay ? `<span class="group-stat group-cpu ${serviceCpuClass}" title="Max CPU usage">⚡ ${serviceCpuDisplay}</span>` : ''}
                        ${serviceGpuDisplay ? `<span class="group-stat group-gpu ${serviceGpuClass}" title="GPU - Max compute usage">🎮 ${serviceGpuDisplay}</span>` : ''}
                        ${serviceVramDisplay}
                    </div>
                    <div class="compose-content">
                        <div class="container-list">
            `;
            
            for (const c of containers) {
                // Format stats display
                const hasStats = c.cpu_percent != null || c.memory_percent != null;
                const cpuDisplay = c.cpu_percent != null ? `${c.cpu_percent}%` : '-';
                const memDisplay = c.memory_percent != null 
                    ? `${c.memory_percent}%${c.memory_usage_mb ? ` (${c.memory_usage_mb}MB)` : ''}`
                    : '-';
                const containerAge = c.created ? formatTimeAgo(c.created) : '';
                
                const containerNameHtml = escapeHtml(c.name);

                // Per-container GPU stats
                let gpuMiniHtml = '';
                if (c.gpu_memory_used_mb != null) {
                    const gpuSmDisplay = c.gpu_percent != null ? `${c.gpu_percent}%` : '';
                    const gpuMemDisplay = formatMemory(c.gpu_memory_used_mb);
                    gpuMiniHtml = `
                            <span class="stat-mini stat-mini-gpu" title="GPU${gpuSmDisplay ? ' SM ' + gpuSmDisplay : ''} — VRAM ${gpuMemDisplay}">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                                    <rect x="2" y="6" width="20" height="12" rx="2"/>
                                    <path d="M6 10h4v4H6zM14 10h4v4h-4z"/>
                                </svg>
                                ${gpuMemDisplay}
                            </span>`;
                }
                
                topLevelHtml += `
                    <div class="container-item" onclick="openContainer('${escapeHtml(c.host)}', '${escapeHtml(c.id)}', ${JSON.stringify(c).replace(/"/g, '&quot;')})">
                        <div class="container-info">
                            <span class="container-status ${c.status}"></span>
                            <div>
                                <div class="container-name">${containerNameHtml}</div>
                                <div class="container-image">${formatImageName(c.image)}${containerAge ? ` <span class="container-age">• ${containerAge}</span>` : ''}</div>
                            </div>
                        </div>
                        ${c.status === 'running' ? `
                        <div class="container-stats-mini">
                            <span class="stat-mini" title="CPU %">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                                    <rect x="4" y="4" width="16" height="16" rx="2"/>
                                    <rect x="9" y="9" width="6" height="6"/>
                                    <path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 15h3M1 9h3M1 15h3"/>
                                </svg>
                                ${cpuDisplay}
                            </span>
                            <span class="stat-mini" title="RAM">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                                    <path d="M2 20h20M6 16V8a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v8"/>
                                </svg>
                                ${memDisplay}
                            </span>${gpuMiniHtml}
                        </div>
                        ` : ''}
                        <div class="container-actions">
                            <button class="btn btn-sm btn-secondary" onclick="event.stopPropagation(); quickAction('${escapeHtml(c.host)}', '${escapeHtml(c.id)}', 'restart', '${escapeHtml(c.name)}')">
                                Restart
                            </button>
                            <button class="btn btn-sm btn-danger" onclick="event.stopPropagation(); quickAction('${escapeHtml(c.host)}', '${escapeHtml(c.id)}', 'remove', '${escapeHtml(c.name)}')">
                                Remove
                            </button>
                        </div>
                    </div>
                `;
            }
            
            topLevelHtml += `
                        </div>
                    </div>
                </div>
            `;
        }
        
        topLevelHtml += `</div>`; // Close host-content
        
        topLevelDiv.innerHTML = topLevelHtml;
        container.appendChild(topLevelDiv);
    }
}

function toggleHostGroup(event, headerEl) {
    event?.stopPropagation();
    const hostGroup = headerEl.closest('.host-group');
    if (!hostGroup) return;
    
    const host = hostGroup.dataset.host;
    const isCollapsed = hostGroup.classList.toggle('collapsed');
    
    // Save state
    const storedGroups = getStoredGroups();
    const hostKey = getGroupKey(host, '');
    storedGroups[hostKey] = !isCollapsed; // true = expanded, false = collapsed
    saveGroups(storedGroups);
}

function toggleComposeGroup(event, headerEl) {
    event.stopPropagation();
    const composeGroup = headerEl.closest('.compose-group');
    if (!composeGroup) return;
    
    const host = composeGroup.dataset.host;
    const project = composeGroup.dataset.project;
    const isCollapsed = composeGroup.classList.toggle('collapsed');
    
    // Save state
    const storedGroups = getStoredGroups();
    const groupKey = getGroupKey(host, project);
    storedGroups[groupKey] = !isCollapsed; // true = expanded, false = collapsed
    saveGroups(storedGroups);
}

async function refreshContainers() {
    await loadContainers();
}

function filterContainers() {
    // Save filter value
    const filterValue = document.getElementById('status-filter').value;
    saveFilter(filterValue);
    
    loadContainers();
}

// Host action (reboot/shutdown)
async function hostAction(hostName, action) {
    const actionLabel = action === 'reboot' ? 'reboot' : 'shutdown';
    const confirmMessage = `Are you sure you want to ${actionLabel} the computer "${hostName}"?\n\nThis action cannot be undone and will affect all containers on this host.`;
    
    if (!confirm(confirmMessage)) {
        return;
    }
    
    try {
        const result = await apiPost(`/hosts/${encodeURIComponent(hostName)}/action`, {
            action: action
        });
        
        if (result && result.success) {
            showNotification('success', `${actionLabel.charAt(0).toUpperCase() + actionLabel.slice(1)} command sent to ${hostName}`);
        } else {
            showNotification('error', result?.message || `Failed to ${actionLabel} ${hostName}`);
        }
    } catch (error) {
        console.error(`Failed to ${actionLabel} host:`, error);
        showNotification('error', `Failed to ${actionLabel}: ${error.message || 'Unknown error'}`);
    }
}

// ============== Container Modal ==============

// Store raw logs for filtering
let currentContainerLogs = [];
// Store raw env vars for filtering
let currentContainerEnv = {};

function openContainer(host, containerId, containerData) {
    currentContainer = { host, id: containerId, data: containerData };
    currentContainerLogs = [];
    currentContainerEnv = {};

    document.getElementById('modal-container-name').textContent = containerData.name;

    // Update status badge
    const statusBadge = document.getElementById('modal-container-status');
    statusBadge.textContent = containerData.status;
    statusBadge.className = `status-badge ${containerData.status}`;

    // Clear filter inputs
    document.getElementById('logs-filter').value = '';
    document.getElementById('logs-errors-only').checked = false;
    document.getElementById('env-filter').value = '';

    // Clear env viewer
    document.getElementById('container-env').innerHTML = '';

    // Show info
    const infoDiv = document.getElementById('container-info');
    infoDiv.innerHTML = `
        <div class="info-row"><span class="label">ID:</span><span class="value">${escapeHtml(containerData.id)}</span></div>
        <div class="info-row"><span class="label">Image:</span><span class="value">${escapeHtml(containerData.image)}</span></div>
        <div class="info-row"><span class="label">Status:</span><span class="value">${escapeHtml(containerData.status)}</span></div>
        <div class="info-row"><span class="label">Host:</span><span class="value">${escapeHtml(host)}</span></div>
        <div class="info-row"><span class="label">Compose Project:</span><span class="value">${escapeHtml(containerData.compose_project || '-')}</span></div>
        <div class="info-row"><span class="label">Compose Service:</span><span class="value">${escapeHtml(containerData.compose_service || '-')}</span></div>
        <div class="info-row"><span class="label">Created:</span><span class="value">${formatDateTime(containerData.created)}</span></div>
    `;
    
    // Load logs and stats
    refreshContainerLogs();
    refreshContainerStats();
    
    // Show modal
    document.getElementById('container-modal').classList.add('open');
    
    // Switch to logs tab
    document.querySelector('.tab-btn[data-tab="logs"]').click();
}

function closeModal() {
    document.getElementById('container-modal').classList.remove('open');
    currentContainer = null;
    currentContainerLogs = [];
    currentContainerEnv = {};
    _destroyContainerCharts();
}

async function refreshContainerLogs() {
    if (!currentContainer) return;
    
    const tail = document.getElementById('logs-tail').value || 500;
    const logs = await apiGetWithRetry(
        `/containers/${currentContainer.host}/${currentContainer.id}/logs?tail=${tail}`,
        () => loadContainers(true)  // Refresh container list on 404
    );
    
    const logViewer = document.getElementById('container-logs');
    currentContainerLogs = logs || [];
    
    renderContainerLogs();
}

function renderContainerLogs() {
    const logViewer = document.getElementById('container-logs');
    const filterText = document.getElementById('logs-filter').value.toLowerCase();
    const errorsOnly = document.getElementById('logs-errors-only').checked;
    
    if (currentContainerLogs.length === 0) {
        logViewer.innerHTML = '<div class="log-line">No logs available</div>';
        return;
    }
    
    const html = currentContainerLogs.map((log, index) => {
        const message = log.message || '';
        const msgLower = message.toLowerCase();
        
        // Determine log level class - prefer explicit level, then detect from message
        let levelClass = 'log-info';
        const level = (log.level || '').toUpperCase();
        
        if (level === 'ERROR' || level === 'FATAL' || level === 'CRITICAL') {
            levelClass = 'log-error';
        } else if (level === 'WARN' || level === 'WARNING') {
            levelClass = 'log-warning';
        } else if (level === 'DEBUG') {
            levelClass = 'log-debug';
        } else if (isErrorLog(msgLower)) {
            levelClass = 'log-error';
        } else if (isWarningLog(msgLower)) {
            levelClass = 'log-warning';
        } else if (msgLower.includes('debug')) {
            levelClass = 'log-debug';
        }
        
        // Check filters
        let hidden = false;
        if (errorsOnly && levelClass !== 'log-error' && levelClass !== 'log-warning') {
            hidden = true;
        }
        if (filterText && !msgLower.includes(filterText)) {
            hidden = true;
        }
        
        // Highlight search term
        let displayMessage = escapeHtml(message);
        if (filterText && !hidden) {
            const regex = new RegExp(`(${escapeRegex(filterText)})`, 'gi');
            displayMessage = displayMessage.replace(regex, '<span class="log-highlight">$1</span>');
        }
        
        const timestamp = formatDateTime(log.timestamp);
        
        return `
            <div class="log-line ${levelClass}${hidden ? ' hidden' : ''}" data-index="${index}" onclick="toggleContainerLogExpand(this, ${index})">
                <span class="log-timestamp">${timestamp}</span>
                <span class="log-message-truncate">${displayMessage}</span>
            </div>
            <div class="container-log-expand" id="container-log-expand-${index}" style="display: none;">
                <div class="log-expand-content">
                    <div class="log-full-message">
                        <pre>${escapeHtml(message)}</pre>
                    </div>
                    <div class="log-analysis">
                        <div class="analysis-item similar-count">
                            <span class="analysis-label">📊 Similar logs (24h):</span>
                            <span class="analysis-value loading" id="container-similar-${index}">Loading...</span>
                        </div>
                        <div class="analysis-item create-task-item">
                            <button class="btn btn-task-create" onclick="event.stopPropagation(); openCreateTaskModal('container', ${index})">🤖 Create Agent Task</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }).join('');
    
    logViewer.innerHTML = html;
    logViewer.scrollTop = logViewer.scrollHeight;
}

function toggleContainerLogExpand(element, index) {
    console.log('toggleContainerLogExpand called', index);
    
    const expandDiv = document.getElementById(`container-log-expand-${index}`);
    if (!expandDiv) {
        console.error('Expand div not found for index', index);
        return;
    }
    
    const isExpanded = expandDiv.style.display !== 'none';
    
    // Close all other expanded logs
    document.querySelectorAll('.container-log-expand').forEach(el => el.style.display = 'none');
    document.querySelectorAll('.log-line').forEach(el => el.classList.remove('expanded'));
    
    if (!isExpanded) {
        expandDiv.style.display = 'block';
        element.classList.add('expanded');
        
        // Load analysis
        const similarEl = document.getElementById(`container-similar-${index}`);
        
        console.log('Similar element:', similarEl);
        
        if (similarEl && similarEl.classList.contains('loading')) {
            console.log('Loading similar count for index', index);
            loadContainerLogSimilar(index, similarEl);
        }
    }
}

async function loadContainerLogSimilar(index, element) {
    try {
        const log = currentContainerLogs[index];
        const result = await apiPost('/logs/similar-count', {
            message: log.message,
            container_name: currentContainer.data.name,
            hours: 24
        });
        
        element.classList.remove('loading');
        if (result && result.count !== undefined) {
            const count = result.count;
            element.textContent = count;
            element.classList.add(count > 100 ? 'high' : count > 10 ? 'medium' : 'low');
        } else {
            element.textContent = 'N/A';
        }
    } catch (e) {
        element.classList.remove('loading');
        element.textContent = 'Error';
    }
}

function isErrorLog(msg) {
    // Remove URLs from the message before checking for error keywords
    // This prevents "/api/errors-timeseries" from being flagged as an error
    const msgWithoutUrls = removeUrlPaths(msg);
    
    return msgWithoutUrls.includes('error') || msgWithoutUrls.includes('fail') || 
           msgWithoutUrls.includes('fatal') || msgWithoutUrls.includes('exception') || 
           msgWithoutUrls.includes('critical') || msgWithoutUrls.includes('panic');
}

function isWarningLog(msg) {
    const msgWithoutUrls = removeUrlPaths(msg);
    return msgWithoutUrls.includes('warn') || msgWithoutUrls.includes('warning');
}

function removeUrlPaths(msg) {
    // Remove URL paths (e.g., /api/errors-timeseries, GET /path/to/error)
    // Keep the rest of the message for error detection
    return msg
        // Remove quoted URLs like "GET /api/errors HTTP/1.1"
        .replace(/"[A-Z]+\s+\/[^"]*"/g, '')
        // Remove unquoted paths like GET /api/errors
        .replace(/(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+\/\S+/gi, '')
        // Remove standalone URL paths /path/to/something
        .replace(/\/[\w\-\/]+(?:\?[^\s]*)?/g, '');
}

function escapeRegex(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function filterContainerLogs() {
    renderContainerLogs();
}

async function refreshContainerStats() {
    if (!currentContainer) return;

    const stats = await apiGetWithRetry(
        `/containers/${currentContainer.host}/${currentContainer.id}/stats`,
        () => loadContainers(true)  // Refresh container list on 404
    );

    if (stats) {
        document.getElementById('stat-cpu').textContent = `${stats.cpu_percent.toFixed(2)}%`;
        document.getElementById('stat-memory').textContent =
            `${stats.memory_usage_mb.toFixed(1)} MB / ${stats.memory_limit_mb.toFixed(1)} MB (${stats.memory_percent.toFixed(1)}%)`;
        document.getElementById('stat-network').textContent =
            `↓ ${formatBytes(stats.network_rx_bytes)} / ↑ ${formatBytes(stats.network_tx_bytes)}`;
        document.getElementById('stat-block').textContent =
            `Read: ${formatBytes(stats.block_read_bytes)} / Write: ${formatBytes(stats.block_write_bytes)}`;
    } else {
        document.getElementById('stat-cpu').textContent = '-';
        document.getElementById('stat-memory').textContent = '-';
        document.getElementById('stat-network').textContent = '-';
        document.getElementById('stat-block').textContent = '-';
    }
}

const _containerCharts = { cpu: null, memory: null, errors: null };

function _destroyContainerCharts() {
    for (const key of Object.keys(_containerCharts)) {
        if (_containerCharts[key]) {
            _containerCharts[key].destroy();
            _containerCharts[key] = null;
        }
    }
}

function _buildContainerChart(canvasId, label, color, points) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    const labels = points.map(p => {
        const d = new Date(p.timestamp);
        return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    });
    const values = points.map(p => p.value);
    return new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label,
                data: values,
                borderColor: color,
                backgroundColor: color + '22',
                borderWidth: 1.5,
                pointRadius: points.length > 100 ? 0 : 2,
                tension: 0.3,
                fill: true,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y}` } },
            },
            scales: {
                x: {
                    ticks: { maxTicksLimit: 8, font: { size: 10 }, color: '#888', maxRotation: 0 },
                    grid: { color: '#333' },
                },
                y: {
                    beginAtZero: true,
                    ticks: { font: { size: 10 }, color: '#888' },
                    grid: { color: '#333' },
                },
            },
        }
    });
}

async function loadContainerMetrics() {
    if (!currentContainer) return;
    const period = document.getElementById('stats-period')?.value || '7d';
    const data = await apiGet(`/containers/${currentContainer.host}/${currentContainer.id}/metrics?period=${period}`);
    _destroyContainerCharts();
    if (!data) return;
    _containerCharts.cpu    = _buildContainerChart('chart-container-cpu',    'CPU %',    '#4e9af1', data.cpu    || []);
    _containerCharts.memory = _buildContainerChart('chart-container-memory', 'Memory %', '#a78bfa', data.memory || []);
    _containerCharts.errors = _buildContainerChart('chart-container-errors', 'Errors',   '#f87171', data.errors || []);
}

async function refreshContainerEnv() {
    if (!currentContainer) return;

    const envViewer = document.getElementById('container-env');
    envViewer.innerHTML = '<div class="loading">Loading environment variables...</div>';

    const envData = await apiGetWithRetry(
        `/containers/${currentContainer.host}/${currentContainer.id}/env`,
        () => loadContainers(true)  // Refresh container list on 404
    );

    if (envData && envData.variables) {
        currentContainerEnv = envData.variables;
        renderContainerEnv();
    } else if (envData && envData.error) {
        envViewer.innerHTML = `<div class="error-message">${escapeHtml(envData.error)}</div>`;
        currentContainerEnv = {};
    } else {
        envViewer.innerHTML = '<div class="error-message">Failed to load environment variables</div>';
        currentContainerEnv = {};
    }
}

function renderContainerEnv() {
    const envViewer = document.getElementById('container-env');
    const filter = document.getElementById('env-filter').value.toLowerCase();

    // Sort keys alphabetically
    const sortedKeys = Object.keys(currentContainerEnv).sort();

    if (sortedKeys.length === 0) {
        envViewer.innerHTML = '<div class="empty-message">No environment variables found</div>';
        return;
    }

    let html = '';
    for (const key of sortedKeys) {
        const value = currentContainerEnv[key];
        const matchesFilter = !filter ||
            key.toLowerCase().includes(filter) ||
            value.toLowerCase().includes(filter);

        html += `
            <div class="env-row${matchesFilter ? '' : ' hidden'}">
                <span class="env-key">${escapeHtml(key)}</span>
                <span class="env-value">${escapeHtml(value)}</span>
            </div>
        `;
    }

    envViewer.innerHTML = html;
}

function filterContainerEnv() {
    renderContainerEnv();
}

async function containerAction(action) {
    if (!currentContainer) return;
    
    const result = await apiPost('/containers/action', {
        host: currentContainer.host,
        container_id: currentContainer.id,
        action: action
    });
    
    if (result) {
        alert(result.message);
        if (result.success) {
            setTimeout(() => {
                refreshContainerStats();
                loadContainers();
            }, 1000);
        }
    }
}

async function quickAction(host, containerId, action, containerName = '') {
    // Show confirmation for destructive actions
    let confirmMessage = '';
    const name = containerName || containerId;
    
    if (action === 'restart') {
        confirmMessage = `Are you sure you want to restart container "${name}"?`;
    } else if (action === 'remove') {
        confirmMessage = `Are you sure you want to remove container "${name}"?\n\nThis action cannot be undone. The container will be stopped and deleted.`;
    }
    
    if (confirmMessage && !confirm(confirmMessage)) {
        return;
    }
    
    const result = await apiPost('/containers/action', {
        host: host,
        container_id: containerId,
        action: action
    });
    
    if (result) {
        alert(result.message);
        setTimeout(loadContainers, 1000);
    }
}

async function removeStack(stackName, host) {
    // Show confirmation
    const confirmMessage = `Are you sure you want to remove the entire Docker Swarm stack "${stackName}"?\n\nThis will remove ALL services and containers in this stack. This action cannot be undone.`;
    
    if (!confirm(confirmMessage)) {
        return;
    }
    
    try {
        const url = `/api/stacks/${encodeURIComponent(stackName)}/remove${host ? `?host=${encodeURIComponent(host)}` : ''}`;
        const response = await fetch(`${API_BASE}${url}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', ...authHeaders() }
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result && result.success) {
            alert(result.message);
            setTimeout(loadContainers, 2000); // Wait a bit longer for stack removal
        } else {
            alert(result?.message || result?.detail || 'Failed to remove stack');
        }
    } catch (error) {
        console.error('Failed to remove stack:', error);
        alert(`Failed to remove stack: ${error.message || 'Unknown error'}`);
    }
}

// Remove deployed stack from Stacks view
async function removeDeployedStack(stackName) {
    // Show confirmation
    const confirmMessage = `Are you sure you want to remove the deployed stack "${stackName}"?\n\nThis will remove ALL services and containers in this stack. This action cannot be undone.`;
    
    if (!confirm(confirmMessage)) {
        return;
    }
    
    try {
        const url = `/stacks/${encodeURIComponent(stackName)}/remove`;
        const response = await fetch(`${API_BASE}${url}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', ...authHeaders() }
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result && result.success) {
            showNotification('success', result.message);
            scheduleStacksStateUpdate(); // Incremental update after stack removal
        } else {
            showNotification('error', result?.message || result?.detail || 'Failed to remove stack');
        }
    } catch (error) {
        console.error('Failed to remove stack:', error);
        showNotification('error', `Failed to remove stack: ${error.message || 'Unknown error'}`);
    }
}

// ============== Remove Service ==============

async function removeService(serviceName) {
    const confirmMessage = `Are you sure you want to remove the service "${serviceName}"?\n\nThis will stop and remove all containers for this service. This action cannot be undone.`;
    
    if (!confirm(confirmMessage)) {
        return;
    }
    
    try {
        const url = `/services/${encodeURIComponent(serviceName)}/remove`;
        const response = await fetch(`${API_BASE}${url}`, { method: 'POST', headers: authHeaders() });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result && result.success) {
            showNotification('success', result.message);
            scheduleStacksStateUpdate(); // Incremental update after service removal
        } else {
            showNotification('error', result?.message || 'Failed to remove service');
        }
    } catch (error) {
        console.error('Failed to remove service:', error);
        showNotification('error', `Failed to remove service: ${error.message || 'Unknown error'}`);
    }
}

// ============== Service Status (docker service ps) ==============

let serviceStatusInterval = null;
let currentStatusServiceName = null;

async function openServiceStatus(serviceName) {
    currentStatusServiceName = serviceName;

    const modal = document.getElementById('service-status-modal');
    const title = document.getElementById('service-status-title');
    const content = document.getElementById('service-status-content');
    const autoRefreshCheckbox = document.getElementById('service-status-auto-refresh');

    title.textContent = `Status: ${serviceName}`;
    content.innerHTML = '<div class="loading-placeholder">Loading task status...</div>';

    modal.classList.add('active');

    await refreshServiceStatus();

    if (autoRefreshCheckbox.checked) {
        startServiceStatusAutoRefresh();
    }
}

function closeServiceStatusModal() {
    stopServiceStatusAutoRefresh();
    document.getElementById('service-status-modal').classList.remove('active');
    currentStatusServiceName = null;
}

async function refreshServiceStatus() {
    if (!currentStatusServiceName) return;

    const result = await apiGet(`/services/${encodeURIComponent(currentStatusServiceName)}/tasks`);
    renderServiceStatusTable(result ? result.tasks : [], result ? result.service : currentStatusServiceName);
}

function renderServiceStatusTable(tasks, serviceName) {
    const content = document.getElementById('service-status-content');

    if (!tasks || tasks.length === 0) {
        content.innerHTML = '<div class="empty-placeholder">No task information available for this service</div>';
        return;
    }

    const stateColors = {
        'running': '#4caf50',
        'complete': '#2196f3',
        'ready': '#ff9800',
        'starting': '#ff9800',
        'preparing': '#ff9800',
        'assigned': '#ff9800',
        'accepted': '#ff9800',
        'pending': '#ff9800',
        'new': '#ff9800',
        'failed': '#f44336',
        'rejected': '#f44336',
        'shutdown': '#999',
        'orphaned': '#f44336',
        'remove': '#999',
    };

    let html = `<div style="padding: 12px; font-family: monospace; font-size: 13px;">`;
    html += `<table style="width: 100%; border-collapse: collapse; color: #e0e0e0;">`;
    html += `<thead><tr style="border-bottom: 1px solid #444; text-align: left;">`;
    html += `<th style="padding: 6px 10px;">ID</th>`;
    html += `<th style="padding: 6px 10px;">Image</th>`;
    html += `<th style="padding: 6px 10px;">Node</th>`;
    html += `<th style="padding: 6px 10px;">Desired State</th>`;
    html += `<th style="padding: 6px 10px;">Current State</th>`;
    html += `<th style="padding: 6px 10px;">Error</th>`;
    html += `<th style="padding: 6px 10px;">Updated</th>`;
    html += `</tr></thead><tbody>`;

    for (const task of tasks) {
        const stateColor = stateColors[task.state] || '#999';
        const desiredColor = stateColors[task.desired_state] || '#999';
        const errorText = task.error || task.message || '';
        const updatedAt = task.updated_at ? new Date(task.updated_at).toLocaleString() : '';
        const taskIdShort = (task.id || '').substring(0, 12);
        const imageShort = (task.image || '').replace(/^.*\//, '').replace(/@sha256:.*$/, '');

        html += `<tr style="border-bottom: 1px solid #333;">`;
        html += `<td style="padding: 6px 10px; font-family: monospace; font-size: 12px;" title="${escapeHtml(task.id || '')}">${escapeHtml(taskIdShort)}</td>`;
        html += `<td style="padding: 6px 10px; font-size: 12px;" title="${escapeHtml(task.image || '')}">${escapeHtml(imageShort)}</td>`;
        html += `<td style="padding: 6px 10px;">${escapeHtml(task.node || '')}</td>`;
        html += `<td style="padding: 6px 10px; color: ${desiredColor};">${escapeHtml(task.desired_state || '')}</td>`;
        html += `<td style="padding: 6px 10px; color: ${stateColor};">${escapeHtml(task.state || '')}</td>`;
        html += `<td style="padding: 6px 10px; color: #f44336; max-width: 400px; word-break: break-word;">${escapeHtml(errorText)}</td>`;
        html += `<td style="padding: 6px 10px; white-space: nowrap;">${escapeHtml(updatedAt)}</td>`;
        html += `</tr>`;
    }

    html += `</tbody></table></div>`;
    content.innerHTML = html;
}

function toggleServiceStatusAutoRefresh() {
    const checked = document.getElementById('service-status-auto-refresh').checked;
    if (checked) {
        startServiceStatusAutoRefresh();
    } else {
        stopServiceStatusAutoRefresh();
    }
}

function startServiceStatusAutoRefresh() {
    stopServiceStatusAutoRefresh();
    serviceStatusInterval = setInterval(refreshServiceStatus, 5000);
}

function stopServiceStatusAutoRefresh() {
    if (serviceStatusInterval) {
        clearInterval(serviceStatusInterval);
        serviceStatusInterval = null;
    }
}

// ============== Service Logs ==============

let serviceLogsInterval = null;
let currentServiceLogs = [];
let currentServiceName = null;

async function openServiceLogs(serviceName) {
    currentServiceName = serviceName;
    currentServiceLogs = [];
    
    const modal = document.getElementById('service-logs-modal');
    const title = document.getElementById('service-logs-title');
    const logViewer = document.getElementById('service-logs-content');
    const autoRefreshCheckbox = document.getElementById('service-logs-auto-refresh');
    
    title.textContent = `Logs: ${serviceName}`;
    logViewer.innerHTML = '<div class="loading-placeholder">Loading logs...</div>';
    
    modal.classList.add('active');
    
    // Initial load
    await refreshServiceLogs();
    
    // Start auto-refresh if checkbox is checked
    if (autoRefreshCheckbox.checked) {
        startServiceLogsAutoRefresh();
    }
}

function closeServiceLogsModal() {
    stopServiceLogsAutoRefresh();
    document.getElementById('service-logs-modal').classList.remove('active');
    currentServiceName = null;
    currentServiceLogs = [];
}

async function refreshServiceLogs() {
    if (!currentServiceName) return;
    
    const tail = document.getElementById('service-logs-tail').value || 200;
    const result = await apiGet(`/services/${encodeURIComponent(currentServiceName)}/logs?tail=${tail}`);
    
    // Check if the response is service task status info (service not running)
    if (result && result.type === 'service_tasks') {
        currentServiceLogs = [];
        renderServiceTasks(result.tasks, result.service);
        return;
    }
    
    currentServiceLogs = result || [];
    renderServiceLogs();
}

function renderServiceLogs() {
    const logViewer = document.getElementById('service-logs-content');
    const filter = document.getElementById('service-logs-filter').value.toLowerCase();
    const errorsOnly = document.getElementById('service-logs-errors-only').checked;
    
    let filteredLogs = currentServiceLogs;
    
    if (filter) {
        filteredLogs = filteredLogs.filter(log => 
            (log.message || '').toLowerCase().includes(filter)
        );
    }
    
    if (errorsOnly) {
        filteredLogs = filteredLogs.filter(log => 
            log.stream === 'stderr' || 
            /\b(error|exception|fatal|critical|panic|traceback)\b/i.test(log.message || '')
        );
    }
    
    if (filteredLogs.length === 0) {
        logViewer.innerHTML = '<div class="empty-placeholder">No logs found</div>';
        return;
    }
    
    logViewer.innerHTML = filteredLogs.map(log => {
        const isError = log.stream === 'stderr' || 
            /\b(error|exception|fatal|critical|panic|traceback)\b/i.test(log.message || '');
        const timestamp = log.timestamp ? formatLogTimestamp(log.timestamp) : '';
        
        return `<div class="log-line ${isError ? 'log-error' : ''}">` +
            (timestamp ? `<span class="log-timestamp">${escapeHtml(timestamp)}</span>` : '') +
            `<span class="log-message">${escapeHtml(log.message || '')}</span>` +
            `</div>`;
    }).join('');
    
    // Auto-scroll to bottom
    logViewer.scrollTop = logViewer.scrollHeight;
}

function renderServiceTasks(tasks, serviceName) {
    const logViewer = document.getElementById('service-logs-content');
    
    if (!tasks || tasks.length === 0) {
        logViewer.innerHTML = '<div class="empty-placeholder">No task information available for this service</div>';
        return;
    }
    
    const stateColors = {
        'running': '#4caf50',
        'complete': '#2196f3',
        'ready': '#ff9800',
        'starting': '#ff9800',
        'preparing': '#ff9800',
        'assigned': '#ff9800',
        'accepted': '#ff9800',
        'pending': '#ff9800',
        'new': '#ff9800',
        'failed': '#f44336',
        'rejected': '#f44336',
        'shutdown': '#999',
        'orphaned': '#f44336',
        'remove': '#999',
    };
    
    let html = `<div style="padding: 12px; font-family: monospace; font-size: 13px;">`;
    html += `<div style="color: #ff9800; margin-bottom: 12px; font-size: 14px;">`;
    html += `&#9888; Service logs unavailable — showing task status (docker service ps)</div>`;
    html += `<table style="width: 100%; border-collapse: collapse; color: #e0e0e0;">`;
    html += `<thead><tr style="border-bottom: 1px solid #444; text-align: left;">`;
    html += `<th style="padding: 6px 10px;">ID</th>`;
    html += `<th style="padding: 6px 10px;">Node</th>`;
    html += `<th style="padding: 6px 10px;">Desired State</th>`;
    html += `<th style="padding: 6px 10px;">Current State</th>`;
    html += `<th style="padding: 6px 10px;">Error</th>`;
    html += `<th style="padding: 6px 10px;">Updated</th>`;
    html += `</tr></thead><tbody>`;
    
    for (const task of tasks) {
        const stateColor = stateColors[task.state] || '#999';
        const desiredColor = stateColors[task.desired_state] || '#999';
        const errorText = task.error || task.message || '';
        const updatedAt = task.updated_at ? new Date(task.updated_at).toLocaleString() : '';
        const taskIdShort = (task.id || '').substring(0, 12);
        
        html += `<tr style="border-bottom: 1px solid #333;">`;
        html += `<td style="padding: 6px 10px; font-family: monospace; font-size: 12px;">${escapeHtml(taskIdShort)}</td>`;
        html += `<td style="padding: 6px 10px;">${escapeHtml(task.node || '')}</td>`;
        html += `<td style="padding: 6px 10px; color: ${desiredColor};">${escapeHtml(task.desired_state || '')}</td>`;
        html += `<td style="padding: 6px 10px; color: ${stateColor};">${escapeHtml(task.state || '')}</td>`;
        html += `<td style="padding: 6px 10px; color: #f44336; max-width: 400px; word-break: break-word;">${escapeHtml(errorText)}</td>`;
        html += `<td style="padding: 6px 10px; white-space: nowrap;">${escapeHtml(updatedAt)}</td>`;
        html += `</tr>`;
    }
    
    html += `</tbody></table></div>`;
    logViewer.innerHTML = html;
}

function formatLogTimestamp(ts) {
    try {
        const d = new Date(ts);
        if (isNaN(d.getTime())) return ts.substring(0, 23);
        return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 });
    } catch {
        return ts.substring(0, 23);
    }
}

function filterServiceLogs() {
    renderServiceLogs();
}

function toggleServiceLogsAutoRefresh() {
    const checked = document.getElementById('service-logs-auto-refresh').checked;
    if (checked) {
        startServiceLogsAutoRefresh();
    } else {
        stopServiceLogsAutoRefresh();
    }
}

function startServiceLogsAutoRefresh() {
    stopServiceLogsAutoRefresh();
    serviceLogsInterval = setInterval(refreshServiceLogs, 3000);
}

function stopServiceLogsAutoRefresh() {
    if (serviceLogsInterval) {
        clearInterval(serviceLogsInterval);
        serviceLogsInterval = null;
    }
}

// ============== Logs Search ==============

// Store last search params for pagination
let lastSearchParams = null;

async function executeSearchWithParams(params) {
    const searchQuery = {
        ...params,
        size: logsPageSize,
        from: logsPage * logsPageSize,
    };
    
    const result = await apiPost('/logs/search', searchQuery);
    if (!result) return;
    
    totalLogs = result.total;
    displayLogsResults(result.hits);
    updatePagination();
}

function updatePagination() {
    const totalPages = Math.ceil(totalLogs / logsPageSize);
    document.getElementById('page-info').textContent = `Page ${logsPage + 1} of ${totalPages || 1}`;
    document.getElementById('prev-page').disabled = logsPage === 0;
    document.getElementById('next-page').disabled = logsPage >= totalPages - 1;
}

function prevPage() {
    if (logsPage > 0 && lastSearchParams) {
        logsPage--;
        executeSearchWithParams(lastSearchParams);
    }
}

function nextPage() {
    const totalPages = Math.ceil(totalLogs / logsPageSize);
    if (logsPage < totalPages - 1 && lastSearchParams) {
        logsPage++;
        executeSearchWithParams(lastSearchParams);
    }
}

function exportLogs() {
    // Simple CSV export
    const table = document.querySelector('.logs-table');
    const rows = Array.from(table.querySelectorAll('tr'));
    
    const csv = rows.map(row => {
        const cells = Array.from(row.querySelectorAll('th, td'));
        return cells.map(cell => `"${cell.textContent.replace(/"/g, '""')}"`).join(',');
    }).join('\n');
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `logs-export-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}

// ============== Stacks (GitHub Integration) ==============

let stacksRepos = [];
let stacksDeployedTags = {};
let stacksLatestBuilt = {};
let stacksPipelineState = {};
let stacksAutoBuildState = {};
let stacksBuildable = {};  // {repo_name: bool} — whether compose has build: directives

async function loadStacks() {
    // Check GitHub status
    const status = await apiGet('/stacks/status');
    _updateGitHubBadge(status);

    if (!status || !status.configured) {
        document.getElementById('stacks-list').innerHTML = `
            <div class="stacks-not-configured">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="48" height="48">
                    <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22"/>
                </svg>
                <h3>GitHub Integration Not Configured</h3>
                <p>Set the following environment variables to enable:</p>
                <code>PULSARCD_GITHUB__TOKEN</code><br>
                <code>PULSARCD_GITHUB__USERNAME</code>
            </div>
        `;
        stopStacksPolling();
        return;
    }
    
    // Load starred repos (full render), then start polling
    await refreshStacks();
    startStacksPolling();
}

async function refreshStacks() {
    const listEl = document.getElementById('stacks-list');
    listEl.innerHTML = '<div class="loading-placeholder">Loading starred repositories...</div>';
    
    // Load repos, deployed tags, pipeline state, auto-build state, and containers in parallel
    const [reposData, tagsData, containersData, hostMetrics, pipelineData, autoBuildData, buildableData] = await Promise.all([
        apiGet('/stacks/repos'),
        apiGet('/stacks/deployed-tags'),
        apiGet('/containers/grouped?refresh=true&group_by=stack'),
        apiGet('/hosts/metrics'),
        apiGet('/stacks/pipeline/status'),
        apiGet('/stacks/auto-build/status'),
        apiGet('/stacks/buildable?refresh=true'),
    ]);

    if (!reposData || !reposData.repos) {
        listEl.innerHTML = '<div class="error-placeholder">Failed to load repositories</div>';
        return;
    }

    stacksRepos = reposData.repos;
    stacksDeployedTags = (tagsData && tagsData.tags) ? tagsData.tags : {};
    stacksLatestBuilt = (tagsData && tagsData.latest_built) ? tagsData.latest_built : {};
    stacksContainers = containersData || {};
    stacksHostMetrics = hostMetrics || {};
    stacksPipelineState = (pipelineData && pipelineData.pipelines) ? pipelineData.pipelines : {};
    stacksAutoBuildState = (autoBuildData && autoBuildData.state) ? autoBuildData.state : {};
    stacksBuildable = buildableData || {};
    
    if (stacksRepos.length === 0) {
        listEl.innerHTML = '<div class="empty-placeholder">No starred repositories found</div>';
        return;
    }
    
    renderStacksList();
}

// Storage for stack containers and metrics
let stacksContainers = {};
let stacksHostMetrics = {};

// ============== Stacks Polling (incremental container state updates) ==============

let stacksPollingInterval = null;
const STACKS_POLL_INTERVAL = 5000; // 5 seconds
let _versionPollCounter = 0;
const VERSION_POLL_EVERY = 6; // Re-fetch versions every 6th poll (~30s)

function startStacksPolling() {
    stopStacksPolling();
    stacksPollingInterval = setInterval(updateStacksContainerStates, STACKS_POLL_INTERVAL);
}

function stopStacksPolling() {
    if (stacksPollingInterval) {
        clearInterval(stacksPollingInterval);
        stacksPollingInterval = null;
    }
}

/**
 * Schedule an accelerated state update after an action (deploy, remove, etc.).
 * Polls a few times rapidly then resumes normal polling.
 */
function scheduleStacksStateUpdate() {
    stopStacksPolling();
    let rapidCount = 0;
    const maxRapidPolls = 6; // 6 rapid polls over ~12 seconds
    const rapidInterval = 2000;

    async function rapidPoll() {
        await updateStacksContainerStates();
        rapidCount++;
        if (rapidCount >= maxRapidPolls) {
            startStacksPolling(); // resume normal interval
        } else {
            setTimeout(rapidPoll, rapidInterval);
        }
    }
    setTimeout(rapidPoll, rapidInterval);
}

/**
 * Incremental update: fetches container data grouped by stack (same endpoint
 * as refreshStacks) plus pipeline/version metadata, then updates DOM in-place.
 */
async function updateStacksContainerStates() {
    if (currentView !== 'stacks') return;
    if (!stacksRepos || stacksRepos.length === 0) return;

    try {
        // Always fetch pipeline status (for cross-browser sync)
        // Periodically re-fetch version info to detect new builds
        _versionPollCounter++;
        const fetchVersions = (_versionPollCounter % VERSION_POLL_EVERY === 0);

        const promises = [
            apiGet('/containers/grouped?refresh=true&group_by=stack'),
            apiGet('/hosts/metrics'),
            apiGet('/stacks/pipeline/status'),
            apiGet('/stacks/auto-build/status'),
        ];
        if (fetchVersions) {
            promises.push(apiGet('/stacks/deployed-tags'));
        }

        const results = await Promise.all(promises);
        const containersData = results[0];
        const hostMetrics = results[1];
        const pipelineData = results[2];
        const autoBuildData = results[3];
        const tagsData = fetchVersions ? results[4] : null;

        if (!containersData) return;
        if (hostMetrics) stacksHostMetrics = hostMetrics;

        // Update version + pipeline data if fetched
        let needsRerender = false;
        if (tagsData) {
            const oldDeployed = JSON.stringify(stacksDeployedTags);
            const oldBuilt = JSON.stringify(stacksLatestBuilt);
            stacksDeployedTags = (tagsData.tags) ? tagsData.tags : {};
            stacksLatestBuilt = (tagsData.latest_built) ? tagsData.latest_built : {};
            if (JSON.stringify(stacksDeployedTags) !== oldDeployed || JSON.stringify(stacksLatestBuilt) !== oldBuilt) {
                needsRerender = true;
            }
        }
        if (pipelineData) {
            const oldPipeline = JSON.stringify(stacksPipelineState);
            stacksPipelineState = pipelineData.pipelines || {};
            if (JSON.stringify(stacksPipelineState) !== oldPipeline) {
                needsRerender = true;
            }
        }
        if (autoBuildData && autoBuildData.state) {
            const oldAutoBuild = JSON.stringify(stacksAutoBuildState);
            stacksAutoBuildState = autoBuildData.state;
            if (JSON.stringify(stacksAutoBuildState) !== oldAutoBuild) {
                needsRerender = true;
            }
        }

        // Update stacksContainers from server-grouped data (same source as refreshStacks)
        for (const repo of stacksRepos) {
            const stackName = repoToStackName(repo.name);
            const newStackContainers = containersData[stackName] || {};

            // If this stack had no entry in the response at all but had containers before,
            // keep the old data — it's likely a transient fetch failure, not a real removal
            if (!(stackName in containersData) && Object.keys(stacksContainers[stackName] || {}).length > 0) {
                continue;
            }

            stacksContainers[stackName] = newStackContainers;
        }

        if (needsRerender) {
            renderStacksList();
            return;
        }

        // Incremental DOM update for each stack
        for (const repo of stacksRepos) {
            const stackName = repoToStackName(repo.name);
            updateStackDom(repo.name, stackName, stacksContainers[stackName] || {});
        }
    } catch (e) {
        console.error('Failed to update stacks container states:', e);
    }
}

/**
 * Update a single stack's DOM in-place without full re-render.
 */
function updateStackDom(repoName, stackName, services) {
    const hostGroupEl = document.querySelector(`[data-repo="${repoName}"].host-group`);
    if (!hostGroupEl) return;

    // Update stack-level summary stats in the header
    let stackTotalMemory = 0;
    let stackMaxCpu = 0;
    let containerCount = 0;

    for (const containers of Object.values(services)) {
        for (const c of containers) {
            containerCount++;
            if (c.memory_usage_mb != null) stackTotalMemory += c.memory_usage_mb;
            if (c.cpu_percent != null && c.cpu_percent > stackMaxCpu) stackMaxCpu = c.cpu_percent;
        }
    }

    // Compute GPU stats
    let maxGpuPercent = null;
    let totalVramUsed = 0;
    let totalVramTotal = 0;
    let hasVramData = false;
    if (stacksHostMetrics) {
        const hostsInStack = new Set();
        for (const containers of Object.values(services)) {
            for (const c of containers) { if (c.host) hostsInStack.add(c.host); }
        }
        for (const host of hostsInStack) {
            if (stacksHostMetrics[host]) {
                const gp = stacksHostMetrics[host].gpu_percent;
                if (gp != null) maxGpuPercent = maxGpuPercent != null ? Math.max(maxGpuPercent, gp) : gp;
                const gu = stacksHostMetrics[host].gpu_memory_used_mb;
                const gt = stacksHostMetrics[host].gpu_memory_total_mb;
                if (gu != null && gt != null) { totalVramUsed += gu; totalVramTotal += gt; hasVramData = true; }
            }
        }
    }

    // Update health class on host-header
    const headerEl = hostGroupEl.querySelector('.host-header');
    if (headerEl) {
        const maxGpuForHealth = maxGpuPercent || 0;
        headerEl.classList.remove('health-critical', 'health-warning');
        if (stackMaxCpu >= 80 || maxGpuForHealth >= 80) headerEl.classList.add('health-critical');
        else if (stackMaxCpu >= 50 || maxGpuForHealth >= 50) headerEl.classList.add('health-warning');
    }

    // Update header stats
    const hostNameEl = hostGroupEl.querySelector('.host-name');
    if (hostNameEl) {
        // Update group-count
        const groupCountEl = hostNameEl.querySelector('.group-count');
        if (groupCountEl) {
            groupCountEl.textContent = `${Object.keys(services).length} svc, ${containerCount} ct`;
        }

        // Update tooltip content
        const tooltipContent = hostNameEl.querySelector('.tooltip-content');
        if (tooltipContent) {
            const lines = [];
            if (stackTotalMemory > 0) lines.push(`<div>RAM: ${formatMemory(stackTotalMemory)}</div>`);
            if (stackMaxCpu > 0) lines.push(`<div>CPU: ${stackMaxCpu.toFixed(1)}%</div>`);
            if (maxGpuPercent != null) lines.push(`<div>GPU: ${maxGpuPercent.toFixed(1)}%</div>`);
            if (hasVramData && totalVramTotal > 0) lines.push(`<div>VRAM: ${formatMemory(totalVramUsed)} / ${formatMemory(totalVramTotal)}</div>`);
            tooltipContent.innerHTML = lines.join('');
        }

        // Update health dot
        const healthDot = hostNameEl.querySelector('.health-dot');
        if (healthDot) {
            healthDot.className = 'health-dot';
            const maxGpuForHealth = maxGpuPercent || 0;
            if (stackMaxCpu >= 80 || maxGpuForHealth >= 80) healthDot.classList.add('dot-critical');
            else if (stackMaxCpu >= 50 || maxGpuForHealth >= 50) healthDot.classList.add('dot-warning');
            else healthDot.classList.add('dot-ok');
        }
    }

    // Update each service's containers
    const contentEl = document.getElementById(`stack-containers-${repoName}`);
    if (!contentEl) return;

    const composeGroups = contentEl.querySelectorAll('.compose-group');
    composeGroups.forEach(groupEl => {
        const headerEl = groupEl.querySelector('.compose-header');
        if (!headerEl) return;

        // Find the service name from the header text
        // The display name is shown in the header, we need to match to the service key
        const containerListEl = groupEl.querySelector('.container-list');
        if (!containerListEl) return;

        // Try to identify which service this compose-group corresponds to
        // We look at existing container items for data-container-id or match by service name in text
        let matchedServiceName = null;
        for (const [svcName, containers] of Object.entries(services)) {
            let displayName = svcName;
            if (svcName.startsWith(stackName + '_')) {
                displayName = svcName.substring(stackName.length + 1);
            }
            // Check if header text contains this service name
            const headerText = headerEl.textContent.trim();
            if (headerText.includes(displayName)) {
                matchedServiceName = svcName;
                break;
            }
        }

        if (!matchedServiceName) return;
        const containers = services[matchedServiceName] || [];

        // Update group count
        const countEl = headerEl.querySelector('.group-count');
        if (countEl) countEl.textContent = containers.length;

        // Update service-level stats
        let svcTotalMemory = 0;
        let svcMaxCpu = 0;
        for (const c of containers) {
            if (c.memory_usage_mb != null) svcTotalMemory += c.memory_usage_mb;
            if (c.cpu_percent != null && c.cpu_percent > svcMaxCpu) svcMaxCpu = c.cpu_percent;
        }
        const svcMemEl = headerEl.querySelector('.group-memory');
        if (svcMemEl) {
            svcMemEl.innerHTML = `💾 ${formatMemory(svcTotalMemory)}`;
            svcMemEl.style.display = svcTotalMemory > 0 ? '' : 'none';
        }
        const svcCpuEl = headerEl.querySelector('.group-cpu');
        if (svcCpuEl) {
            svcCpuEl.className = `group-stat group-cpu ${svcMaxCpu >= 80 ? 'cpu-critical' : (svcMaxCpu >= 50 ? 'cpu-warning' : '')}`;
            svcCpuEl.innerHTML = `⚡ ${svcMaxCpu > 0 ? svcMaxCpu.toFixed(1) + '%' : ''}`;
            svcCpuEl.style.display = svcMaxCpu > 0 ? '' : 'none';
        }

        // Update no-replicas badge
        const noReplicasBadge = headerEl.querySelector('.service-no-replicas');
        if (containers.length === 0 && !noReplicasBadge) {
            const span = document.createElement('span');
            span.className = 'service-no-replicas';
            span.textContent = '0 replicas';
            const countSpan = headerEl.querySelector('.group-count');
            if (countSpan) countSpan.after(span);
        } else if (containers.length > 0 && noReplicasBadge) {
            noReplicasBadge.remove();
        }

        // Update individual container items
        updateContainerItems(containerListEl, containers, stackName);
    });
}

/**
 * Update container items within a service's container-list.
 * Updates status dots, CPU/memory stats in-place for existing containers,
 * and adds/removes container items as needed.
 */
// Track consecutive misses per container to avoid flickering on transient failures
const _containerMissCount = {};
const CONTAINER_MISS_THRESHOLD = 3; // Remove after 3 consecutive missed polls

function updateContainerItems(containerListEl, containers, stackName) {
    const existingItems = containerListEl.querySelectorAll('.container-item:not(.container-item-empty)');
    const existingById = {};
    existingItems.forEach(el => {
        // Extract container id from the onclick attribute
        const onclick = el.getAttribute('onclick') || '';
        const match = onclick.match(/openContainer\('[^']*',\s*'([^']*)'/);
        if (match) existingById[match[1]] = el;
    });

    const newIds = new Set(containers.map(c => c.id));

    // Remove containers only after they've been missing for multiple consecutive polls
    for (const [id, el] of Object.entries(existingById)) {
        if (!newIds.has(id)) {
            _containerMissCount[id] = (_containerMissCount[id] || 0) + 1;
            if (_containerMissCount[id] >= CONTAINER_MISS_THRESHOLD) {
                el.remove();
                delete _containerMissCount[id];
            } else {
                // Grey out the container to hint it may be stale
                const statusDot = el.querySelector('.container-status');
                if (statusDot) statusDot.className = 'container-status exited';
            }
        } else {
            delete _containerMissCount[id]; // Reset miss counter
        }
    }

    // Remove empty placeholder if we now have containers
    if (containers.length > 0) {
        const emptyItem = containerListEl.querySelector('.container-item-empty');
        if (emptyItem) emptyItem.remove();
    }

    for (const c of containers) {
        const existingEl = existingById[c.id];
        if (existingEl) {
            // Update status dot
            const statusDot = existingEl.querySelector('.container-status');
            if (statusDot) {
                statusDot.className = `container-status ${c.status}`;
            }

            // Update CPU/memory stats
            const statMinis = existingEl.querySelectorAll('.stat-mini:not(.stat-mini-gpu)');
            if (statMinis.length >= 2) {
                const cpuDisplay = c.cpu_percent != null ? `${c.cpu_percent}%` : '-';
                const memDisplay = c.memory_percent != null
                    ? `${c.memory_percent}%${c.memory_usage_mb ? ` (${c.memory_usage_mb}MB)` : ''}`
                    : '-';
                // CPU stat is first, memory stat is second
                const cpuTextNode = statMinis[0].lastChild;
                if (cpuTextNode) cpuTextNode.textContent = '\n                                    ' + cpuDisplay + '\n                                ';
                const memTextNode = statMinis[1].lastChild;
                if (memTextNode) memTextNode.textContent = '\n                                    ' + memDisplay + '\n                                ';
            }

            // Update GPU stat
            const gpuMini = existingEl.querySelector('.stat-mini-gpu');
            if (c.gpu_memory_used_mb != null) {
                const gpuMemDisplay = formatMemory(c.gpu_memory_used_mb);
                const gpuSmDisplay = c.gpu_percent != null ? `${c.gpu_percent}%` : '';
                if (gpuMini) {
                    gpuMini.title = `GPU${gpuSmDisplay ? ' SM ' + gpuSmDisplay : ''} — VRAM ${gpuMemDisplay}`;
                    const gpuTextNode = gpuMini.lastChild;
                    if (gpuTextNode) gpuTextNode.textContent = '\n                                    ' + gpuMemDisplay + '\n                                ';
                } else {
                    const statsSection = existingEl.querySelector('.container-stats-mini');
                    if (statsSection) {
                        statsSection.insertAdjacentHTML('beforeend', `
                                <span class="stat-mini stat-mini-gpu" title="GPU${gpuSmDisplay ? ' SM ' + gpuSmDisplay : ''} — VRAM ${gpuMemDisplay}">
                                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                                        <rect x="2" y="6" width="20" height="12" rx="2"/>
                                        <path d="M6 10h4v4H6zM14 10h4v4h-4z"/>
                                    </svg>
                                    ${gpuMemDisplay}
                                </span>`);
                    }
                }
            } else if (gpuMini) {
                gpuMini.remove();
            }

            // Show/hide stats section based on running status
            const statsSection = existingEl.querySelector('.container-stats-mini');
            if (c.status === 'running' && !statsSection) {
                // Container became running, would need a re-render for this item
                // For simplicity, leave as-is until next full render
            } else if (c.status !== 'running' && statsSection) {
                statsSection.style.display = 'none';
            } else if (statsSection) {
                statsSection.style.display = '';
            }
        }
        // New containers will appear on next full refresh or can be added here
        // but a structural change (new container appearing) warrants a full re-render
        // which the rapid polling handles after ~12sec anyway
    }

    // If no containers left, show empty placeholder
    if (containers.length === 0) {
        const existing = containerListEl.querySelector('.container-item-empty');
        if (!existing) {
            containerListEl.innerHTML = `
                <div class="container-item container-item-empty">
                    <div class="container-info">
                        <span class="container-status exited"></span>
                        <div>
                            <div class="container-name" style="color: var(--text-muted);">No running containers</div>
                            <div class="container-image">Service has 0 replicas or all tasks failed</div>
                        </div>
                    </div>
                </div>
            `;
        }
    }
}

// Track expanded stacks
const STACKS_EXPANDED_KEY = 'pulsarcd_stacks_expanded';

function getExpandedStacks() {
    try {
        const stored = localStorage.getItem(STACKS_EXPANDED_KEY);
        return stored ? JSON.parse(stored) : {};
    } catch {
        return {};
    }
}

function saveExpandedStacks(expanded) {
    try {
        localStorage.setItem(STACKS_EXPANDED_KEY, JSON.stringify(expanded));
    } catch (e) {
        console.error('Failed to save expanded stacks:', e);
    }
}

function toggleStackExpand(stackName) {
    const expanded = getExpandedStacks();
    expanded[stackName] = !expanded[stackName];
    saveExpandedStacks(expanded);
    
    const hostGroupEl = document.querySelector(`[data-repo="${stackName}"].host-group`);
    const contentEl = document.getElementById(`stack-containers-${stackName}`);
    
    if (hostGroupEl) {
        hostGroupEl.classList.toggle('collapsed', !expanded[stackName]);
    }
    if (contentEl) {
        contentEl.style.display = expanded[stackName] ? 'block' : 'none';
    }
}

function renderStacksList() {
    const listEl = document.getElementById('stacks-list');
    const expandedStacks = getExpandedStacks();
    
    // Update stacks count in topbar
    const deployedCount = Object.keys(stacksDeployedTags).length;
    const countEl = document.getElementById('stacks-count');
    if (countEl) countEl.textContent = `${deployedCount} stack${deployedCount !== 1 ? 's' : ''} deployed`;

    // Use containers-grouped class for similar styling to Computers view
    listEl.className = 'containers-grouped';
    
    listEl.innerHTML = stacksRepos.map(repo => {
        const pipelineData = stacksPipelineState[repo.name];
        const deployedTag = (pipelineData && pipelineData.deployed_version) ? pipelineData.deployed_version : stacksDeployedTags[repo.name];
        const latestBuilt = stacksLatestBuilt[repo.name];
        const hasUpdate = deployedTag && latestBuilt && normalizeVersion(latestBuilt) !== normalizeVersion(deployedTag);
        const isDeployed = !!deployedTag;
        const autoBuild = stacksAutoBuildState[repo.name];
        const untaggedCount = (autoBuild && autoBuild.untagged_commits) || 0;
        // Docker stack names: lowercase, non-alphanumeric → hyphens (mirrors deploy-service.sh)
        const stackName = repoToStackName(repo.name);
        const stackContainers = stacksContainers[stackName] || {};
        const isExpanded = expandedStacks[repo.name] || false;
        
        // Calculate stack-level stats
        let stackTotalMemory = 0;
        let stackMaxCpu = 0;
        let containerCount = 0;
        
        for (const serviceContainers of Object.values(stackContainers)) {
            for (const c of serviceContainers) {
                containerCount++;
                if (c.memory_usage_mb != null) stackTotalMemory += c.memory_usage_mb;
                if (c.cpu_percent != null && c.cpu_percent > stackMaxCpu) stackMaxCpu = c.cpu_percent;
            }
        }
        
        // Calculate GPU stats from host metrics
        let maxGpuPercent = null;
        let totalVramUsed = 0;
        let totalVramTotal = 0;
        let hasVramData = false;

        if (isDeployed && stacksHostMetrics) {
            const hostsInStack = new Set();
            for (const serviceContainers of Object.values(stackContainers)) {
                for (const c of serviceContainers) {
                    if (c.host) hostsInStack.add(c.host);
                }
            }
            
            for (const host of hostsInStack) {
                if (stacksHostMetrics[host]) {
                    const gpuPercent = stacksHostMetrics[host].gpu_percent;
                    const gpuMemUsed = stacksHostMetrics[host].gpu_memory_used_mb;
                    const gpuMemTotal = stacksHostMetrics[host].gpu_memory_total_mb;
                    
                    if (gpuPercent != null) {
                        maxGpuPercent = maxGpuPercent != null ? Math.max(maxGpuPercent, gpuPercent) : gpuPercent;
                    }
                    if (gpuMemUsed != null && gpuMemTotal != null) {
                        totalVramUsed += gpuMemUsed;
                        totalVramTotal += gpuMemTotal;
                        hasVramData = true;
                    }
                }
            }
            
        }

        const stackMemoryDisplay = isDeployed && stackTotalMemory > 0 ? formatMemory(stackTotalMemory) : '';
        const stackCpuDisplay = isDeployed && stackMaxCpu > 0 ? `${stackMaxCpu.toFixed(1)}%` : '';

        // Health-based background class
        const maxGpuForHealth = maxGpuPercent || 0;
        const healthClass = (stackMaxCpu >= 80 || maxGpuForHealth >= 80) ? 'health-critical'
                          : (stackMaxCpu >= 50 || maxGpuForHealth >= 50) ? 'health-warning' : '';

        // Pipeline step states
        const pipeline = stacksPipelineState[repo.name];
        const stageOrder = { build: 1, test: 2, deploy: 3, done: 4 };
        const isBuildable = stacksBuildable[repo.name] !== false;  // default true if unknown
        let versionStep = 'idle', buildStep = isBuildable ? 'idle' : 'skipped', testStep = 'idle', deployStep = 'idle';

        // Whether build/test steps were part of this pipeline
        const hadBuild = pipeline ? !!pipeline.build_action_id : false;
        const hadTest = pipeline ? !!pipeline.test_action_id : false;

        // If the pipeline version matches the latest built version, build+test were already done
        // (e.g. manual deploy of a version that went through the full pipeline previously)
        const pipelineVersionNorm = pipeline && pipeline.version ? normalizeVersion(pipeline.version) : null;
        const latestBuiltNorm = latestBuilt ? normalizeVersion(latestBuilt) : null;
        const versionAlreadyBuilt = !!(latestBuiltNorm && pipelineVersionNorm && pipelineVersionNorm === latestBuiltNorm);
        const effectiveHadBuild = hadBuild || versionAlreadyBuilt;
        const effectiveHadTest = hadTest || versionAlreadyBuilt;

        const skipBuild = !isBuildable || (pipeline && pipeline.skip_build);
        if (pipeline && pipeline.status === 'running') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (cs === 1 ? 'running' : (effectiveHadBuild && cs > 1 ? 'success' : (cs > 1 ? 'idle' : 'pending')));
            testStep = cs === 2 ? 'running' : (effectiveHadTest && cs > 2 ? 'success' : (cs > 2 ? 'idle' : 'pending'));
            deployStep = cs === 3 ? 'running' : 'pending';
        } else if (pipeline && pipeline.status === 'gate_rejected') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (cs >= 1 ? 'success' : 'pending');
            testStep = cs === 1 ? 'gate_rejected' : (cs >= 2 ? 'success' : 'pending');
            deployStep = cs === 2 ? 'gate_rejected' : 'pending';
        } else if (pipeline && pipeline.status === 'failed') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (cs === 1 ? 'failed' : (effectiveHadBuild && cs > 1 ? 'success' : (cs > 1 ? 'idle' : 'pending')));
            testStep = cs === 2 ? 'failed' : (effectiveHadTest && cs > 2 ? 'success' : (cs > 2 ? 'idle' : 'pending'));
            deployStep = cs === 3 ? 'failed' : (cs > 3 ? 'success' : 'pending');
        } else if (pipeline && pipeline.stage === 'done') {
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (effectiveHadBuild ? 'success' : 'idle');
            // Use actual per-stage status: if test failed but deploy was forced, show warning
            const testStageStatus = pipeline.stages && pipeline.stages.test ? pipeline.stages.test.status : null;
            testStep = testStageStatus === 'failed' ? 'warning' : (effectiveHadTest ? 'success' : 'idle');
            deployStep = 'success';
        } else if (pipeline && pipeline.status === 'success') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (effectiveHadBuild && cs >= 1 ? 'success' : (cs >= 1 ? 'idle' : 'pending'));
            testStep = effectiveHadTest && cs >= 2 ? 'success' : (cs >= 2 ? 'idle' : 'pending');
            deployStep = cs >= 3 ? 'success' : 'pending';
        } else if (hasUpdate) {
            versionStep = 'success'; buildStep = skipBuild ? 'skipped' : 'success'; testStep = 'success'; deployStep = 'pending';
        } else if (untaggedCount > 0) {
            versionStep = 'pending'; buildStep = skipBuild ? 'skipped' : 'pending'; testStep = 'pending'; deployStep = 'pending';
        } else if (isDeployed) {
            versionStep = 'success'; buildStep = skipBuild ? 'skipped' : 'success'; testStep = 'success'; deployStep = 'success';
        }

        const pipelineVersion = (pipeline && pipeline.version) ? pipeline.version : (latestBuilt ? normalizeVersion(latestBuilt) : (deployedTag || '–'));
        const buildActionId = pipeline ? pipeline.build_action_id : null;
        const testActionId = pipeline ? pipeline.test_action_id : null;
        const deployActionId = pipeline ? pipeline.deploy_action_id : null;

        // Build containers HTML (similar to Computers view compose-group style)
        let containersHtml = '';
        const serviceCount = Object.keys(stackContainers).length;
        if (isDeployed && serviceCount > 0) {
            containersHtml = `<div class="host-content" id="stack-containers-${escapeHtml(repo.name)}" style="display: ${isExpanded ? 'block' : 'none'};">`;
            
            for (const [serviceName, containers] of Object.entries(stackContainers)) {
                // serviceName is the full swarm service name (e.g., "pulsarcd_backend")
                // Extract short display name by removing the stack prefix
                let displayServiceName = serviceName;
                if (serviceName === '_standalone') {
                    displayServiceName = 'Standalone';
                } else if (serviceName.startsWith(stackName + '_')) {
                    displayServiceName = serviceName.substring(stackName.length + 1);
                }
                const hasContainers = containers.length > 0;
                
                // Calculate service stats
                let serviceTotalMemory = 0;
                let serviceMaxCpu = 0;
                for (const c of containers) {
                    if (c.memory_usage_mb != null) serviceTotalMemory += c.memory_usage_mb;
                    if (c.cpu_percent != null && c.cpu_percent > serviceMaxCpu) serviceMaxCpu = c.cpu_percent;
                }
                const serviceMemoryDisplay = serviceTotalMemory > 0 ? formatMemory(serviceTotalMemory) : '';
                const serviceCpuClass = serviceMaxCpu >= 80 ? 'cpu-critical' : (serviceMaxCpu >= 50 ? 'cpu-warning' : '');
                const serviceCpuDisplay = serviceMaxCpu > 0 ? `${serviceMaxCpu.toFixed(1)}%` : '';
                
                // serviceName is already the full swarm service name
                const fullServiceName = serviceName;
                // Get image from first container for deploy modal
                const firstContainerImage = containers.length > 0 ? containers[0].image : '';
                
                containersHtml += `
                    <div class="compose-group${hasContainers ? '' : ' compose-group-empty'}">
                        <div class="compose-header">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                            </svg>
                            ${escapeHtml(displayServiceName)}
                            <span class="group-count">${containers.length}</span>
                            ${!hasContainers ? '<span class="service-no-replicas">0 replicas</span>' : ''}
                            ${serviceMemoryDisplay ? `<span class="group-stat group-memory" title="Total memory usage">💾 ${serviceMemoryDisplay}</span>` : ''}
                            ${serviceCpuDisplay ? `<span class="group-stat group-cpu ${serviceCpuClass}" title="Max CPU usage">⚡ ${serviceCpuDisplay}</span>` : ''}
                            <button class="btn btn-sm btn-ghost service-logs-btn" onclick="event.stopPropagation(); openServiceLogs('${escapeHtml(fullServiceName)}')" title="View service logs">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                                    <polyline points="14 2 14 8 20 8"/>
                                    <line x1="16" y1="13" x2="8" y2="13"/>
                                    <line x1="16" y1="17" x2="8" y2="17"/>
                                    <polyline points="10 9 9 9 8 9"/>
                                </svg>
                                Logs
                            </button>
                            <button class="btn btn-sm btn-ghost service-status-btn" onclick="event.stopPropagation(); openServiceStatus('${escapeHtml(fullServiceName)}')" title="Service task status (docker service ps)">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                    <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
                                </svg>
                                Status
                            </button>
                            <button class="btn btn-sm btn-primary service-deploy-btn" onclick="event.stopPropagation(); openServiceDeploy('${escapeHtml(fullServiceName)}', '${escapeHtml(repo.name)}', '${escapeHtml(repo.ssh_url)}', '${escapeHtml(firstContainerImage)}')" title="Deploy new version">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                    <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                                </svg>
                                Deploy
                            </button>
                            <button class="btn btn-sm btn-danger service-remove-btn" onclick="event.stopPropagation(); removeService('${escapeHtml(fullServiceName)}')" title="Remove service">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                    <polyline points="3 6 5 6 21 6"/>
                                    <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                                </svg>
                                Remove
                            </button>
                        </div>
                        <div class="compose-content">
                            <div class="container-list">
                `;
                
                if (!hasContainers) {
                    containersHtml += `
                        <div class="container-item container-item-empty">
                            <div class="container-info">
                                <span class="container-status exited"></span>
                                <div>
                                    <div class="container-name" style="color: var(--text-muted);">No running containers</div>
                                    <div class="container-image">Service has 0 replicas or all tasks failed</div>
                                </div>
                            </div>
                        </div>
                    `;
                }
                
                for (const c of containers) {
                    const cpuDisplay = c.cpu_percent != null ? `${c.cpu_percent}%` : '-';
                    const memDisplay = c.memory_percent != null 
                        ? `${c.memory_percent}%${c.memory_usage_mb ? ` (${c.memory_usage_mb}MB)` : ''}`
                        : '-';
                    const containerAge = c.created ? formatTimeAgo(c.created) : '';

                    // Per-container GPU stats
                    let gpuMiniHtml = '';
                    if (c.gpu_memory_used_mb != null) {
                        const gpuSmDisplay = c.gpu_percent != null ? `${c.gpu_percent}%` : '';
                        const gpuMemDisplay = formatMemory(c.gpu_memory_used_mb);
                        gpuMiniHtml = `
                                <span class="stat-mini stat-mini-gpu" title="GPU${gpuSmDisplay ? ' SM ' + gpuSmDisplay : ''} — VRAM ${gpuMemDisplay}">
                                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                                        <rect x="2" y="6" width="20" height="12" rx="2"/>
                                        <path d="M6 10h4v4H6zM14 10h4v4h-4z"/>
                                    </svg>
                                    ${gpuMemDisplay}
                                </span>`;
                    }
                    
                    containersHtml += `
                        <div class="container-item" onclick="openContainer('${escapeHtml(c.host)}', '${escapeHtml(c.id)}', ${JSON.stringify(c).replace(/"/g, '&quot;')})">
                            <div class="container-info">
                                <span class="container-status ${c.status}"></span>
                                <div>
                                    <div class="container-name">${escapeHtml(c.name)} <span style="color: var(--text-muted); font-size: 0.85em;">(${escapeHtml(c.host)})</span></div>
                                    <div class="container-image">${formatImageName(c.image)}${containerAge ? ` <span class="container-age">• ${containerAge}</span>` : ''}</div>
                                </div>
                            </div>
                            ${c.status === 'running' ? `
                            <div class="container-stats-mini">
                                <span class="stat-mini" title="CPU %">
                                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                                        <rect x="4" y="4" width="16" height="16" rx="2"/>
                                        <rect x="9" y="9" width="6" height="6"/>
                                        <path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 15h3M1 9h3M1 15h3"/>
                                    </svg>
                                    ${cpuDisplay}
                                </span>
                                <span class="stat-mini" title="RAM">
                                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                                        <path d="M2 20h20M6 16V8a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v8"/>
                                    </svg>
                                    ${memDisplay}
                                </span>${gpuMiniHtml}
                            </div>
                            ` : ''}
                            <div class="container-actions">
                                <button class="btn btn-sm btn-secondary" onclick="event.stopPropagation(); quickAction('${escapeHtml(c.host)}', '${escapeHtml(c.id)}', 'restart', '${escapeHtml(c.name)}')">
                                    Restart
                                </button>
                            </div>
                        </div>
                    `;
                }
                
                containersHtml += `
                            </div>
                        </div>
                    </div>
                `;
            }
            
            containersHtml += `</div>`;
        }
        
        // Stack icon
        const stackIcon = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
        </svg>`;
        
        // SVG icons for pipeline steps
        const checkSvg = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="12" height="12"><polyline points="20 6 9 17 4 12"/></svg>`;
        const xSvg = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="12" height="12"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>`;
        const spinnerSvg = `<svg class="pipeline-spinner" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="12" height="12"><path d="M12 2a10 10 0 0 1 10 10"/></svg>`;

        const idleSvg = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><circle cx="12" cy="12" r="10"/><polygon points="10 8 16 12 10 16 10 8" fill="currentColor" stroke="none"/></svg>`;
        const pendingSvg = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`;
        const gateSvg = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="12" height="12"><circle cx="12" cy="12" r="10"/><line x1="8" y1="12" x2="16" y2="12"/></svg>`;
        const warningSvg = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="12" height="12"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`;
        const stepIcon = (state) => state === 'success' ? checkSvg : state === 'failed' ? xSvg : state === 'warning' ? warningSvg : state === 'gate_rejected' ? gateSvg : state === 'running' ? spinnerSvg : state === 'pending' ? pendingSvg : idleSvg;

        // Gate arrow coloring: green if gate approved (next step running/success), red if rejected
        function _gateArrowClass(pipeline, fromStage, fromStep, toStep) {
            if (!pipeline) return '';
            if (toStep === 'gate_rejected') return 'arrow-rejected';
            // If from step succeeded and to step is running/success/failed → gate approved
            if (fromStep === 'success' && (toStep === 'running' || toStep === 'success' || toStep === 'failed')) return 'arrow-approved';
            return '';
        }
        // Transition mode CSS class for arrow indicator
        function _transitionModeClass(pipeline, transition) {
            if (!pipeline || !pipeline.transition_configs) return '';
            const cfg = pipeline.transition_configs[transition];
            if (!cfg || !cfg.mode) return '';
            return ' transition-mode-' + cfg.mode;
        }
        // Transition mode badge label
        function _transitionModeIcon(pipeline, transition) {
            if (!pipeline || !pipeline.transition_configs) return '';
            const cfg = pipeline.transition_configs[transition];
            if (!cfg || !cfg.mode || cfg.mode === 'auto_with_success') return '';
            if (cfg.mode === 'manual') return '<span class="transition-badge badge-manual">Manual</span>';
            if (cfg.mode === 'agent') return '<span class="transition-badge badge-agent">AI</span>';
            if (cfg.mode === 'auto') return '<span class="transition-badge badge-auto">Auto</span>';
            return '';
        }
        // Find gate decision for a transition
        function _gateDecision(pipeline, transition) {
            if (!pipeline || !pipeline.gates) return null;
            // Find most recent decision for this transition
            for (let i = pipeline.gates.length - 1; i >= 0; i--) {
                if (pipeline.gates[i].transition === transition) return pipeline.gates[i];
            }
            return null;
        }
        const gateVersionBuild = _gateDecision(pipeline, 'version_to_build');
        const gateBuildTest = _gateDecision(pipeline, 'build_to_test');
        const gateTestDeploy = _gateDecision(pipeline, 'test_to_deploy');

        // Monitoring tooltip content
        const tooltipLines = [];
        if (stackMemoryDisplay) tooltipLines.push(`RAM: ${stackMemoryDisplay}`);
        if (stackCpuDisplay) tooltipLines.push(`CPU: ${stackCpuDisplay}`);
        if (maxGpuPercent != null) tooltipLines.push(`GPU: ${maxGpuPercent.toFixed(1)}%`);
        if (hasVramData && totalVramTotal > 0) tooltipLines.push(`VRAM: ${formatMemory(totalVramUsed)} / ${formatMemory(totalVramTotal)}`);

        // Use host-group structure similar to Computers view
        return `
        <div class="host-group ${isExpanded ? '' : 'collapsed'}" data-repo="${escapeHtml(repo.name)}">
            <div class="host-header ${healthClass}" ${isDeployed ? `onclick="toggleStackExpand('${escapeHtml(repo.name)}')"` : ''}>
                <span class="host-name">
                    ${isDeployed ? `
                    <svg class="chevron-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="6 9 12 15 18 9"/>
                    </svg>
                    ` : ''}
                    ${stackIcon}
                    ${escapeHtml(repo.name)}
                    ${deployedTag ? `<span class="stack-badge deployed" title="Deployed version">${escapeHtml(deployedTag)}</span>` : '<span class="stack-badge" style="background: var(--bg-tertiary); color: var(--text-muted);">Not deployed</span>'}
                    ${pipeline && pipeline.last_deployed_at ? `<span class="stack-deployed-ago" title="${new Date(pipeline.last_deployed_at).toLocaleString()}">${formatTimeAgo(pipeline.last_deployed_at)}</span>` : ''}
                    ${hasUpdate ? `<span class="stack-badge update-available" title="New version available">${escapeHtml(latestBuilt)}</span>` : ''}
                    ${isDeployed ? `<span class="group-count">${Object.keys(stackContainers).length} svc, ${containerCount} ct</span>` : ''}
                    ${isDeployed && tooltipLines.length > 0 ? `
                    <span class="stack-monitoring-tooltip">
                        <span class="health-dot ${healthClass ? (healthClass === 'health-critical' ? 'dot-critical' : 'dot-warning') : 'dot-ok'}"></span>
                        <span class="tooltip-content">${tooltipLines.map(l => `<div>${l}</div>`).join('')}</span>
                    </span>` : ''}
                </span>
                <div class="host-header-actions" onclick="event.stopPropagation();">
                    <a class="btn btn-sm btn-ghost" href="${escapeHtml(repo.html_url)}" target="_blank" rel="noopener" title="Open on GitHub">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22"/>
                        </svg>
                        <span>GitHub</span>
                    </a>
                    <button class="btn btn-sm btn-ghost" onclick="showStackActivity('${escapeHtml(repo.owner)}', '${escapeHtml(repo.name)}')" title="View git activity">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <circle cx="18" cy="18" r="3"/>
                            <circle cx="6" cy="6" r="3"/>
                            <path d="M6 21V9a9 9 0 0 0 9 9"/>
                        </svg>
                        <span>Activity</span>
                    </button>
                    <button class="btn btn-sm btn-ghost" onclick="editStackEnv('${escapeHtml(repo.name)}')" title="Edit .env file">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                        </svg>
                        <span>Env</span>
                    </button>

                    <div class="pipeline-flow">
                        <div class="pipeline-step step-${versionStep}" onclick="event.stopPropagation(); pipelineStepClick('${escapeHtml(repo.name)}', '${escapeHtml(repo.ssh_url)}', 'version')" title="Version: ${escapeHtml(pipelineVersion)} (click to run full pipeline)" style="cursor:pointer">
                            ${stepIcon(versionStep)}
                            <span>${escapeHtml(pipelineVersion)}</span>
                        </div>
                        <span class="pipeline-transition-btn ${_gateArrowClass(pipeline, 'version', versionStep, buildStep)}${gateVersionBuild ? ' has-gate' : ''}${_transitionModeClass(pipeline, 'version_to_build')}" onclick="event.stopPropagation(); openTransitionConfig('${escapeHtml(repo.name)}', 'version_to_build')" title="Version → Build transition (click to configure)">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
                            ${_transitionModeIcon(pipeline, 'version_to_build')}
                        </span>
                        <div class="pipeline-step step-${buildStep}" ${skipBuild ? 'title="Build: skipped (no build config)"' : `onclick="event.stopPropagation(); pipelineStepClick('${escapeHtml(repo.name)}', '${escapeHtml(repo.ssh_url)}', 'build')" title="Build" style="cursor:pointer"`}>
                            ${skipBuild ? `<span class="step-icon">–</span>` : stepIcon(buildStep)}
                            <span>Build</span>
                            ${!skipBuild && buildActionId ? `<span class="pipeline-log-btn" onclick="event.stopPropagation(); openActionLogs('${buildActionId}', 'Build Logs', '${escapeHtml(repo.name)}')" title="View build logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                        </div>
                        <span class="pipeline-transition-btn ${_gateArrowClass(pipeline, 'build', buildStep, testStep)}${gateBuildTest ? ' has-gate' : ''}${_transitionModeClass(pipeline, 'build_to_test')}" onclick="event.stopPropagation(); openTransitionConfig('${escapeHtml(repo.name)}', 'build_to_test')" title="Build → Test transition (click to configure)">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
                            ${_transitionModeIcon(pipeline, 'build_to_test')}
                        </span>
                        <div class="pipeline-step step-${testStep}" onclick="event.stopPropagation(); pipelineStepClick('${escapeHtml(repo.name)}', '${escapeHtml(repo.ssh_url)}', 'test')" title="Test">
                            ${stepIcon(testStep)}
                            <span>Test</span>
                            ${testActionId ? `<span class="pipeline-log-btn" onclick="event.stopPropagation(); openActionLogs('${testActionId}', 'Test Logs', '${escapeHtml(repo.name)}')" title="View test logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                        </div>
                        <span class="pipeline-transition-btn ${_gateArrowClass(pipeline, 'test', testStep, deployStep)}${gateTestDeploy ? ' has-gate' : ''}${_transitionModeClass(pipeline, 'test_to_deploy')}" onclick="event.stopPropagation(); openTransitionConfig('${escapeHtml(repo.name)}', 'test_to_deploy')" title="Test → Deploy transition (click to configure)">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
                            ${_transitionModeIcon(pipeline, 'test_to_deploy')}
                        </span>
                        <div class="pipeline-step step-${deployStep}" onclick="event.stopPropagation(); pipelineStepClick('${escapeHtml(repo.name)}', '${escapeHtml(repo.ssh_url)}', 'deploy')" title="Deploy">
                            ${stepIcon(deployStep)}
                            <span>Deploy</span>
                            ${deployActionId ? `<span class="pipeline-log-btn" onclick="event.stopPropagation(); openActionLogs('${deployActionId}', 'Deploy Logs', '${escapeHtml(repo.name)}')" title="View deploy logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                        </div>
                    </div>

                    ${isDeployed ? `
                    <button class="btn btn-sm btn-danger" onclick="removeDeployedStack('${escapeHtml(repo.name)}')" title="Remove deployed stack">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <polyline points="3 6 5 6 21 6"/>
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                        </svg>
                    </button>
                    ` : ''}
                </div>
            </div>
            ${containersHtml}
        </div>
    `;
    }).join('');
}

// ============== Pipeline Step Click ==============

function pipelineStepClick(repoName, sshUrl, step) {
    const pipeline = stacksPipelineState[repoName];

    // If the step is currently running, show live logs instead of opening the modal
    if (pipeline && pipeline.status === 'running') {
        let actionId = null;
        if (step === 'build' && pipeline.stage === 'build') actionId = pipeline.build_action_id;
        else if (step === 'test' && pipeline.stage === 'test') actionId = pipeline.test_action_id;
        else if (step === 'deploy' && pipeline.stage === 'deploy') actionId = pipeline.deploy_action_id;

        if (actionId) {
            const label = step.charAt(0).toUpperCase() + step.slice(1);
            openActionLogs(actionId, `${label} Logs`, repoName);
            return;
        }
    }

    // Otherwise, always open the action modal (past logs accessible via the log icon button)
    if (step === 'version') pipelineStack(repoName, sshUrl);
    else if (step === 'build') buildStack(repoName, sshUrl);
    else if (step === 'test') testStack(repoName, sshUrl);
    else if (step === 'deploy') deployStack(repoName, sshUrl);
}

// ============== Gate Decision Modal ==============

function showGateDecision(repoName, transition) {
    const pipeline = stacksPipelineState[repoName];
    if (!pipeline || !pipeline.gates) return;

    // Find the decision for this transition
    let decision = null;
    for (let i = pipeline.gates.length - 1; i >= 0; i--) {
        if (pipeline.gates[i].transition === transition) { decision = pipeline.gates[i]; break; }
    }
    if (!decision) return;

    const label = transition === 'build_to_test' ? 'Build → Test' : 'Test → Deploy';
    const ts = decision.timestamp ? new Date(decision.timestamp).toLocaleString() : '';

    const modal = document.getElementById('gate-decision-modal');
    if (!modal) {
        const m = document.createElement('div');
        m.id = 'gate-decision-modal';
        m.className = 'modal';
        m.onclick = (e) => { if (e.target === m) m.classList.remove('active'); };
        m.innerHTML = `
            <div class="modal-content" style="max-width: 800px; width: 90%;">
                <div class="modal-header">
                    <h3 id="gate-decision-title">Gate Decision</h3>
                    <button class="modal-close" onclick="document.getElementById('gate-decision-modal').classList.remove('active');">&times;</button>
                </div>
                <div class="modal-body" style="padding: 20px;">
                    <div id="gate-decision-info" style="display: flex; align-items: center; gap: 16px; margin-bottom: 16px; padding: 12px 16px; background: var(--bg-tertiary); border-radius: 8px; flex-wrap: wrap;">
                        <div id="gate-decision-status" style="font-size: 15px; font-weight: 600;"></div>
                        <div style="width: 1px; height: 20px; background: var(--border-color);"></div>
                        <div id="gate-decision-meta" style="font-size: 13px; color: var(--text-muted); flex: 1;"></div>
                    </div>
                    <div style="margin-bottom: 8px; font-size: 12px; font-weight: 600; text-transform: uppercase; color: var(--text-muted); letter-spacing: 0.5px;">AI Analysis</div>
                    <div id="gate-decision-reason" class="markdown-body" style="font-size: 13px; line-height: 1.6; background: var(--bg-secondary); padding: 16px; border-radius: 8px; max-height: 500px; overflow-y: auto; border: 1px solid var(--border-color);"></div>
                </div>
                <div class="modal-footer">
                    <button class="btn btn-secondary" onclick="document.getElementById('gate-decision-modal').classList.remove('active')">Close</button>
                </div>
            </div>`;
        document.body.appendChild(m);
    }

    const statusIcon = decision.approved
        ? '<span style="color: var(--status-success); display: inline-flex; align-items: center; gap: 6px;"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="16" height="16"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Approved</span>'
        : '<span style="color: var(--status-error); display: inline-flex; align-items: center; gap: 6px;"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="16" height="16"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Rejected</span>';

    document.getElementById('gate-decision-title').textContent = `Gate: ${label}`;
    document.getElementById('gate-decision-meta').innerHTML = `<strong>${escapeHtml(repoName)}</strong>${pipeline.project_name && pipeline.project_name !== repoName ? ` (${escapeHtml(pipeline.project_name)})` : ''}${pipeline.stack_name ? ` &mdash; Stack: <code>${escapeHtml(pipeline.stack_name)}</code>` : ''}${ts ? ` &mdash; <span title="${escapeHtml(decision.timestamp || '')}">${ts}</span>` : ''}`;
    document.getElementById('gate-decision-status').innerHTML = statusIcon;
    document.getElementById('gate-decision-reason').innerHTML = simpleMarkdown(decision.reason || 'No reason provided');
    document.getElementById('gate-decision-modal').classList.add('active');
}

// ============== Transition Config Modal ==============

async function openTransitionConfig(repoName, transition) {
    const label = transition === 'version_to_build' ? 'Version → Build' : transition === 'build_to_test' ? 'Build → Test' : 'Test → Deploy';

    // Create modal if not exists
    let modal = document.getElementById('transition-config-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'transition-config-modal';
        modal.className = 'modal';
        modal.onclick = (e) => { if (e.target === modal) modal.classList.remove('active'); };
        modal.innerHTML = `
            <div class="modal-content" style="max-width: 600px; width: 90%;">
                <div class="modal-header">
                    <h3 id="transition-config-title">Transition Config</h3>
                    <button class="modal-close" onclick="document.getElementById('transition-config-modal').classList.remove('active');">&times;</button>
                </div>
                <div class="modal-body" style="padding: 20px;">
                    <div id="transition-config-loading" class="loading-placeholder">Loading...</div>
                    <div id="transition-config-content" style="display:none;">
                        <div style="margin-bottom: 16px; font-size: 13px; color: var(--text-muted);">
                            Configure how the pipeline transitions between stages for this project.
                        </div>
                        <div class="transition-mode-options" id="transition-mode-options">
                            <label class="transition-mode-option" data-mode="auto">
                                <input type="radio" name="transition-mode" value="auto">
                                <div class="transition-mode-info">
                                    <div class="transition-mode-label">
                                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><polyline points="13 17 18 12 13 7"/><polyline points="6 17 11 12 6 7"/></svg>
                                        Automatic
                                    </div>
                                    <div class="transition-mode-desc">Always proceed to the next stage automatically, regardless of conditions.</div>
                                </div>
                            </label>
                            <label class="transition-mode-option" data-mode="auto_with_success">
                                <input type="radio" name="transition-mode" value="auto_with_success">
                                <div class="transition-mode-info">
                                    <div class="transition-mode-label">
                                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
                                        Automatic with success check
                                    </div>
                                    <div class="transition-mode-desc">Proceed only if the previous stage succeeded. Stop on failure.</div>
                                </div>
                            </label>
                            <label class="transition-mode-option" data-mode="agent">
                                <input type="radio" name="transition-mode" value="agent">
                                <div class="transition-mode-info">
                                    <div class="transition-mode-label">
                                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M12 2a4 4 0 0 1 4 4v1a4 4 0 0 1-8 0V6a4 4 0 0 1 4-4z"/><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="6" r="1"/></svg>
                                        AI Agent gate
                                    </div>
                                    <div class="transition-mode-desc">LLM agent analyzes logs and decides whether to proceed. Uses global gate instructions.</div>
                                </div>
                            </label>
                            <label class="transition-mode-option" data-mode="manual">
                                <input type="radio" name="transition-mode" value="manual">
                                <div class="transition-mode-info">
                                    <div class="transition-mode-label">
                                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
                                        Manual only
                                    </div>
                                    <div class="transition-mode-desc">Pipeline stops. A user must manually trigger the next stage.</div>
                                </div>
                            </label>
                        </div>
                        <div id="transition-ai-logs-section" style="margin-top: 20px; display: none;">
                            <div style="margin-bottom: 8px; font-size: 12px; font-weight: 600; text-transform: uppercase; color: var(--text-muted); letter-spacing: 0.5px;">Last AI Decision</div>
                            <div id="transition-ai-decision-info" style="display: flex; align-items: center; gap: 12px; margin-bottom: 12px; padding: 10px 14px; background: var(--bg-tertiary); border-radius: 8px; flex-wrap: wrap;">
                                <div id="transition-ai-decision-status" style="font-size: 14px; font-weight: 600;"></div>
                                <div style="width: 1px; height: 18px; background: var(--border-color);"></div>
                                <div id="transition-ai-decision-meta" style="font-size: 12px; color: var(--text-muted); flex: 1;"></div>
                            </div>
                            <div id="transition-ai-decision-reason" class="markdown-body" style="font-size: 13px; line-height: 1.6; background: var(--bg-secondary); padding: 14px; border-radius: 8px; max-height: 300px; overflow-y: auto; border: 1px solid var(--border-color);"></div>
                        </div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn btn-secondary" onclick="document.getElementById('transition-config-modal').classList.remove('active')">Cancel</button>
                    <button class="btn btn-primary" id="transition-config-save" onclick="saveTransitionConfig()">Save</button>
                </div>
            </div>`;
        document.body.appendChild(modal);
    }

    // Store context
    modal.dataset.repo = repoName;
    modal.dataset.transition = transition;
    document.getElementById('transition-config-title').textContent = `Transition: ${label}`;
    document.getElementById('transition-config-loading').style.display = '';
    document.getElementById('transition-config-content').style.display = 'none';
    modal.classList.add('active');

    // Fetch current config from API
    try {
        const data = await apiGet(`/stacks/pipeline/${encodeURIComponent(repoName)}/transition/${encodeURIComponent(transition)}`);
        const config = (data && data.config) || {};
        const currentMode = config.mode || 'auto_with_success';
        const lastDecision = data && data.last_decision;

        // Select current mode
        document.querySelectorAll('#transition-mode-options input[name="transition-mode"]').forEach(r => {
            r.checked = (r.value === currentMode);
            r.closest('.transition-mode-option').classList.toggle('selected', r.value === currentMode);
        });

        // Add click handler for mode selection visual feedback
        document.querySelectorAll('#transition-mode-options .transition-mode-option').forEach(opt => {
            opt.onclick = () => {
                const radio = opt.querySelector('input[type="radio"]');
                radio.checked = true;
                document.querySelectorAll('#transition-mode-options .transition-mode-option').forEach(o => o.classList.remove('selected'));
                opt.classList.add('selected');
            };
        });

        // Show AI decision logs if available
        const aiSection = document.getElementById('transition-ai-logs-section');
        if (lastDecision) {
            aiSection.style.display = '';
            const statusIcon = lastDecision.approved
                ? '<span style="color: var(--status-success); display: inline-flex; align-items: center; gap: 5px;"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="14" height="14"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Approved</span>'
                : '<span style="color: var(--status-error); display: inline-flex; align-items: center; gap: 5px;"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Rejected</span>';
            document.getElementById('transition-ai-decision-status').innerHTML = statusIcon;
            const ts = lastDecision.timestamp ? new Date(lastDecision.timestamp).toLocaleString() : '';
            const versionTag = lastDecision.version ? `<span style="background: var(--bg-secondary); padding: 2px 8px; border-radius: 4px; font-family: var(--font-mono, monospace); font-size: 11px; border: 1px solid var(--border-color);">v${escapeHtml(lastDecision.version)}</span>` : '';
            document.getElementById('transition-ai-decision-meta').innerHTML = [versionTag, ts ? `<span>${ts}</span>` : ''].filter(Boolean).join(' <span style="width: 1px; height: 14px; background: var(--border-color); display: inline-block; vertical-align: middle; margin: 0 4px;"></span> ');
            document.getElementById('transition-ai-decision-reason').innerHTML = simpleMarkdown(lastDecision.reason || 'No details available');
        } else {
            aiSection.style.display = 'none';
        }

        document.getElementById('transition-config-loading').style.display = 'none';
        document.getElementById('transition-config-content').style.display = '';
    } catch (err) {
        document.getElementById('transition-config-loading').innerHTML = `<div class="error-placeholder">Failed to load config: ${escapeHtml(err.message || String(err))}</div>`;
    }
}

async function saveTransitionConfig() {
    const modal = document.getElementById('transition-config-modal');
    const repoName = modal.dataset.repo;
    const transition = modal.dataset.transition;
    const selected = document.querySelector('#transition-mode-options input[name="transition-mode"]:checked');
    if (!selected) return;

    const saveBtn = document.getElementById('transition-config-save');
    saveBtn.disabled = true;
    saveBtn.textContent = 'Saving...';

    try {
        await apiPut(`/stacks/pipeline/${encodeURIComponent(repoName)}/transition/${encodeURIComponent(transition)}`, { mode: selected.value });
        modal.classList.remove('active');
        // Refresh stacks view to reflect new config
        if (typeof refreshStacks === 'function') refreshStacks();
    } catch (err) {
        alert('Failed to save: ' + (err.message || err));
    } finally {
        saveBtn.disabled = false;
        saveBtn.textContent = 'Save';
    }
}

// ============== Pipeline Modal (Version → Build → Test → Deploy) ==============

let currentPipelineRepo = null;
let currentPipelineSshUrl = null;
let selectedPipelineTag = null;
let selectedPipelineCommit = null;  // commit SHA for untagged builds

async function pipelineStack(repoName, sshUrl) {
    currentPipelineRepo = repoName;
    currentPipelineSshUrl = sshUrl;
    selectedPipelineTag = null;
    selectedPipelineCommit = null;

    const modal = document.getElementById('stack-pipeline-modal');
    const title = document.getElementById('stack-pipeline-title');
    const tagsList = document.getElementById('pipeline-tags-list');
    const untaggedSection = document.getElementById('pipeline-untagged-section');
    const untaggedList = document.getElementById('pipeline-untagged-list');
    const selectedDisplay = document.getElementById('pipeline-selected-tag');

    title.textContent = `Pipeline: ${repoName}`;
    tagsList.innerHTML = '<div class="loading-placeholder">Loading...</div>';
    untaggedSection.style.display = 'none';
    untaggedList.innerHTML = '';
    selectedDisplay.style.display = 'none';

    modal.classList.add('active');

    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        tagsList.innerHTML = '<div class="error-placeholder">Failed to parse repository URL</div>';
        return;
    }
    const owner = ownerMatch[1];

    // Load tags and untagged commits in parallel
    try {
        const [tagsData, untaggedData] = await Promise.all([
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/tags?limit=20`),
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/untagged-commits?limit=10`),
        ]);

        // Render untagged commits if any
        const untaggedCommits = (untaggedData && untaggedData.untagged_commits) || [];
        if (untaggedCommits.length > 0) {
            untaggedSection.style.display = '';
            document.getElementById('pipeline-untagged-count').textContent = untaggedCommits.length;
            renderPipelineUntaggedList(untaggedCommits);
            // Auto-select the latest untagged commit
            selectPipelineCommit(untaggedCommits[0].sha);
        }

        // Render tags
        const tags = (tagsData && tagsData.tags) || [];
        if (tags.length > 0) {
            renderPipelineTagsList(tags, tagsData.default_branch || 'main');
            // Auto-select first tag only if no untagged commits
            if (untaggedCommits.length === 0) {
                selectPipelineTag(tags[0].name);
            }
        } else if (untaggedCommits.length === 0) {
            tagsList.innerHTML = `
                <div class="empty-placeholder">
                    <p>No tags found in this repository.</p>
                    <p class="hint">Create a git tag first, or push a commit to trigger auto-tagging.</p>
                </div>
            `;
        } else {
            tagsList.innerHTML = `<div class="empty-placeholder"><p>No tags yet. The selected commit will be auto-tagged.</p></div>`;
        }
    } catch (e) {
        console.error('Failed to load pipeline data:', e);
        tagsList.innerHTML = `
            <div class="error-placeholder">
                <p>Failed to load data: ${escapeHtml(e.message || 'Unknown error')}</p>
            </div>
        `;
    }
}

function renderPipelineUntaggedList(commits) {
    const list = document.getElementById('pipeline-untagged-list');

    let html = `<div class="tags-group"><div class="tags-group-items">`;
    for (const commit of commits) {
        const timeAgo = formatTimeAgo(commit.date);
        const msgFirstLine = (commit.message || '').split('\n')[0].substring(0, 60);
        html += `
            <div class="tag-item untagged-commit-item" data-commit="${escapeHtml(commit.sha)}" onclick="selectPipelineCommit('${escapeHtml(commit.sha)}')">
                <span class="tag-name" style="display:flex;align-items:center;gap:6px;">
                    <span class="tag-sha" style="font-weight:600;color:var(--accent)">${escapeHtml(commit.short_sha)}</span>
                    <span style="color:var(--text-secondary);font-size:0.85em;">${escapeHtml(msgFirstLine)}</span>
                </span>
                <span class="tag-meta">
                    ${timeAgo ? `<span class="tag-age">${timeAgo}</span>` : ''}
                    <span style="font-size:0.8em;color:var(--text-muted)">${escapeHtml(commit.author_name || '')}</span>
                </span>
            </div>
        `;
    }
    html += `</div></div>`;
    list.innerHTML = html;
}

function renderPipelineTagsList(tags, defaultBranch) {
    const tagsList = document.getElementById('pipeline-tags-list');

    let html = `
        <div class="tags-group">
            <div class="tags-group-header">
                <span class="branch-name">${escapeHtml(defaultBranch)}</span>
                <span class="tag-count">${tags.length} tag${tags.length !== 1 ? 's' : ''}</span>
            </div>
            <div class="tags-group-items">
    `;

    for (const tag of tags) {
        const timeAgo = formatTimeAgo(tag.created_at);
        html += `
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" onclick="selectPipelineTag('${escapeHtml(tag.name)}')">
                <span class="tag-name">${escapeHtml(tag.name)}</span>
                <span class="tag-meta">
                    ${timeAgo ? `<span class="tag-age">${timeAgo}</span>` : ''}
                    <span class="tag-sha">${escapeHtml(tag.sha.substring(0, 7))}</span>
                </span>
            </div>
        `;
    }

    html += `
            </div>
        </div>
    `;

    tagsList.innerHTML = html;
}

function selectPipelineCommit(sha) {
    selectedPipelineCommit = sha;
    selectedPipelineTag = null;  // deselect tag

    // Update visual selection in both lists
    document.querySelectorAll('#pipeline-tags-list .tag-item').forEach(el => el.classList.remove('selected'));
    document.querySelectorAll('#pipeline-untagged-list .tag-item').forEach(el => {
        el.classList.toggle('selected', el.dataset.commit === sha);
    });

    const selectedDisplay = document.getElementById('pipeline-selected-tag');
    const selectedValue = document.getElementById('pipeline-selected-tag-value');
    selectedValue.textContent = `${sha.substring(0, 7)} (will be auto-tagged)`;
    selectedDisplay.style.display = 'flex';
}

function selectPipelineTag(tagName) {
    selectedPipelineTag = tagName;
    selectedPipelineCommit = null;  // deselect commit

    // Update visual selection in both lists
    document.querySelectorAll('#pipeline-untagged-list .tag-item').forEach(el => el.classList.remove('selected'));
    const tagsList = document.getElementById('pipeline-tags-list');
    tagsList.querySelectorAll('.tag-item').forEach(el => {
        el.classList.toggle('selected', el.dataset.tag === tagName);
    });

    // Show selected tag display
    const selectedDisplay = document.getElementById('pipeline-selected-tag');
    const selectedValue = document.getElementById('pipeline-selected-tag-value');
    selectedValue.textContent = tagName;
    selectedDisplay.style.display = 'flex';
}

function closePipelineModal() {
    document.getElementById('stack-pipeline-modal').classList.remove('active');
    currentPipelineRepo = null;
    currentPipelineSshUrl = null;
    selectedPipelineTag = null;
    selectedPipelineCommit = null;
}

async function submitPipeline() {
    if (!currentPipelineRepo || !currentPipelineSshUrl) return;

    if (!selectedPipelineTag && !selectedPipelineCommit) {
        showNotification('error', 'Please select a version tag or an untagged commit');
        return;
    }

    const submitBtn = document.getElementById('stack-pipeline-submit');
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="btn-loading"></span> Starting...';

    try {
        let url;
        if (selectedPipelineTag) {
            url = `/stacks/pipeline?repo_name=${encodeURIComponent(currentPipelineRepo)}&ssh_url=${encodeURIComponent(currentPipelineSshUrl)}&tag=${encodeURIComponent(selectedPipelineTag)}`;
        } else {
            url = `/stacks/pipeline?repo_name=${encodeURIComponent(currentPipelineRepo)}&ssh_url=${encodeURIComponent(currentPipelineSshUrl)}&commit=${encodeURIComponent(selectedPipelineCommit)}`;
        }
        const response = await fetch(`${API_BASE}${url}`, { method: 'POST', headers: authHeaders() });
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        const result = await response.json();
        if (result.auto_tagged) {
            showNotification('success', `Commit auto-tagged as ${result.tag}`);
        }
        closePipelineModal();
        scheduleStacksStateUpdate();
    } catch (e) {
        showNotification('error', e.message || 'Pipeline failed to start');
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <polygon points="5 3 19 12 5 21 5 3"/>
            </svg>
            Run Pipeline
        `;
    }
}

// ============== Build Modal ==============

let currentBuildRepo = null;
let currentBuildSshUrl = null;
let selectedBuildTag = null;

async function buildStack(repoName, sshUrl) {
    currentBuildRepo = repoName;
    currentBuildSshUrl = sshUrl;
    selectedBuildTag = null;

    const modal = document.getElementById('stack-build-modal');
    const title = document.getElementById('stack-build-title');
    const tagsList = document.getElementById('build-tags-list');
    const branchSelect = document.getElementById('build-branch-select');
    const versionInput = document.getElementById('build-version-input');
    const commitInput = document.getElementById('build-commit-input');
    const selectedDisplay = document.getElementById('build-selected-tag');

    title.textContent = `Build: ${repoName}`;
    tagsList.innerHTML = '<div class="loading-placeholder">Loading tags...</div>';
    branchSelect.innerHTML = '<option value="">Loading branches...</option>';
    commitInput.value = '';
    versionInput.value = '1.0';
    selectedDisplay.style.display = 'none';
    document.getElementById('build-no-cache').checked = false;

    // Reset to tag mode
    document.querySelector('input[name="build-source"][value="tag"]').checked = true;
    toggleBuildSource('tag');

    modal.classList.add('active');

    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        tagsList.innerHTML = '<div class="error-placeholder">Failed to parse repository URL</div>';
        return;
    }
    const owner = ownerMatch[1];

    // Load tags and branches in parallel
    try {
        const [tagsData, branchesData] = await Promise.all([
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/tags?limit=20`),
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/branches`),
        ]);

        // Render tags
        if (tagsData && tagsData.tags && tagsData.tags.length > 0) {
            renderBuildTagsList(tagsData.tags, tagsData.default_branch || 'main');
            selectBuildTag(tagsData.tags[0].name);
        } else {
            tagsList.innerHTML = `
                <div class="empty-placeholder">
                    <p>No tags found.</p>
                    <p class="hint">Use Branch or Commit ID with a manual version.</p>
                </div>
            `;
        }

        // Render branches
        if (branchesData && branchesData.branches && branchesData.branches.length > 0) {
            branchSelect.innerHTML = branchesData.branches.map(b =>
                `<option value="${escapeHtml(b.name)}" ${b.name === 'main' || b.name === 'master' ? 'selected' : ''}>
                    ${escapeHtml(b.name)}${b.protected ? ' \uD83D\uDD12' : ''}
                </option>`
            ).join('');
        } else {
            branchSelect.innerHTML = '<option value="main">main</option>';
        }
    } catch (e) {
        console.error('Failed to load build data:', e);
        tagsList.innerHTML = '<div class="error-placeholder">Failed to load tags</div>';
        branchSelect.innerHTML = '<option value="main">main (default)</option>';
    }
}

function renderBuildTagsList(tags, defaultBranch) {
    const tagsList = document.getElementById('build-tags-list');
    let html = `
        <div class="tags-group">
            <div class="tags-group-header">
                <span class="branch-name">${escapeHtml(defaultBranch)}</span>
                <span class="tag-count">${tags.length} tag${tags.length !== 1 ? 's' : ''}</span>
            </div>
            <div class="tags-group-items">
    `;
    for (const tag of tags) {
        const timeAgo = formatTimeAgo(tag.created_at);
        html += `
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" onclick="selectBuildTag('${escapeHtml(tag.name)}')">
                <span class="tag-name">${escapeHtml(tag.name)}</span>
                <span class="tag-meta">
                    ${timeAgo ? `<span class="tag-age">${timeAgo}</span>` : ''}
                    <span class="tag-sha">${escapeHtml(tag.sha.substring(0, 7))}</span>
                </span>
            </div>
        `;
    }
    html += '</div></div>';
    tagsList.innerHTML = html;
}

function selectBuildTag(tagName) {
    selectedBuildTag = tagName;
    document.getElementById('build-tags-list').querySelectorAll('.tag-item').forEach(el => {
        el.classList.toggle('selected', el.dataset.tag === tagName);
    });
    const selectedDisplay = document.getElementById('build-selected-tag');
    const selectedValue = document.getElementById('build-selected-tag-value');
    selectedValue.textContent = tagName;
    selectedDisplay.style.display = 'flex';
}

function toggleBuildSource(source) {
    const tagGroup = document.getElementById('build-tag-group');
    const branchGroup = document.getElementById('build-branch-group');
    const commitGroup = document.getElementById('build-commit-group');
    const versionGroup = document.getElementById('build-version-group');

    tagGroup.style.display = source === 'tag' ? 'block' : 'none';
    branchGroup.style.display = source === 'branch' ? 'block' : 'none';
    commitGroup.style.display = source === 'commit' ? 'block' : 'none';
    // Show manual version input only for branch/commit (tag already has a version)
    versionGroup.style.display = (source === 'branch' || source === 'commit') ? 'block' : 'none';
}

function closeBuildModal() {
    document.getElementById('stack-build-modal').classList.remove('active');
    currentBuildRepo = null;
    currentBuildSshUrl = null;
    selectedBuildTag = null;
}

async function submitBuild() {
    if (!currentBuildRepo || !currentBuildSshUrl) return;

    const submitBtn = document.getElementById('stack-build-submit');
    const source = document.querySelector('input[name="build-source"]:checked').value;

    let tag = null;
    let branch = null;
    let commit = null;
    let version = document.getElementById('build-version-input').value || '1.0';

    if (source === 'tag') {
        if (!selectedBuildTag) {
            showNotification('error', 'Please select a version tag');
            return;
        }
        tag = selectedBuildTag;
    } else if (source === 'branch') {
        branch = document.getElementById('build-branch-select').value;
    } else {
        commit = document.getElementById('build-commit-input').value.trim();
        if (!commit) {
            showNotification('error', 'Please enter a commit ID');
            return;
        }
        if (!/^[a-fA-F0-9]{7,40}$/.test(commit)) {
            showNotification('error', 'Invalid commit ID format. Expected 7-40 hexadecimal characters.');
            return;
        }
    }

    // Disable button and show loading
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="btn-loading"></span> Starting...';

    try {
        let url = `/stacks/build?repo_name=${encodeURIComponent(currentBuildRepo)}&ssh_url=${encodeURIComponent(currentBuildSshUrl)}`;
        if (tag) {
            url += `&tag=${encodeURIComponent(tag)}`;
        } else {
            url += `&version=${encodeURIComponent(version)}`;
        }
        if (branch) {
            url += `&branch=${encodeURIComponent(branch)}`;
        }
        if (commit) {
            url += `&commit=${encodeURIComponent(commit)}`;
        }
        const noCache = document.getElementById('build-no-cache').checked;
        if (noCache) {
            url += '&no_cache=true';
        }

        const repoName = currentBuildRepo;
        const response = await fetch(`${API_BASE}${url}`, { method: 'POST', headers: authHeaders() });
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        const data = await response.json();
        if (data && data.action_id) {
            closeBuildModal();
            trackBackgroundAction(data.action_id, 'Build', repoName);
        } else {
            showNotification('error', 'Failed to start build');
        }
    } catch (e) {
        showNotification('error', e.message || 'Build failed');
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
                <polyline points="22 4 12 14.01 9 11.01"/>
            </svg>
            Build
        `;
    }
}

// ============== Test Modal ==============

let currentTestRepo = null;
let currentTestSshUrl = null;

let selectedTestTag = null;

async function testStack(repoName, sshUrl) {
    currentTestRepo = repoName;
    currentTestSshUrl = sshUrl;
    selectedTestTag = null;

    const modal = document.getElementById('stack-test-modal');
    const title = document.getElementById('stack-test-title');
    const tagsList = document.getElementById('test-tags-list');
    const branchSelect = document.getElementById('test-branch-select');
    const commitInput = document.getElementById('test-commit-input');
    const selectedDisplay = document.getElementById('test-selected-tag');

    title.textContent = `Test: ${repoName}`;
    tagsList.innerHTML = '<div class="loading-placeholder">Loading tags...</div>';
    branchSelect.innerHTML = '<option value="">Loading branches...</option>';
    commitInput.value = '';
    selectedDisplay.style.display = 'none';

    // Reset to tag mode
    document.querySelector('input[name="test-source"][value="tag"]').checked = true;
    toggleTestSource('tag');

    modal.classList.add('active');

    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        tagsList.innerHTML = '<div class="error-placeholder">Failed to parse repository URL</div>';
        return;
    }
    const owner = ownerMatch[1];

    // Load tags and branches in parallel
    try {
        const [tagsData, branchesData] = await Promise.all([
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/tags?limit=20`),
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/branches`),
        ]);

        // Render tags
        if (tagsData && tagsData.tags && tagsData.tags.length > 0) {
            renderTestTagsList(tagsData.tags, tagsData.default_branch || 'main');
            selectTestTag(tagsData.tags[0].name);
        } else {
            tagsList.innerHTML = `
                <div class="empty-placeholder">
                    <p>No tags found.</p>
                    <p class="hint">Use Branch or Commit ID instead.</p>
                </div>
            `;
        }

        // Render branches
        if (branchesData && branchesData.branches && branchesData.branches.length > 0) {
            branchSelect.innerHTML = branchesData.branches.map(b =>
                `<option value="${escapeHtml(b.name)}" ${b.name === 'main' || b.name === 'master' ? 'selected' : ''}>
                    ${escapeHtml(b.name)}${b.protected ? ' \uD83D\uDD12' : ''}
                </option>`
            ).join('');
        } else {
            branchSelect.innerHTML = '<option value="main">main</option>';
        }
    } catch (e) {
        console.error('Failed to load test data:', e);
        tagsList.innerHTML = '<div class="error-placeholder">Failed to load tags</div>';
        branchSelect.innerHTML = '<option value="main">main (default)</option>';
    }
}

function renderTestTagsList(tags, defaultBranch) {
    const tagsList = document.getElementById('test-tags-list');
    let html = `
        <div class="tags-group">
            <div class="tags-group-header">
                <span class="branch-name">${escapeHtml(defaultBranch)}</span>
                <span class="tag-count">${tags.length} tag${tags.length !== 1 ? 's' : ''}</span>
            </div>
            <div class="tags-group-items">
    `;
    for (const tag of tags) {
        const timeAgo = formatTimeAgo(tag.created_at);
        html += `
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" onclick="selectTestTag('${escapeHtml(tag.name)}')">
                <span class="tag-name">${escapeHtml(tag.name)}</span>
                <span class="tag-meta">
                    ${timeAgo ? `<span class="tag-age">${timeAgo}</span>` : ''}
                    <span class="tag-sha">${escapeHtml(tag.sha.substring(0, 7))}</span>
                </span>
            </div>
        `;
    }
    html += '</div></div>';
    tagsList.innerHTML = html;
}

function selectTestTag(tagName) {
    selectedTestTag = tagName;
    document.getElementById('test-tags-list').querySelectorAll('.tag-item').forEach(el => {
        el.classList.toggle('selected', el.dataset.tag === tagName);
    });
    const selectedDisplay = document.getElementById('test-selected-tag');
    const selectedValue = document.getElementById('test-selected-tag-value');
    selectedValue.textContent = tagName;
    selectedDisplay.style.display = 'flex';
}

function toggleTestSource(source) {
    const tagGroup = document.getElementById('test-tag-group');
    const branchGroup = document.getElementById('test-branch-group');
    const commitGroup = document.getElementById('test-commit-group');

    tagGroup.style.display = source === 'tag' ? 'block' : 'none';
    branchGroup.style.display = source === 'branch' ? 'block' : 'none';
    commitGroup.style.display = source === 'commit' ? 'block' : 'none';
}

function closeTestModal() {
    document.getElementById('stack-test-modal').classList.remove('active');
    currentTestRepo = null;
    currentTestSshUrl = null;
    selectedTestTag = null;
}

async function submitTest() {
    if (!currentTestRepo || !currentTestSshUrl) return;

    const submitBtn = document.getElementById('stack-test-submit');
    const source = document.querySelector('input[name="test-source"]:checked').value;

    let tag = null;
    let branch = null;
    let commit = null;

    if (source === 'tag') {
        if (!selectedTestTag) {
            showNotification('error', 'Please select a version tag');
            return;
        }
        tag = selectedTestTag;
    } else if (source === 'branch') {
        branch = document.getElementById('test-branch-select').value;
    } else {
        commit = document.getElementById('test-commit-input').value.trim();
        if (!commit) {
            showNotification('error', 'Please enter a commit ID');
            return;
        }
        if (!/^[a-fA-F0-9]{7,40}$/.test(commit)) {
            showNotification('error', 'Invalid commit ID format. Expected 7-40 hexadecimal characters.');
            return;
        }
    }

    // Disable button and show loading
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="btn-loading"></span> Starting...';

    try {
        let url = `/stacks/test?repo_name=${encodeURIComponent(currentTestRepo)}&ssh_url=${encodeURIComponent(currentTestSshUrl)}`;
        if (tag) {
            url += `&tag=${encodeURIComponent(tag)}`;
        }
        if (branch) {
            url += `&branch=${encodeURIComponent(branch)}`;
        }
        if (commit) {
            url += `&commit=${encodeURIComponent(commit)}`;
        }

        const repoName = currentTestRepo;
        const response = await fetch(`${API_BASE}${url}`, { method: 'POST', headers: authHeaders() });
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        const data = await response.json();
        if (data && data.action_id) {
            closeTestModal();
            trackBackgroundAction(data.action_id, 'Test', repoName);
        } else {
            showNotification('error', 'Failed to start tests');
        }
    } catch (e) {
        showNotification('error', e.message || 'Test failed');
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M9 11l3 3L22 4"/>
                <path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/>
            </svg>
            Run Tests
        `;
    }
}

// ============== Deploy Modal ==============

let currentDeployRepo = null;
let currentDeploySshUrl = null;
let selectedDeployTag = null;

async function deployStack(repoName, sshUrl) {
    currentDeployRepo = repoName;
    currentDeploySshUrl = sshUrl;
    selectedDeployTag = null;
    
    const modal = document.getElementById('stack-deploy-modal');
    const title = document.getElementById('stack-deploy-title');
    const tagsList = document.getElementById('deploy-tags-list');
    const tagInput = document.getElementById('deploy-tag-input');
    const selectedDisplay = document.getElementById('deploy-selected-tag');
    
    title.textContent = `Deploy: ${repoName}`;
    tagsList.innerHTML = '<div class="loading-placeholder">Loading tags...</div>';
    tagInput.value = '';
    selectedDisplay.style.display = 'none';
    
    // Reset to select mode
    document.querySelector('input[name="deploy-source"][value="select"]').checked = true;
    toggleDeploySource('select');
    
    modal.classList.add('active');
    
    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        tagsList.innerHTML = '<div class="error-placeholder">Failed to parse repository URL</div>';
        return;
    }
    const owner = ownerMatch[1];
    
    // Load tags
    try {
        const data = await apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/tags?limit=20`);
        if (data && data.tags && data.tags.length > 0) {
            renderDeployTagsList(data.tags, data.default_branch || 'main');
            
            // Auto-select the first (most recent) tag
            if (data.tags.length > 0) {
                selectDeployTag(data.tags[0].name);
            }
        } else {
            tagsList.innerHTML = `
                <div class="empty-placeholder">
                    <p>No tags found in this repository.</p>
                    <p class="hint">Use the manual input to enter a tag version.</p>
                </div>
            `;
        }
    } catch (e) {
        console.error('Failed to load tags:', e);
        tagsList.innerHTML = `
            <div class="error-placeholder">
                <p>Failed to load tags: ${escapeHtml(e.message || 'Unknown error')}</p>
                <p class="hint">You can still enter a tag manually.</p>
            </div>
        `;
    }
}

function formatTimeAgo(dateString) {
    if (!dateString) return '';
    
    const date = new Date(dateString);
    const now = new Date();
    const seconds = Math.floor((now - date) / 1000);
    
    if (seconds < 60) return 'just now';
    
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    
    const days = Math.floor(hours / 24);
    if (days < 7) return `${days}d ago`;
    
    const weeks = Math.floor(days / 7);
    if (weeks < 4) return `${weeks}w ago`;
    
    const months = Math.floor(days / 30);
    if (months < 12) return `${months}mo ago`;
    
    const years = Math.floor(days / 365);
    return `${years}y ago`;
}

function renderDeployTagsList(tags, defaultBranch) {
    const tagsList = document.getElementById('deploy-tags-list');
    
    // Group tags - for now we show them all in one group
    let html = `
        <div class="tags-group">
            <div class="tags-group-header">
                <span class="branch-name">${escapeHtml(defaultBranch)}</span>
                <span class="tag-count">${tags.length} tag${tags.length !== 1 ? 's' : ''}</span>
            </div>
            <div class="tags-group-items">
    `;
    
    for (const tag of tags) {
        const timeAgo = formatTimeAgo(tag.created_at);
        html += `
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" onclick="selectDeployTag('${escapeHtml(tag.name)}')">
                <span class="tag-name">${escapeHtml(tag.name)}</span>
                <span class="tag-meta">
                    ${timeAgo ? `<span class="tag-age">${timeAgo}</span>` : ''}
                    <span class="tag-sha">${escapeHtml(tag.sha.substring(0, 7))}</span>
                </span>
            </div>
        `;
    }
    
    html += `
            </div>
        </div>
    `;
    
    tagsList.innerHTML = html;
}

function selectDeployTag(tagName) {
    selectedDeployTag = tagName;

    // Update visual selection (scoped to deploy modal)
    document.getElementById('deploy-tags-list').querySelectorAll('.tag-item').forEach(el => {
        el.classList.toggle('selected', el.dataset.tag === tagName);
    });
    
    // Show selected tag display
    const selectedDisplay = document.getElementById('deploy-selected-tag');
    const selectedValue = document.getElementById('deploy-selected-tag-value');
    selectedValue.textContent = tagName;
    selectedDisplay.style.display = 'flex';
}

function toggleDeploySource(source) {
    const selectGroup = document.getElementById('deploy-select-group');
    const manualGroup = document.getElementById('deploy-manual-group');
    const selectedDisplay = document.getElementById('deploy-selected-tag');
    
    if (source === 'select') {
        selectGroup.style.display = 'block';
        manualGroup.style.display = 'none';
        // Restore selection display if we have a selected tag
        if (selectedDeployTag) {
            selectedDisplay.style.display = 'flex';
        }
    } else {
        selectGroup.style.display = 'none';
        manualGroup.style.display = 'block';
        selectedDisplay.style.display = 'none';
    }
}

function closeDeployModal() {
    document.getElementById('stack-deploy-modal').classList.remove('active');
    currentDeployRepo = null;
    currentDeploySshUrl = null;
    selectedDeployTag = null;
}

// ============== Service Deploy (Individual Container) ==============

let currentDeployServiceName = null;
let currentServiceRepoName = null;
let currentServiceSshUrl = null;
let currentServiceImage = null;
let selectedServiceDeployTag = null;

async function openServiceDeploy(serviceName, repoName, sshUrl, currentImage) {
    currentDeployServiceName = serviceName;
    currentServiceRepoName = repoName;
    currentServiceSshUrl = sshUrl;
    currentServiceImage = currentImage;
    selectedServiceDeployTag = null;
    
    const modal = document.getElementById('service-deploy-modal');
    const title = document.getElementById('service-deploy-title');
    const tagsList = document.getElementById('service-deploy-tags-list');
    const tagInput = document.getElementById('service-deploy-tag-input');
    const selectedDisplay = document.getElementById('service-deploy-selected-tag');
    const currentDisplay = document.getElementById('service-deploy-current');
    
    title.textContent = `Deploy: ${serviceName}`;
    currentDisplay.innerHTML = `<strong>Current image:</strong> ${escapeHtml(currentImage)}`;
    tagsList.innerHTML = '<div class="loading-placeholder">Loading tags...</div>';
    tagInput.value = '';
    selectedDisplay.style.display = 'none';
    
    // Reset to select mode
    const selectRadio = document.querySelector('input[name="service-deploy-source"][value="select"]');
    if (selectRadio) selectRadio.checked = true;
    toggleServiceDeploySource('select');
    
    modal.classList.add('active');
    
    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        tagsList.innerHTML = '<div class="error-placeholder">Failed to parse repository URL</div>';
        return;
    }
    const owner = ownerMatch[1];
    
    // Load tags
    try {
        const data = await apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/tags?limit=20`);
        if (data && data.tags && data.tags.length > 0) {
            renderServiceDeployTagsList(data.tags, data.default_branch || 'main');
        } else {
            tagsList.innerHTML = `
                <div class="empty-placeholder">
                    <p>No tags found in this repository.</p>
                    <p class="hint">Use the manual input to enter a tag version.</p>
                </div>
            `;
        }
    } catch (e) {
        console.error('Failed to load tags:', e);
        tagsList.innerHTML = `
            <div class="error-placeholder">
                <p>Failed to load tags: ${escapeHtml(e.message || 'Unknown error')}</p>
                <p class="hint">You can still enter a tag manually.</p>
            </div>
        `;
    }
}

function renderServiceDeployTagsList(tags, defaultBranch) {
    const tagsList = document.getElementById('service-deploy-tags-list');
    
    let html = `
        <div class="tags-group">
            <div class="tags-group-header">
                <span class="branch-name">${escapeHtml(defaultBranch)}</span>
                <span class="tag-count">${tags.length} tag${tags.length !== 1 ? 's' : ''}</span>
            </div>
            <div class="tags-group-items">
    `;
    
    for (const tag of tags) {
        const timeAgo = formatTimeAgo(tag.created_at);
        html += `
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" onclick="selectServiceDeployTag('${escapeHtml(tag.name)}')">
                <span class="tag-name">${escapeHtml(tag.name)}</span>
                <span class="tag-meta">
                    ${timeAgo ? `<span class="tag-age">${timeAgo}</span>` : ''}
                    <span class="tag-sha">${escapeHtml(tag.sha.substring(0, 7))}</span>
                </span>
            </div>
        `;
    }
    
    html += `
            </div>
        </div>
    `;
    
    tagsList.innerHTML = html;
}

function selectServiceDeployTag(tagName) {
    selectedServiceDeployTag = tagName;
    
    // Update visual selection in service deploy modal
    document.querySelectorAll('#service-deploy-tags-list .tag-item').forEach(el => {
        el.classList.toggle('selected', el.dataset.tag === tagName);
    });
    
    // Show selected tag display
    const selectedDisplay = document.getElementById('service-deploy-selected-tag');
    const selectedValue = document.getElementById('service-deploy-selected-tag-value');
    selectedValue.textContent = tagName;
    selectedDisplay.style.display = 'flex';
}

function toggleServiceDeploySource(source) {
    const selectGroup = document.getElementById('service-deploy-select-group');
    const manualGroup = document.getElementById('service-deploy-manual-group');
    const selectedDisplay = document.getElementById('service-deploy-selected-tag');
    
    if (source === 'select') {
        selectGroup.style.display = 'block';
        manualGroup.style.display = 'none';
        if (selectedServiceDeployTag) {
            selectedDisplay.style.display = 'flex';
        }
    } else {
        selectGroup.style.display = 'none';
        manualGroup.style.display = 'block';
        selectedDisplay.style.display = 'none';
    }
}

function closeServiceDeployModal() {
    document.getElementById('service-deploy-modal').classList.remove('active');
    currentDeployServiceName = null;
    currentServiceRepoName = null;
    currentServiceSshUrl = null;
    currentServiceImage = null;
    selectedServiceDeployTag = null;
}

async function submitServiceDeploy() {
    if (!currentDeployServiceName) return;
    
    const submitBtn = document.getElementById('service-deploy-submit');
    const source = document.querySelector('input[name="service-deploy-source"]:checked').value;
    
    let tag = null;
    
    if (source === 'select') {
        tag = selectedServiceDeployTag;
        if (!tag) {
            showNotification('error', 'Please select a tag to deploy');
            return;
        }
    } else {
        tag = document.getElementById('service-deploy-tag-input').value.trim();
        if (!tag) {
            showNotification('error', 'Please enter a tag to deploy');
            return;
        }
    }
    
    // Disable button and show loading
    submitBtn.disabled = true;
    submitBtn.innerHTML = `
        <svg class="spinner" viewBox="0 0 24 24" width="14" height="14">
            <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3" fill="none" stroke-dasharray="31.4 31.4" transform="rotate(-90 12 12)"/>
        </svg>
        Deploying...
    `;
    
    try {
        const url = `/services/${encodeURIComponent(currentDeployServiceName)}/update-image?tag=${encodeURIComponent(tag)}`;
        console.log('Deploying service:', currentDeployServiceName, 'with tag:', tag, 'URL:', `${API_BASE}${url}`);
        const response = await fetch(`${API_BASE}${url}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', ...authHeaders() }
        });

        console.log('Deploy response status:', response.status);
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            showNotification('success', result.message || `Service updated to tag ${tag}`);
            closeServiceDeployModal();
            // Incremental update after service deploy
            scheduleStacksStateUpdate();
        } else {
            showNotification('error', result.message || 'Failed to update service');
        }
    } catch (error) {
        console.error('Failed to update service:', error);
        showNotification('error', `Failed to update service: ${error.message || 'Unknown error'}`);
    } finally {
        // Reset button
        submitBtn.disabled = false;
        submitBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
            </svg>
            Deploy
        `;
    }
}

async function submitDeploy() {
    if (!currentDeployRepo || !currentDeploySshUrl) return;
    
    const submitBtn = document.getElementById('stack-deploy-submit');
    const source = document.querySelector('input[name="deploy-source"]:checked').value;
    const version = '1.0';
    
    let tag = null;
    
    if (source === 'select') {
        tag = selectedDeployTag;
        if (!tag) {
            showNotification('error', 'Please select a tag to deploy');
            return;
        }
    } else {
        tag = document.getElementById('deploy-tag-input').value.trim();
        if (!tag) {
            showNotification('error', 'Please enter a tag to deploy');
            return;
        }
        // Basic validation for tag format
        if (!/^v?\d+(\.\d+){0,2}$/.test(tag)) {
            showNotification('error', 'Invalid tag format. Expected: vX.X.X or X.X.X (e.g., v1.0.5)');
            return;
        }
    }
    
    // Disable button and show loading
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="btn-loading"></span> Starting...';
    
    try {
        let url = `/stacks/deploy?repo_name=${encodeURIComponent(currentDeployRepo)}&ssh_url=${encodeURIComponent(currentDeploySshUrl)}&version=${encodeURIComponent(version)}`;
        if (tag) {
            url += `&tag=${encodeURIComponent(tag)}`;
        }
        
        const repoName = currentDeployRepo;
        const response = await fetch(`${API_BASE}${url}`, { method: 'POST', headers: authHeaders() });
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        const data = await response.json();
        if (data && data.action_id) {
            closeDeployModal();
            trackBackgroundAction(data.action_id, 'Deploy', repoName);
        } else {
            showNotification('error', 'Failed to start deployment');
        }
    } catch (e) {
        showNotification('error', e.message || 'Deploy failed');
    } finally {
        submitBtn.disabled = false;
        submitBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                <polyline points="7.5 4.21 12 6.81 16.5 4.21"/>
                <polyline points="7.5 19.79 7.5 14.6 3 12"/>
                <polyline points="21 12 16.5 14.6 16.5 19.79"/>
                <polyline points="3.27 6.96 12 12.01 20.73 6.96"/>
                <line x1="12" y1="22.08" x2="12" y2="12"/>
            </svg>
            Deploy
        `;
    }
}

// ============== Activity Modal ==============

let activityOwner = null;
let activityRepo = null;
let activityCommits = [];
let activityBranches = [];
let activityBranchTipMap = {};
let activityTagMap = {};
let activityCommitBranches = {};

const BRANCH_COLORS = ['#00d4aa', '#0ea5e9', '#8b5cf6', '#f59e0b', '#ef4444', '#ec4899', '#14b8a6', '#f97316'];

async function showStackActivity(owner, repo) {
    activityOwner = owner;
    activityRepo = repo;
    activityCommits = [];

    const modal = document.getElementById('stack-activity-modal');
    const title = document.getElementById('stack-activity-title');
    const graphContainer = document.getElementById('activity-graph-container');
    const diffPanel = document.getElementById('activity-diff-panel');

    title.textContent = `Activity: ${repo}`;
    graphContainer.innerHTML = '<div class="loading-placeholder">Loading activity...</div>';
    diffPanel.style.display = 'none';
    modal.classList.add('active');

    await loadActivityData();
}

async function loadActivityData() {
    const graphContainer = document.getElementById('activity-graph-container');

    const data = await apiGet(`/stacks/${encodeURIComponent(activityOwner)}/${encodeURIComponent(activityRepo)}/activity?per_page=30`);

    if (!data) {
        graphContainer.innerHTML = '<div class="loading-placeholder" style="color: var(--status-error);">Failed to load activity data. Check browser console for details.</div>';
        return;
    }

    activityCommits = data.commits || [];
    activityBranches = data.branches || [];
    activityBranchTipMap = data.branch_tip_map || {};
    activityTagMap = data.tag_map || {};
    activityCommitBranches = data.commit_branches || {};

    if (data.error && activityCommits.length === 0) {
        graphContainer.innerHTML = `<div class="loading-placeholder" style="color: var(--status-error);">${escapeHtml(data.error)}</div>`;
        return;
    }

    if (activityCommits.length === 0) {
        // No error from backend but also no commits — run a diagnostic check
        graphContainer.innerHTML = '<div class="loading-placeholder">No commits found. Checking permissions...</div>';
        const diagData = await apiGet(`/stacks/test-permissions/${encodeURIComponent(activityOwner)}/${encodeURIComponent(activityRepo)}`);
        if (diagData && diagData.summary) {
            graphContainer.innerHTML = `<div class="loading-placeholder" style="color: var(--status-error);">${escapeHtml(diagData.summary)}</div>`;
        } else {
            graphContainer.innerHTML = '<div class="loading-placeholder" style="color: var(--status-error);">No commits found. Ensure your GitHub token has \'Contents: Read\' permission.</div>';
        }
        return;
    }

    renderActivityGraph();
}

function closeActivityModal() {
    document.getElementById('stack-activity-modal').classList.remove('active');
    activityOwner = null;
    activityRepo = null;
    activityCommits = [];
}

function renderActivityGraph() {
    const container = document.getElementById('activity-graph-container');
    if (!activityCommits || activityCommits.length === 0) {
        container.innerHTML = '<div class="loading-placeholder">No commits found</div>';
        return;
    }

    const ROW_HEIGHT = 44;
    const GRAPH_LEFT = 16;
    const COL_WIDTH = 20;
    const RADIUS = 5;

    // Build SHA -> index map
    const shaIdx = {};
    activityCommits.forEach((c, i) => { shaIdx[c.sha] = i; });

    // Assign each branch a color based on its name
    const branchColorMap = {};
    activityBranches.forEach((b, i) => {
        branchColorMap[b.name] = BRANCH_COLORS[i % BRANCH_COLORS.length];
    });

    // Lane assignment: figure out which column each commit goes in
    // Strategy: each branch tip starts a lane, first-parent chains stay in their lane
    const lanes = new Array(activityCommits.length).fill(-1);
    const laneUsed = []; // which lanes are currently in use

    function acquireLane(preferred) {
        if (preferred >= 0 && !laneUsed.includes(preferred)) {
            laneUsed.push(preferred);
            return preferred;
        }
        let l = 0;
        while (laneUsed.includes(l)) l++;
        laneUsed.push(l);
        return l;
    }

    function releaseLane(l) {
        const idx = laneUsed.indexOf(l);
        if (idx !== -1) laneUsed.splice(idx, 1);
    }

    // Process commits top-down (newest first)
    // Each commit needs a lane. If it's a branch tip, it gets a new lane.
    // Its first parent inherits the same lane. Merge parents get new lanes.
    for (let i = 0; i < activityCommits.length; i++) {
        const commit = activityCommits[i];

        if (lanes[i] === -1) {
            lanes[i] = acquireLane(-1);
        }
        const myLane = lanes[i];

        // Check how many children use myLane - if this commit has been assigned
        // its lane by a child, and no other child continues it, we can potentially reuse
        const parents = commit.parents || [];

        parents.forEach((pSha, pIdx) => {
            const pi = shaIdx[pSha];
            if (pi === undefined) return;

            if (pIdx === 0) {
                // First parent: inherit my lane if not already assigned
                if (lanes[pi] === -1) {
                    lanes[pi] = myLane;
                } else {
                    // Already assigned by another child - release my lane
                    releaseLane(myLane);
                }
            } else {
                // Merge parent: assign new lane if needed
                if (lanes[pi] === -1) {
                    lanes[pi] = acquireLane(-1);
                }
            }
        });

        // If this commit has no parents in our set, release its lane
        const hasParentInSet = parents.some(p => shaIdx[p] !== undefined);
        if (!hasParentInSet) {
            releaseLane(myLane);
        }
    }

    // Fix any unassigned lanes
    for (let i = 0; i < lanes.length; i++) {
        if (lanes[i] === -1) lanes[i] = 0;
    }

    const maxCol = Math.max(0, ...lanes) + 1;
    const graphWidth = GRAPH_LEFT + maxCol * COL_WIDTH + 12;
    const totalHeight = activityCommits.length * ROW_HEIGHT + 20;

    // Determine the primary branch for each commit (for coloring)
    function getCommitColor(i) {
        const sha = activityCommits[i].sha;
        // Use the first branch that contains this commit
        const branches = activityCommitBranches[sha];
        if (branches && branches.length > 0) {
            return branchColorMap[branches[0]] || BRANCH_COLORS[lanes[i] % BRANCH_COLORS.length];
        }
        return BRANCH_COLORS[lanes[i] % BRANCH_COLORS.length];
    }

    // Build SVG lines and circles
    let svgLines = '';
    let svgCircles = '';

    for (let i = 0; i < activityCommits.length; i++) {
        const commit = activityCommits[i];
        const col = lanes[i];
        const cx = GRAPH_LEFT + col * COL_WIDTH;
        const cy = 22 + i * ROW_HEIGHT;
        const color = getCommitColor(i);

        // Draw lines to parents
        (commit.parents || []).forEach(pSha => {
            const pi = shaIdx[pSha];
            if (pi === undefined) return;
            const pCol = lanes[pi];
            const px = GRAPH_LEFT + pCol * COL_WIDTH;
            const py = 22 + pi * ROW_HEIGHT;
            const pColor = getCommitColor(pi);

            if (col === pCol) {
                svgLines += `<line x1="${cx}" y1="${cy}" x2="${px}" y2="${py}" stroke="${color}" stroke-width="2"/>`;
            } else {
                // Curved path
                const midY = cy + ROW_HEIGHT * 0.7;
                svgLines += `<path d="M${cx},${cy} C${cx},${midY} ${px},${midY} ${px},${py}" stroke="${pColor}" stroke-width="2" fill="none"/>`;
            }
        });

        // Commit circle
        svgCircles += `<circle cx="${cx}" cy="${cy}" r="${RADIUS}" fill="${color}" stroke="var(--bg-card)" stroke-width="2" class="activity-commit-dot" onclick="showCommitDiff('${commit.sha}')"/>`;
    }

    // Build commit rows
    let rowsHtml = '';
    for (let i = 0; i < activityCommits.length; i++) {
        const commit = activityCommits[i];
        const cy = 22 + i * ROW_HEIGHT;

        // Branch tip labels
        const branchLabels = (activityBranchTipMap[commit.sha] || []).map(name =>
            `<span class="activity-label activity-branch-label">${escapeHtml(name)}</span>`
        ).join('');
        const tagLabels = (activityTagMap[commit.sha] || []).map(name =>
            `<span class="activity-label activity-tag-label">${escapeHtml(name)}</span>`
        ).join('');

        const firstLine = commit.message.split('\n')[0];
        const labels = branchLabels + tagLabels;

        rowsHtml += `
        <div class="activity-row" style="height:${ROW_HEIGHT}px;top:${cy - ROW_HEIGHT / 2 + RADIUS}px;" data-sha="${commit.sha}" onclick="showCommitDiff('${commit.sha}')">
            <div class="activity-row-info" style="padding-left:${graphWidth}px;">
                ${labels ? `<span class="activity-labels">${labels}</span>` : ''}
                <span class="activity-sha">${escapeHtml(commit.short_sha)}</span>
                <span class="activity-msg">${escapeHtml(firstLine)}</span>
            </div>
            <div class="activity-row-meta">
                <span class="activity-author">${escapeHtml(commit.author_name)}</span>
                <span class="activity-date">${formatTimeAgo(commit.date)}</span>
            </div>
        </div>`;
    }

    container.innerHTML = `
        <div class="activity-graph" style="position:relative;min-height:${totalHeight}px;">
            <svg class="activity-svg" width="${graphWidth}" height="${totalHeight}" style="position:absolute;left:0;top:0;">
                ${svgLines}
                ${svgCircles}
            </svg>
            <div class="activity-rows">
                ${rowsHtml}
            </div>
        </div>
    `;
}

// ---- Diff Viewer ----

async function showCommitDiff(sha) {
    const diffPanel = document.getElementById('activity-diff-panel');
    const diffTitle = document.getElementById('activity-diff-title');
    const diffContent = document.getElementById('activity-diff-content');

    diffPanel.style.display = 'flex';
    diffTitle.textContent = sha.substring(0, 7);
    diffContent.innerHTML = '<div class="loading-placeholder">Loading diff...</div>';

    // Highlight selected row
    document.querySelectorAll('.activity-row.selected').forEach(el => el.classList.remove('selected'));
    const row = document.querySelector(`.activity-row[data-sha="${sha}"]`);
    if (row) row.classList.add('selected');

    const data = await apiGet(`/stacks/${encodeURIComponent(activityOwner)}/${encodeURIComponent(activityRepo)}/commits/${sha}/diff`);

    if (!data || !data.files) {
        diffContent.innerHTML = '<div class="loading-placeholder" style="color:var(--status-error);">Failed to load diff</div>';
        return;
    }

    const statsHtml = data.stats ? `
        <div class="diff-stats">
            <span class="diff-stat-add">+${data.stats.additions || 0}</span>
            <span class="diff-stat-del">-${data.stats.deletions || 0}</span>
            <span class="diff-stat-files">${data.files.length} file${data.files.length !== 1 ? 's' : ''}</span>
        </div>
    ` : '';

    const commitInfo = `
        <div class="diff-commit-info">
            <div class="diff-commit-message">${escapeHtml(data.message || '')}</div>
            <div class="diff-commit-meta">${escapeHtml(data.author_name || '')} &middot; ${formatTimeAgo(data.date)}</div>
            ${statsHtml}
        </div>
    `;

    const filesHtml = data.files.map(file => {
        const sc = file.status === 'added' ? 'file-added' : file.status === 'removed' ? 'file-removed' : file.status === 'renamed' ? 'file-renamed' : 'file-modified';
        const si = file.status === 'added' ? 'A' : file.status === 'removed' ? 'D' : file.status === 'renamed' ? 'R' : 'M';
        const renamed = file.previous_filename ? ` ← ${escapeHtml(file.previous_filename)}` : '';
        const patchHtml = renderPatch(file.patch || '');

        return `
            <div class="diff-file">
                <div class="diff-file-header" onclick="this.parentElement.classList.toggle('collapsed')">
                    <span class="diff-file-status ${sc}">${si}</span>
                    <span class="diff-file-name">${escapeHtml(file.filename)}${renamed}</span>
                    <span class="diff-file-stats">
                        <span class="diff-stat-add">+${file.additions}</span>
                        <span class="diff-stat-del">-${file.deletions}</span>
                    </span>
                    <svg class="chevron-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                        <polyline points="6 9 12 15 18 9"/>
                    </svg>
                </div>
                <div class="diff-file-body">
                    <pre class="diff-patch">${patchHtml}</pre>
                </div>
            </div>
        `;
    }).join('');

    diffContent.innerHTML = commitInfo + filesHtml;
}

function renderPatch(patch) {
    if (!patch) return '<span class="diff-no-patch">Binary file or no diff available</span>';
    return patch.split('\n').map(line => {
        const e = escapeHtml(line);
        if (line.startsWith('@@')) return `<span class="diff-line diff-hunk">${e}</span>`;
        if (line.startsWith('+') && !line.startsWith('+++')) return `<span class="diff-line diff-add">${e}</span>`;
        if (line.startsWith('-') && !line.startsWith('---')) return `<span class="diff-line diff-del">${e}</span>`;
        return `<span class="diff-line">${e}</span>`;
    }).join('\n');
}

function closeActivityDiff() {
    document.getElementById('activity-diff-panel').style.display = 'none';
    document.querySelectorAll('.activity-row.selected').forEach(el => el.classList.remove('selected'));
}

// ============== Background Action Tracking ==============

const _activeActions = {}; // action_id -> { interval, toastEl, ... }

function trackBackgroundAction(actionId, actionType, repoName) {
    const tracker = { actionId, actionType, repoName, interval: null };
    _activeActions[actionId] = tracker;

    // Force immediate pipeline state refresh to update inline status
    scheduleStacksStateUpdate();

    // Poll for completion (no toast — status shown inline in pipeline flow)
    tracker.interval = setInterval(async () => {
        try {
            const status = await apiGet(`/stacks/actions/${actionId}/status`);
            if (!status) return;

            if (status.status !== 'running') {
                clearInterval(tracker.interval);
                delete _activeActions[actionId];

                if (currentActionLogsId === actionId) {
                    stopActionLogsStream();
                }

                // Refresh pipeline state to update step indicators
                scheduleStacksStateUpdate();
            }
        } catch (e) {
            console.error('Failed to poll action status:', e);
        }
    }, 2000);
}

function formatElapsed(seconds) {
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function showActionToast(actionId, actionType, repoName) {
    // Remove any existing toast for the same action
    const existing = document.getElementById(`action-toast-${actionId}`);
    if (existing) existing.remove();
    
    const toast = document.createElement('div');
    toast.id = `action-toast-${actionId}`;
    toast.className = 'action-toast';
    toast.innerHTML = `
        <div class="action-toast-content">
            <div class="action-toast-info">
                <span class="action-toast-spinner"></span>
                <span class="action-toast-text"><strong>${escapeHtml(actionType)}</strong> ${escapeHtml(repoName)}</span>
                <span class="action-toast-elapsed">0s</span>
            </div>
            <div class="action-toast-actions">
                <button class="btn btn-sm btn-ghost" onclick="openActionLogs('${actionId}', '${escapeHtml(actionType)}', '${escapeHtml(repoName)}')" title="View logs">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                        <polyline points="14 2 14 8 20 8"/>
                        <line x1="16" y1="13" x2="8" y2="13"/>
                        <line x1="16" y1="17" x2="8" y2="17"/>
                    </svg>
                    Logs
                </button>
                <button class="btn btn-sm btn-danger" onclick="cancelAction('${actionId}')" title="Stop">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                        <rect x="6" y="6" width="12" height="12"/>
                    </svg>
                    Stop
                </button>
            </div>
        </div>
    `;
    
    // Add to toast container (create if needed)
    let container = document.getElementById('action-toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'action-toast-container';
        document.body.appendChild(container);
    }
    container.appendChild(toast);
    
    // Trigger entrance animation
    requestAnimationFrame(() => toast.classList.add('toast-visible'));
    
    return toast;
}

async function cancelAction(actionId) {
    try {
        const result = await apiPost(`/stacks/actions/${actionId}/cancel`);
        if (result && result.success) {
            showNotification('warning', 'Cancellation requested...');
        } else {
            showNotification('error', result?.message || 'Failed to cancel');
        }
    } catch (e) {
        showNotification('error', `Failed to cancel: ${e.message || 'Unknown error'}`);
    }
}

// ============== Action Logs Modal ==============

let currentActionLogsId = null;
let currentActionLogsType = null;
let currentActionLogsRepo = null;
let actionLogsPollTimer = null;
let actionLogsPollOffset = 0;
let actionLogsFirstRender = true;

function openActionLogs(actionId, actionType, repoName) {
    currentActionLogsId = actionId;
    currentActionLogsType = actionType;
    currentActionLogsRepo = repoName;

    const modal = document.getElementById('action-logs-modal');
    const title = document.getElementById('action-logs-title');
    const content = document.getElementById('action-logs-content');

    title.textContent = `${actionType}: ${repoName}`;
    content.innerHTML = '<div class="loading-placeholder">Loading logs...</div>';

    modal.classList.add('active');

    startActionLogsPoll(actionId);
}

function closeActionLogsModal() {
    stopActionLogsPoll();
    document.getElementById('action-logs-modal').classList.remove('active');
    currentActionLogsId = null;
    currentActionLogsType = null;
    currentActionLogsRepo = null;
}

function analyzeActionLogs() {
    const content = document.getElementById('action-logs-content');
    const logLines = Array.from(content.querySelectorAll('.log-line'))
        .map(el => el.textContent)
        .join('\n');

    const actionType = currentActionLogsType || 'Action';
    const repoName = currentActionLogsRepo || 'unknown';

    // Truncate logs if too long (keep last 200 lines)
    const lines = logLines.split('\n');
    const truncated = lines.length > 200
        ? '... (truncated)\n' + lines.slice(-200).join('\n')
        : logLines;

    const taskDesc = [
        `Analyze the following ${actionType} logs for project '${repoName}'.`,
        `Identify any errors, warnings, or issues and suggest fixes.\n`,
        `Logs:\n\`\`\`\n${truncated}\n\`\`\``
    ].join('\n');

    document.getElementById('task-project').value = repoName;
    document.getElementById('task-source').value = `${actionType} logs`;
    document.getElementById('task-description').value = taskDesc;
    document.getElementById('task-submit-btn').disabled = false;
    document.getElementById('task-submit-btn').textContent = 'Send Task';

    document.getElementById('create-task-modal').classList.add('active');
}

function stopActionLogsPoll() {
    if (actionLogsPollTimer) {
        clearTimeout(actionLogsPollTimer);
        actionLogsPollTimer = null;
    }
}

function appendLogLine(content, line) {
    const lineEl = document.createElement('div');
    lineEl.className = 'log-line';
    if (/\b(error|ERROR|fatal|FATAL|failed|FAILED)\b/.test(line)) {
        lineEl.classList.add('log-error');
    }
    lineEl.textContent = line;
    content.appendChild(lineEl);
}

async function startActionLogsPoll(actionId) {
    stopActionLogsPoll();
    actionLogsPollOffset = 0;
    actionLogsFirstRender = true;

    async function poll() {
        if (actionId !== currentActionLogsId) return;

        const content = document.getElementById('action-logs-content');

        try {
            const response = await fetch(`${API_BASE}/stacks/actions/${actionId}/logs?offset=${actionLogsPollOffset}`, { headers: authHeaders() });

            if (actionId !== currentActionLogsId) return;

            if (response.status === 401) {
                showLogin();
                return;
            }

            if (response.status === 404) {
                content.innerHTML = '<div class="log-line log-error">Action introuvable — le serveur a peut-être redémarré.</div>';
                return;
            }

            if (!response.ok) {
                // Transient error — retry
                actionLogsPollTimer = setTimeout(poll, 2000);
                return;
            }

            const data = await response.json();
            if (actionId !== currentActionLogsId) return;

            if (data.lines && data.lines.length > 0) {
                if (actionLogsFirstRender) {
                    content.innerHTML = '';
                    actionLogsFirstRender = false;
                }
                for (const line of data.lines) {
                    appendLogLine(content, line);
                }
                actionLogsPollOffset += data.lines.length;
                content.scrollTop = content.scrollHeight;
            }

            if (data.status !== 'running') {
                if (actionLogsFirstRender) {
                    content.innerHTML = '';
                    actionLogsFirstRender = false;
                }
                const statusLine = document.createElement('div');
                statusLine.className = `log-line ${data.status === 'completed' ? 'log-success' : 'log-error'}`;
                statusLine.textContent = `--- ${data.status.toUpperCase()} ---`;
                content.appendChild(statusLine);
                content.scrollTop = content.scrollHeight;
                return; // done
            }
        } catch (e) {
            // Network error — retry
        }

        actionLogsPollTimer = setTimeout(poll, 1000);
    }

    poll();
}

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

async function editStackEnv(repoName) {
    const modal = document.getElementById('stack-env-modal');
    const title = document.getElementById('stack-env-title');
    const textarea = document.getElementById('stack-env-content');
    const saveBtn = document.getElementById('stack-env-save');
    
    title.textContent = `Edit .env: ${repoName}`;
    textarea.value = 'Loading...';
    textarea.disabled = true;
    saveBtn.dataset.repo = repoName;
    
    modal.classList.add('active');
    
    try {
        const data = await apiGet(`/stacks/${encodeURIComponent(repoName)}/env`);
        if (data === null) {
            textarea.value = '# .env file not found or failed to load\n# You can create it here\n';
        } else {
            textarea.value = data.content || '';
        }
        textarea.disabled = false;
        textarea.focus();
    } catch (e) {
        textarea.value = `# Error loading .env file: ${e.message || 'Unknown error'}\n# You can create it here\n`;
        textarea.disabled = false;
    }
}

async function saveStackEnv() {
    const modal = document.getElementById('stack-env-modal');
    const textarea = document.getElementById('stack-env-content');
    const saveBtn = document.getElementById('stack-env-save');
    const repoName = saveBtn.dataset.repo;
    
    saveBtn.disabled = true;
    saveBtn.innerHTML = '<span class="btn-loading"></span> Saving...';
    
    try {
        const result = await apiPut(`/stacks/${encodeURIComponent(repoName)}/env`, {
            content: textarea.value
        });
        
        if (result.success) {
            showNotification('success', 'File saved successfully');
            closeStackEnvModal();
        } else {
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
    document.getElementById('stack-env-modal').classList.remove('active');
}

function formatRelativeTime(isoString) {
    if (!isoString) return '';
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 30) return `${diffDays}d ago`;
    return date.toLocaleDateString();
}

// ============== Utilities ==============

function normalizeVersion(v) {
    if (!v) return '';
    return v.replace(/^v/, '');
}

/** Convert repo name to Docker stack name (mirrors deploy-service.sh get_stack_name). */
function repoToStackName(name) {
    return name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
}

function formatImageName(image) {
    if (!image) return '';
    const escaped = escapeHtml(image);
    const shaIndex = escaped.indexOf('@sha256:');
    if (shaIndex === -1) return escaped;
    const name = escaped.substring(0, shaIndex);
    const sha = escaped.substring(shaIndex);
    return `${name}<span class="image-sha" title="${sha}">${sha}</span>`;
}

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function formatNumber(num) {
    if (num === undefined || num === null) return '-';
    return num.toLocaleString();
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatMemory(mb) {
    if (mb === 0 || mb == null) return '';
    if (mb >= 1024) {
        return (mb / 1024).toFixed(1) + ' GB';
    }
    return Math.round(mb) + ' MB';
}

function formatTime(isoString) {
    if (!isoString) return '-';
    const date = new Date(isoString);
    return date.toLocaleTimeString('en-US', { 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit',
        hour12: false 
    });
}

function formatDateTime(isoString) {
    if (!isoString) return '-';
    const date = new Date(isoString);
    return date.toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
}

// Close modal on escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeModal();
    }
});

// Close modal on backdrop click
document.getElementById('container-modal').addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        closeModal();
    }
});

// Enter key to search with AI
document.getElementById('ai-query').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        aiSearchLogs();
    }
});

// AI search enter key
document.getElementById('ai-query').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        aiSearchLogs();
    }
});

// ============== Terminal ==============

let terminalInstance = null;
let terminalFitAddon = null;
let terminalSocket = null;
let terminalInitialized = false;
let terminalDataDisposable = null;
let terminalResizeDisposable = null;

function initTerminal() {
    if (terminalInitialized) return;

    const container = document.getElementById('terminal-container');
    if (!container) return;

    terminalInstance = new Terminal({
        cursorBlink: true,
        fontSize: 14,
        fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
        theme: {
            background: '#0a0e14',
            foreground: '#e6e6e6',
            cursor: '#00d4aa',
            cursorAccent: '#0a0e14',
            selectionBackground: 'rgba(0, 212, 170, 0.3)',
            black: '#0a0e14',
            red: '#ef4444',
            green: '#22c55e',
            yellow: '#f59e0b',
            blue: '#3b82f6',
            magenta: '#8b5cf6',
            cyan: '#00d4aa',
            white: '#e6e6e6',
            brightBlack: '#6e7681',
            brightRed: '#f87171',
            brightGreen: '#4ade80',
            brightYellow: '#fbbf24',
            brightBlue: '#60a5fa',
            brightMagenta: '#a78bfa',
            brightCyan: '#2dd4bf',
            brightWhite: '#f5f5f5',
        },
        scrollback: 10000,
        convertEol: true,
    });

    terminalFitAddon = new FitAddon.FitAddon();
    terminalInstance.loadAddon(terminalFitAddon);
    terminalInstance.loadAddon(new WebLinksAddon.WebLinksAddon());

    terminalInstance.open(container);
    terminalFitAddon.fit();

    terminalInitialized = true;

    window.addEventListener('resize', () => {
        if (currentView === 'terminal' && terminalFitAddon) {
            terminalFitAddon.fit();
        }
    });

    const resizeObserver = new ResizeObserver(() => {
        if (currentView === 'terminal' && terminalFitAddon) {
            terminalFitAddon.fit();
        }
    });
    resizeObserver.observe(container);

    connectTerminal();
}

function connectTerminal() {
    if (terminalSocket && terminalSocket.readyState === WebSocket.OPEN) return;

    updateTerminalStatus('connecting');

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const cols = terminalInstance ? terminalInstance.cols : 80;
    const rows = terminalInstance ? terminalInstance.rows : 24;
    const token = getAuthToken() || '';
    terminalSocket = new WebSocket(
        `${protocol}//${window.location.host}/api/terminal/ws?cols=${cols}&rows=${rows}&token=${encodeURIComponent(token)}`
    );

    terminalSocket.binaryType = 'arraybuffer';

    terminalSocket.onopen = () => {
        updateTerminalStatus('connected');
        if (terminalDataDisposable) terminalDataDisposable.dispose();
        if (terminalResizeDisposable) terminalResizeDisposable.dispose();
        terminalDataDisposable = terminalInstance.onData((data) => {
            if (terminalSocket && terminalSocket.readyState === WebSocket.OPEN) {
                terminalSocket.send(data);
            }
        });
        terminalResizeDisposable = terminalInstance.onResize(({ cols, rows }) => {
            if (terminalSocket && terminalSocket.readyState === WebSocket.OPEN) {
                terminalSocket.send(JSON.stringify({ type: 'resize', cols, rows }));
            }
        });
        terminalInstance.focus();
    };

    terminalSocket.onmessage = (event) => {
        if (event.data instanceof ArrayBuffer) {
            terminalInstance.write(new Uint8Array(event.data));
        } else {
            terminalInstance.write(event.data);
        }
    };

    terminalSocket.onclose = (event) => {
        updateTerminalStatus('disconnected');
        if (!event.wasClean) {
            terminalInstance.writeln('\r\n\x1b[31m[Connection lost. Click Reconnect to restore.]\x1b[0m');
        }
    };

    terminalSocket.onerror = () => {
        updateTerminalStatus('disconnected');
    };
}

function reconnectTerminal() {
    if (terminalSocket) {
        terminalSocket.close();
        terminalSocket = null;
    }
    if (terminalInstance) {
        terminalInstance.clear();
    }
    connectTerminal();
}

function updateTerminalStatus(status) {
    const el = document.getElementById('terminal-status');
    if (!el) return;
    el.className = `terminal-status ${status}`;
    el.textContent = status.charAt(0).toUpperCase() + status.slice(1);
}
