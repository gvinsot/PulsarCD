/**
 * PulsarCD frontend: Agent modal: instructions, history and MCP servers.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

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
                        <div class="agent-history-detail" data-click="toggleExpanded">${simpleMarkdown(detail)}</div>
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
        <button class="pagination-btn" ${page <= 1 ? 'disabled' : ''} data-click="loadAgentHistory" data-args="${uiArgs(page - 1)}" title="Previous">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><polyline points="15 18 9 12 15 6"/></svg>
        </button>
        <span class="pagination-info">${page} / ${total} <span class="pagination-total">(${totalEntries})</span></span>
        <button class="pagination-btn" ${page >= total ? 'disabled' : ''} data-click="loadAgentHistory" data-args="${uiArgs(page + 1)}" title="Next">
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
        recurring_detected: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`,
        recurring_cooldown: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
        log_analysis: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>`,
        log_analysis_error: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
        chat: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>`,
        // A tool call the security policy refused (shield icon).
        tool_blocked: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
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
        case 'log_analysis':
            return `Log analysis completed — ${entry.project || ''}`;
        case 'log_analysis_error':
            return `Log analysis failed — ${entry.project || ''}`;
        case 'recurring_detected':
            return `Recurring error detected (${entry.count || 0}x) — ${entry.label || entry.services || entry.projects || ''}`;
        case 'recurring_cooldown':
            return `Recurring error skipped (cooldown) — ${entry.label || entry.services || entry.projects || ''}`;
        case 'chat':
            return `Chat: ${(entry.message || '').substring(0, 60)}`;
        case 'tool_blocked':
            return `Tool call blocked — ${entry.tool || ''}`;
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
    if (entry.type === 'log_analysis' || entry.type === 'log_analysis_error') {
        if (entry.project) parts.push('Project: ' + entry.project);
    }
    if (entry.type === 'tool_blocked') {
        if (entry.reason) parts.push('Reason: ' + entry.reason);
        if (entry.arguments) parts.push('Arguments: ' + entry.arguments);
    }
    if (entry.type === 'recurring_detected' || entry.type === 'recurring_cooldown') {
        const params = [];
        if (entry.projects) params.push('Stack: ' + entry.projects);
        if (entry.services) params.push('Services: ' + entry.services);
        if (entry.count) params.push(entry.count + 'x');
        if (params.length) parts.push(params.join(' / '));
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
            <button class="mcp-server-remove" data-click="removeMCPServer" data-args="${uiArgs(i)}" title="Remove">&times;</button>
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
                <button class="btn btn-sm btn-secondary" data-click="testMCPServer" data-args="${uiArgs(i)}" id="mcp-test-btn-${i}">Test Connection</button>
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
    h = h.replace(/^## (.+)$/gm, '<strong class="md-h2">$1</strong>');
    h = h.replace(/^# (.+)$/gm, '<strong class="md-h1">$1</strong>');
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
