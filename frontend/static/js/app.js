/**
 * LogsCrawler Frontend Application
 * Professional Docker Log Analytics Dashboard
 */

// API Base URL
const API_BASE = '/api';

// State
let currentView = 'dashboard';
let currentContainer = null;
let charts = {};
let logsPage = 0;
let logsPageSize = 100;
let totalLogs = 0;

// ============== Authentication ==============

function getAuthToken() { return localStorage.getItem('logscrawler_token'); }
function setAuthToken(token) { localStorage.setItem('logscrawler_token', token); }
function clearAuthToken() { localStorage.removeItem('logscrawler_token'); }

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
        hideLogin();
        loadDashboard();
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
                hideLogin();
                loadDashboard();
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

// ============== Initialization ==============

document.addEventListener('DOMContentLoaded', () => {
    initLoginForm();
    initNavigation();
    initModalTabs();
    checkAuth();
});

// ============== Mobile Menu ==============

function toggleMobileMenu() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    
    sidebar.classList.toggle('mobile-open');
    overlay.classList.toggle('active');
    
    // Prevent body scroll when menu is open
    document.body.style.overflow = sidebar.classList.contains('mobile-open') ? 'hidden' : '';
}

function closeMobileMenu() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    
    sidebar.classList.remove('mobile-open');
    overlay.classList.remove('active');
    document.body.style.overflow = '';
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

function switchView(view) {
    currentView = view;
    
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

const RECENT_QUERIES_KEY = 'logscrawler_recent_queries';
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
                        <div class="analysis-item ai-assessment">
                            <span class="analysis-label">🤖 AI Assessment:</span>
                            <span class="analysis-value loading" id="search-ai-${index}">Analyzing...</span>
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
        const aiEl = document.getElementById(`search-ai-${index}`);
        
        console.log('Similar element:', similarEl, 'AI element:', aiEl);
        
        if (similarEl && similarEl.classList.contains('loading')) {
            console.log('Loading similar count for index', index);
            loadSimilarCount(index, similarEl);
        }
        if (aiEl && aiEl.classList.contains('loading')) {
            console.log('Loading AI assessment for index', index);
            loadAIAssessment(index, aiEl);
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
    try {
        const log = window.currentLogResults[index];
        const result = await apiPost('/logs/ai-analyze', {
            message: log.message,
            level: log.level,
            container_name: log.container_name
        });
        
        element.classList.remove('loading');
        if (result && result.assessment) {
            element.innerHTML = `
                <span class="assessment-badge ${result.severity}">${result.severity}</span>
                <span class="assessment-text">${escapeHtml(result.assessment)}</span>
            `;
        } else {
            element.textContent = 'Could not analyze';
        }
    } catch (e) {
        element.classList.remove('loading');
        element.textContent = 'AI unavailable';
        element.classList.add('error');
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
    
    // Load charts (with error handling for each)
    await Promise.all([
        loadErrorsChart().catch(e => console.error('Failed to load errors chart:', e)),
        loadHttpChart().catch(e => console.error('Failed to load http chart:', e)),
        loadCpuChart().catch(e => console.error('Failed to load cpu chart:', e)),
        loadGpuChart().catch(e => console.error('Failed to load gpu chart:', e)),
        loadMemoryChart().catch(e => console.error('Failed to load memory chart:', e)),
        loadVramChart().catch(e => console.error('Failed to load vram chart:', e))
    ]);
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
                max: isPercent ? 100 : undefined
            }
        }
    };
}

// ============== Containers ==============

// Local storage keys
const CONTAINERS_FILTER_KEY = 'logscrawler_containers_filter';
const CONTAINERS_GROUPS_KEY = 'logscrawler_containers_groups';

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
                
                topLevelHtml += `
                    <div class="container-item" onclick="openContainer('${escapeHtml(c.host)}', '${escapeHtml(c.id)}', ${JSON.stringify(c).replace(/"/g, '&quot;')})">
                        <div class="container-info">
                            <span class="container-status ${c.status}"></span>
                            <div>
                                <div class="container-name">${containerNameHtml}</div>
                                <div class="container-image">${escapeHtml(c.image)}${containerAge ? ` <span class="container-age">• ${containerAge}</span>` : ''}</div>
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
                            </span>
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
                        <div class="analysis-item ai-assessment">
                            <span class="analysis-label">🤖 AI Assessment:</span>
                            <span class="analysis-value loading" id="container-ai-${index}">Analyzing...</span>
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
        const aiEl = document.getElementById(`container-ai-${index}`);
        
        console.log('Similar element:', similarEl, 'AI element:', aiEl);
        
        if (similarEl && similarEl.classList.contains('loading')) {
            console.log('Loading similar count for index', index);
            loadContainerLogSimilar(index, similarEl);
        }
        if (aiEl && aiEl.classList.contains('loading')) {
            console.log('Loading AI assessment for index', index);
            loadContainerLogAI(index, aiEl);
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

async function loadContainerLogAI(index, element) {
    try {
        const log = currentContainerLogs[index];
        const result = await apiPost('/logs/ai-analyze', {
            message: log.message,
            level: log.level || '',
            container_name: currentContainer.data.name
        });
        
        element.classList.remove('loading');
        if (result && result.assessment) {
            element.innerHTML = `
                <span class="assessment-badge ${result.severity}">${result.severity}</span>
                <span class="assessment-text">${escapeHtml(result.assessment)}</span>
            `;
        } else {
            element.textContent = 'Could not analyze';
        }
    } catch (e) {
        element.classList.remove('loading');
        element.textContent = 'AI unavailable';
        element.classList.add('error');
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

async function loadStacks() {
    // Check GitHub status
    const status = await apiGet('/stacks/status');
    const statusText = document.getElementById('github-status-text');
    const statusEl = document.getElementById('github-status');
    
    if (status && status.configured) {
        statusText.textContent = status.username ? `@${status.username}` : 'Connected';
        statusEl.classList.add('connected');
        statusEl.classList.remove('disconnected');
    } else {
        statusText.textContent = 'Not configured';
        statusEl.classList.add('disconnected');
        statusEl.classList.remove('connected');
        document.getElementById('stacks-list').innerHTML = `
            <div class="stacks-not-configured">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="48" height="48">
                    <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22"/>
                </svg>
                <h3>GitHub Integration Not Configured</h3>
                <p>Set the following environment variables to enable:</p>
                <code>LOGSCRAWLER_GITHUB__TOKEN</code><br>
                <code>LOGSCRAWLER_GITHUB__USERNAME</code>
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
    const [reposData, tagsData, containersData, hostMetrics, pipelineData, autoBuildData] = await Promise.all([
        apiGet('/stacks/repos'),
        apiGet('/stacks/deployed-tags'),
        apiGet('/containers/grouped?refresh=true&group_by=stack'),
        apiGet('/hosts/metrics'),
        apiGet('/stacks/pipeline/status'),
        apiGet('/stacks/auto-build/status'),
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
 * Lightweight update: fetches only container states and updates DOM in-place
 * without re-rendering the entire stacks list.
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
            apiGet('/containers/states'),
            apiGet('/hosts/metrics'),
            apiGet('/stacks/pipeline/status'),
            apiGet('/stacks/auto-build/status'),
        ];
        if (fetchVersions) {
            promises.push(apiGet('/stacks/deployed-tags'));
        }

        const results = await Promise.all(promises);
        const statesData = results[0];
        const hostMetrics = results[1];
        const pipelineData = results[2];
        const autoBuildData = results[3];
        const tagsData = fetchVersions ? results[4] : null;

        if (!statesData) return;
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
        if (needsRerender) {
            renderStacksList();
            return;
        }

        // Build a map: stackName -> serviceName -> containers
        const newContainersByStack = {};
        for (const c of statesData) {
            let stackName = null;
            let serviceName = null;

            // Try swarm labels first
            if (c.labels) {
                stackName = c.labels['com.docker.swarm.stack.namespace'] || null;
                serviceName = c.labels['com.docker.swarm.service.name'] || null;
            }

            // Extract from name if needed
            if (!stackName && c.name && c.name.includes('.')) {
                const mainPart = c.name.split('.')[0];
                if (mainPart.includes('_')) {
                    const parts = mainPart.split('_', 1);
                    stackName = parts[0];
                    if (!serviceName) {
                        serviceName = mainPart;
                    }
                }
            }

            if (!stackName) continue; // skip non-stack containers

            if (!newContainersByStack[stackName]) {
                newContainersByStack[stackName] = {};
            }
            if (!serviceName) serviceName = c.name;
            if (!newContainersByStack[stackName][serviceName]) {
                newContainersByStack[stackName][serviceName] = [];
            }
            newContainersByStack[stackName][serviceName].push(c);
        }

        // Update stacksContainers for each known repo
        for (const repo of stacksRepos) {
            const stackName = repoToStackName(repo.name);
            const oldStackContainers = stacksContainers[stackName] || {};
            const newStackContainers = newContainersByStack[stackName] || {};

            // Merge: keep existing service keys (from swarm manager), update containers
            const mergedServices = { ...oldStackContainers };
            for (const [svc, containers] of Object.entries(newStackContainers)) {
                mergedServices[svc] = containers;
            }
            // Clear services that had containers but now have none
            for (const svc of Object.keys(mergedServices)) {
                if (!newStackContainers[svc] && mergedServices[svc].length > 0) {
                    mergedServices[svc] = [];
                }
            }
            stacksContainers[stackName] = mergedServices;

            // Update DOM in-place for this stack
            updateStackDom(repo.name, stackName, mergedServices);
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

    // Remove containers that no longer exist
    for (const [id, el] of Object.entries(existingById)) {
        if (!newIds.has(id)) {
            el.remove();
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
            const statMinis = existingEl.querySelectorAll('.stat-mini');
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
const STACKS_EXPANDED_KEY = 'logscrawler_stacks_expanded';

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
    
    // Use containers-grouped class for similar styling to Computers view
    listEl.className = 'containers-grouped';
    
    listEl.innerHTML = stacksRepos.map(repo => {
        const deployedTag = stacksDeployedTags[repo.name];
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
        let versionStep = 'idle', buildStep = 'idle', testStep = 'idle', deployStep = 'idle';

        // Whether build/test steps were part of this pipeline
        const hadBuild = pipeline ? !!pipeline.build_action_id : false;
        const hadTest = pipeline ? !!pipeline.test_action_id : false;

        if (pipeline && pipeline.status === 'running') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = cs === 1 ? 'running' : (hadBuild && cs > 1 ? 'success' : (cs > 1 ? 'idle' : 'pending'));
            testStep = cs === 2 ? 'running' : (hadTest && cs > 2 ? 'success' : (hadBuild && cs > 2 ? 'success' : (cs > 2 ? 'idle' : 'pending')));
            deployStep = cs === 3 ? 'running' : 'pending';
        } else if (pipeline && pipeline.status === 'failed') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = cs === 1 ? 'failed' : (hadBuild && cs > 1 ? 'success' : (cs > 1 ? 'idle' : 'pending'));
            testStep = cs === 2 ? 'failed' : (hadTest && cs > 2 ? 'success' : (hadBuild && cs > 2 ? 'success' : (cs > 2 ? 'idle' : 'pending')));
            deployStep = cs === 3 ? 'failed' : (cs > 3 ? 'success' : 'pending');
        } else if (pipeline && pipeline.stage === 'done') {
            versionStep = 'success';
            buildStep = hadBuild ? 'success' : 'idle';
            testStep = hadTest ? 'success' : (hadBuild ? 'success' : 'idle');
            deployStep = 'success';
        } else if (pipeline && pipeline.status === 'success') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = hadBuild && cs >= 1 ? 'success' : (cs >= 1 ? 'idle' : 'pending');
            testStep = hadTest && cs >= 2 ? 'success' : (hadBuild && cs >= 2 ? 'success' : (cs >= 2 ? 'idle' : 'pending'));
            deployStep = cs >= 3 ? 'success' : 'pending';
        } else if (hasUpdate) {
            versionStep = 'success'; buildStep = 'success'; testStep = 'success'; deployStep = 'pending';
        } else if (untaggedCount > 0) {
            versionStep = 'pending'; buildStep = 'pending'; testStep = 'pending'; deployStep = 'pending';
        } else if (isDeployed) {
            versionStep = 'success'; buildStep = 'success'; testStep = 'success'; deployStep = 'success';
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
                // serviceName is the full swarm service name (e.g., "logscrawler_backend")
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
                    
                    containersHtml += `
                        <div class="container-item" onclick="openContainer('${escapeHtml(c.host)}', '${escapeHtml(c.id)}', ${JSON.stringify(c).replace(/"/g, '&quot;')})">
                            <div class="container-info">
                                <span class="container-status ${c.status}"></span>
                                <div>
                                    <div class="container-name">${escapeHtml(c.name)} <span style="color: var(--text-muted); font-size: 0.85em;">(${escapeHtml(c.host)})</span></div>
                                    <div class="container-image">${escapeHtml(c.image)}${containerAge ? ` <span class="container-age">• ${containerAge}</span>` : ''}</div>
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
                                </span>
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
        const stepIcon = (state) => state === 'success' ? checkSvg : state === 'failed' ? xSvg : state === 'running' ? spinnerSvg : state === 'pending' ? pendingSvg : idleSvg;

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
                    ${hasUpdate ? `<span class="stack-badge update-available" title="New version available">${escapeHtml(latestBuilt)}</span>` : ''}
                    ${untaggedCount > 0 ? `<span class="stack-badge" style="background: var(--warning-bg, #664d03); color: var(--warning-text, #ffcd39);" title="${untaggedCount} untagged commit${untaggedCount > 1 ? 's' : ''}">${untaggedCount} untagged</span>` : ''}
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
                        <span class="pipeline-arrow">\u2192</span>
                        <div class="pipeline-step step-${buildStep}" onclick="event.stopPropagation(); pipelineStepClick('${escapeHtml(repo.name)}', '${escapeHtml(repo.ssh_url)}', 'build')" title="Build">
                            ${stepIcon(buildStep)}
                            <span>Build</span>
                            ${buildActionId ? `<span class="pipeline-log-btn" onclick="event.stopPropagation(); openActionLogs('${buildActionId}', 'Build Logs', '${escapeHtml(repo.name)}')" title="View build logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                        </div>
                        <span class="pipeline-arrow">\u2192</span>
                        <div class="pipeline-step step-${testStep}" onclick="event.stopPropagation(); pipelineStepClick('${escapeHtml(repo.name)}', '${escapeHtml(repo.ssh_url)}', 'test')" title="Test">
                            ${stepIcon(testStep)}
                            <span>Test</span>
                            ${testActionId ? `<span class="pipeline-log-btn" onclick="event.stopPropagation(); openActionLogs('${testActionId}', 'Test Logs', '${escapeHtml(repo.name)}')" title="View test logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                        </div>
                        <span class="pipeline-arrow">\u2192</span>
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

async function buildStack(repoName, sshUrl) {
    currentBuildRepo = repoName;
    currentBuildSshUrl = sshUrl;
    
    const modal = document.getElementById('stack-build-modal');
    const title = document.getElementById('stack-build-title');
    const branchSelect = document.getElementById('build-branch-select');
    const versionInput = document.getElementById('build-version-input');
    const commitInput = document.getElementById('build-commit-input');
    
    title.textContent = `Build: ${repoName}`;
    branchSelect.innerHTML = '<option value="">Loading branches...</option>';
    commitInput.value = '';
    versionInput.value = '1.0';
    
    // Reset to branch mode
    document.querySelector('input[name="build-source"][value="branch"]').checked = true;
    toggleBuildSource('branch');
    
    modal.classList.add('active');
    
    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        branchSelect.innerHTML = '<option value="">Failed to parse repository URL</option>';
        return;
    }
    const owner = ownerMatch[1];
    
    // Load branches
    try {
        const data = await apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/branches`);
        if (data && data.branches && data.branches.length > 0) {
            branchSelect.innerHTML = data.branches.map(b => 
                `<option value="${escapeHtml(b.name)}" ${b.name === 'main' || b.name === 'master' ? 'selected' : ''}>
                    ${escapeHtml(b.name)}${b.protected ? ' 🔒' : ''}
                </option>`
            ).join('');
        } else {
            branchSelect.innerHTML = '<option value="main">main</option>';
        }
    } catch (e) {
        console.error('Failed to load branches:', e);
        branchSelect.innerHTML = '<option value="main">main (default)</option>';
    }
}

function toggleBuildSource(source) {
    const branchGroup = document.getElementById('build-branch-group');
    const commitGroup = document.getElementById('build-commit-group');
    
    if (source === 'branch') {
        branchGroup.style.display = 'block';
        commitGroup.style.display = 'none';
    } else {
        branchGroup.style.display = 'none';
        commitGroup.style.display = 'block';
    }
}

function closeBuildModal() {
    document.getElementById('stack-build-modal').classList.remove('active');
    currentBuildRepo = null;
    currentBuildSshUrl = null;
}

async function submitBuild() {
    if (!currentBuildRepo || !currentBuildSshUrl) return;
    
    const submitBtn = document.getElementById('stack-build-submit');
    const source = document.querySelector('input[name="build-source"]:checked').value;
    const version = document.getElementById('build-version-input').value || '1.0';
    
    let branch = null;
    let commit = null;
    
    if (source === 'branch') {
        branch = document.getElementById('build-branch-select').value;
    } else {
        commit = document.getElementById('build-commit-input').value.trim();
        if (!commit) {
            showNotification('error', 'Please enter a commit ID');
            return;
        }
        // Basic validation
        if (!/^[a-fA-F0-9]{7,40}$/.test(commit)) {
            showNotification('error', 'Invalid commit ID format. Expected 7-40 hexadecimal characters.');
            return;
        }
    }
    
    // Disable button and show loading
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="btn-loading"></span> Starting...';
    
    try {
        let url = `/stacks/build?repo_name=${encodeURIComponent(currentBuildRepo)}&ssh_url=${encodeURIComponent(currentBuildSshUrl)}&version=${encodeURIComponent(version)}`;
        if (branch) {
            url += `&branch=${encodeURIComponent(branch)}`;
        }
        if (commit) {
            url += `&commit=${encodeURIComponent(commit)}`;
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

async function testStack(repoName, sshUrl) {
    currentTestRepo = repoName;
    currentTestSshUrl = sshUrl;

    const modal = document.getElementById('stack-test-modal');
    const title = document.getElementById('stack-test-title');
    const branchSelect = document.getElementById('test-branch-select');
    const commitInput = document.getElementById('test-commit-input');

    title.textContent = `Test: ${repoName}`;
    branchSelect.innerHTML = '<option value="">Loading branches...</option>';
    commitInput.value = '';

    // Reset to branch mode
    document.querySelector('input[name="test-source"][value="branch"]').checked = true;
    toggleTestSource('branch');

    modal.classList.add('active');

    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        branchSelect.innerHTML = '<option value="">Failed to parse repository URL</option>';
        return;
    }
    const owner = ownerMatch[1];

    // Load branches
    try {
        const data = await apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/branches`);
        if (data && data.branches && data.branches.length > 0) {
            branchSelect.innerHTML = data.branches.map(b =>
                `<option value="${escapeHtml(b.name)}" ${b.name === 'main' || b.name === 'master' ? 'selected' : ''}>
                    ${escapeHtml(b.name)}${b.protected ? ' \uD83D\uDD12' : ''}
                </option>`
            ).join('');
        } else {
            branchSelect.innerHTML = '<option value="main">main</option>';
        }
    } catch (e) {
        console.error('Failed to load branches:', e);
        branchSelect.innerHTML = '<option value="main">main (default)</option>';
    }
}

function toggleTestSource(source) {
    const branchGroup = document.getElementById('test-branch-group');
    const commitGroup = document.getElementById('test-commit-group');

    if (source === 'branch') {
        branchGroup.style.display = 'block';
        commitGroup.style.display = 'none';
    } else {
        branchGroup.style.display = 'none';
        commitGroup.style.display = 'block';
    }
}

function closeTestModal() {
    document.getElementById('stack-test-modal').classList.remove('active');
    currentTestRepo = null;
    currentTestSshUrl = null;
}

async function submitTest() {
    if (!currentTestRepo || !currentTestSshUrl) return;

    const submitBtn = document.getElementById('stack-test-submit');
    const source = document.querySelector('input[name="test-source"]:checked').value;

    let branch = null;
    let commit = null;

    if (source === 'branch') {
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
        graphContainer.innerHTML = '<div class="loading-placeholder" style="color: var(--status-error);">Failed to load activity data</div>';
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
let actionLogsEventSource = null;

function openActionLogs(actionId, actionType, repoName) {
    currentActionLogsId = actionId;

    const modal = document.getElementById('action-logs-modal');
    const title = document.getElementById('action-logs-title');
    const content = document.getElementById('action-logs-content');

    title.textContent = `${actionType}: ${repoName}`;
    content.innerHTML = '<div class="loading-placeholder">Loading logs...</div>';

    modal.classList.add('active');

    // Start SSE stream
    startActionLogsStream(actionId);
}

function closeActionLogsModal() {
    stopActionLogsStream();
    document.getElementById('action-logs-modal').classList.remove('active');
    currentActionLogsId = null;
}

function stopActionLogsStream() {
    if (actionLogsEventSource) {
        actionLogsEventSource.close();
        actionLogsEventSource = null;
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

function startActionLogsStream(actionId) {
    stopActionLogsStream();

    const token = getAuthToken();
    const url = `${API_BASE}/stacks/actions/${actionId}/logs/stream?token=${encodeURIComponent(token || '')}`;
    const es = new EventSource(url);
    actionLogsEventSource = es;
    let firstLine = true;

    es.onmessage = (event) => {
        if (actionId !== currentActionLogsId) { es.close(); return; }

        const content = document.getElementById('action-logs-content');
        const data = JSON.parse(event.data);

        if (data.type === 'line') {
            if (firstLine) {
                content.innerHTML = '';
                firstLine = false;
            }
            appendLogLine(content, data.line);
            content.scrollTop = content.scrollHeight;
        } else if (data.type === 'done') {
            const statusLine = document.createElement('div');
            statusLine.className = `log-line ${data.status === 'completed' ? 'log-success' : 'log-error'}`;
            statusLine.textContent = `\n--- ${data.status.toUpperCase()} ---`;
            content.appendChild(statusLine);
            content.scrollTop = content.scrollHeight;
            if (firstLine) {
                content.innerHTML = '';
                content.appendChild(statusLine);
            }
            es.close();
            actionLogsEventSource = null;
        }
    };

    es.onerror = () => {
        // EventSource will auto-reconnect; if modal closed, stop
        if (actionId !== currentActionLogsId) {
            es.close();
            actionLogsEventSource = null;
        }
    };
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
