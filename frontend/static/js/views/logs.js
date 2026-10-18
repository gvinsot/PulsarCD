/**
 * PulsarCD frontend: Logs view: AI search, recent queries, results, agent tasks and paging.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

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
        <span class="recent-query-item" data-click="useRecentQuery" data-args="${uiArgs(idx)}" title="${escapeHtml(q.question)}">
            ${escapeHtml(q.question.length > 40 ? q.question.substring(0, 40) + '...' : q.question)}
            <span class="delete-query" data-click="deleteRecentQuery" data-args="${uiArgs(idx)}" data-click-stop>✕</span>
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
        <tr class="${getLogRowClass(log)} log-row" data-log-index="${index}" data-click="toggleLogRow" data-args="${uiArgs(index)}">
            <td class="col-time">${formatTime(log.timestamp)}</td>
            <td class="col-source" title="${escapeHtml(log.host)} / ${escapeHtml(log.container_name)}">
                <span class="source-host">${escapeHtml(log.host)}</span>
                <span class="source-container">${escapeHtml(log.container_name)}</span>
            </td>
            <td class="col-level">${log.level ? `<span class="log-level ${log.level.toLowerCase()}">${escapeHtml(log.level)}</span>` : ''}</td>
            <td class="col-message"><div class="message-truncate">${escapeHtml(log.message)}</div></td>
        </tr>
        <tr class="log-expand-row start-hidden" id="log-expand-${index}">
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
                            <button class="btn btn-task-create" data-click="openCreateTaskModal" data-args="${uiArgs('search', index)}" data-click-stop>🤖 Create Agent Task</button>
                        </div>
                    </div>
                </div>
            </td>
        </tr>
    `).join('');
    adoptStartHidden(tbody);

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

// OpenSearch refuses a result window past index.max_result_window, and the API
// now validates `from` against the same bound instead of clamping it silently.
// Cap the pager here too, otherwise "Next" walks past the last reachable page
// and the request comes back as a 422.
const LOGS_MAX_RESULT_WINDOW = 10000;

function logsTotalPages() {
    const reachable = Math.min(totalLogs, LOGS_MAX_RESULT_WINDOW);
    return Math.ceil(reachable / logsPageSize);
}

function updatePagination() {
    const totalPages = logsTotalPages();
    const capped = totalLogs > LOGS_MAX_RESULT_WINDOW;
    document.getElementById('page-info').textContent =
        `Page ${logsPage + 1} of ${totalPages || 1}${capped ? ' (search window limit)' : ''}`;
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
    if (logsPage < logsTotalPages() - 1 && lastSearchParams) {
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
