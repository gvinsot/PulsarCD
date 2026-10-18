/**
 * PulsarCD frontend: Swarm service status and service logs modals.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

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

// Swarm task state -> colour class in the task tables (docker service ps).
const _TASK_STATE_CLASSES = {
    running: 'task-state-running',
    complete: 'task-state-complete',
    ready: 'task-state-pending',
    starting: 'task-state-pending',
    preparing: 'task-state-pending',
    assigned: 'task-state-pending',
    accepted: 'task-state-pending',
    pending: 'task-state-pending',
    new: 'task-state-pending',
    failed: 'task-state-failed',
    rejected: 'task-state-failed',
    orphaned: 'task-state-failed',
};

function taskStateClass(state) {
    return _TASK_STATE_CLASSES[state] || 'task-state-other';
}

function renderServiceStatusTable(tasks, serviceName) {
    const content = document.getElementById('service-status-content');

    if (!tasks || tasks.length === 0) {
        content.innerHTML = '<div class="empty-placeholder">No task information available for this service</div>';
        return;
    }

    let html = `<div class="task-table-wrap">`;
    html += `<table class="task-table">`;
    html += `<thead><tr>`;
    html += `<th>ID</th>`;
    html += `<th>Image</th>`;
    html += `<th>Node</th>`;
    html += `<th>Desired State</th>`;
    html += `<th>Current State</th>`;
    html += `<th>Error</th>`;
    html += `<th>Updated</th>`;
    html += `</tr></thead><tbody>`;

    for (const task of tasks) {
        const errorText = task.error || task.message || '';
        const updatedAt = task.updated_at ? new Date(task.updated_at).toLocaleString() : '';
        const taskIdShort = (task.id || '').substring(0, 12);
        const imageShort = (task.image || '').replace(/^.*\//, '').replace(/@sha256:.*$/, '');

        html += `<tr>`;
        html += `<td class="task-cell-id" title="${escapeHtml(task.id || '')}">${escapeHtml(taskIdShort)}</td>`;
        html += `<td class="task-cell-image" title="${escapeHtml(task.image || '')}">${escapeHtml(imageShort)}</td>`;
        html += `<td>${escapeHtml(task.node || '')}</td>`;
        html += `<td class="${taskStateClass(task.desired_state)}">${escapeHtml(task.desired_state || '')}</td>`;
        html += `<td class="${taskStateClass(task.state)}">${escapeHtml(task.state || '')}</td>`;
        html += `<td class="task-cell-error">${escapeHtml(errorText)}</td>`;
        html += `<td class="task-cell-updated">${escapeHtml(updatedAt)}</td>`;
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
    
    let html = `<div class="task-table-wrap">`;
    html += `<div class="task-table-notice">`;
    html += `&#9888; Service logs unavailable — showing task status (docker service ps)</div>`;
    html += `<table class="task-table">`;
    html += `<thead><tr>`;
    html += `<th>ID</th>`;
    html += `<th>Node</th>`;
    html += `<th>Desired State</th>`;
    html += `<th>Current State</th>`;
    html += `<th>Error</th>`;
    html += `<th>Updated</th>`;
    html += `</tr></thead><tbody>`;

    for (const task of tasks) {
        const errorText = task.error || task.message || '';
        const updatedAt = task.updated_at ? new Date(task.updated_at).toLocaleString() : '';
        const taskIdShort = (task.id || '').substring(0, 12);

        html += `<tr>`;
        html += `<td class="task-cell-id">${escapeHtml(taskIdShort)}</td>`;
        html += `<td>${escapeHtml(task.node || '')}</td>`;
        html += `<td class="${taskStateClass(task.desired_state)}">${escapeHtml(task.desired_state || '')}</td>`;
        html += `<td class="${taskStateClass(task.state)}">${escapeHtml(task.state || '')}</td>`;
        html += `<td class="task-cell-error">${escapeHtml(errorText)}</td>`;
        html += `<td class="task-cell-updated">${escapeHtml(updatedAt)}</td>`;
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
