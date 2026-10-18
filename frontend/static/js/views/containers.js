/**
 * PulsarCD frontend: Computers view: hosts, containers and the container modal.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

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
            <div class="host-header" data-click="toggleHostHeader" data-click-stop>
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
                <div class="host-header-actions" data-click-stop>
                    <button class="btn btn-sm btn-warning" data-click="hostAction" data-args="${uiArgs(topLevel, 'reboot')}" title="Reboot this computer">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <polyline points="23 4 23 10 17 10"/>
                            <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
                        </svg>
                        <span>Reboot</span>
                    </button>
                    <button class="btn btn-sm btn-danger" data-click="hostAction" data-args="${uiArgs(topLevel, 'shutdown')}" title="Shutdown this computer">
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
                    <div class="compose-header" data-click="toggleComposeHeader" data-click-stop>
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
                    <div class="container-item" data-click="openContainer" data-args="${uiArgs(c.host, c.id, c)}">
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
                            <button class="btn btn-sm btn-secondary" data-click="quickAction" data-args="${uiArgs(c.host, c.id, 'restart', c.name)}" data-click-stop>
                                Restart
                            </button>
                            <button class="btn btn-sm btn-danger" data-click="quickAction" data-args="${uiArgs(c.host, c.id, 'remove', c.name)}" data-click-stop>
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

function toggleHostGroup(headerEl) {
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

function toggleComposeGroup(headerEl) {
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
            <div class="log-line ${levelClass}${hidden ? ' hidden' : ''}" data-index="${index}" data-click="toggleContainerLogRow" data-args="${uiArgs(index)}">
                <span class="log-timestamp">${timestamp}</span>
                <span class="log-message-truncate">${displayMessage}</span>
            </div>
            <div class="container-log-expand start-hidden" id="container-log-expand-${index}">
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
                            <button class="btn btn-task-create" data-click="openCreateTaskModal" data-args="${uiArgs('container', index)}" data-click-stop>🤖 Create Agent Task</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }).join('');
    
    logViewer.innerHTML = html;
    adoptStartHidden(logViewer);
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
