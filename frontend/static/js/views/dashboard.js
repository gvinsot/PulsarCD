/**
 * PulsarCD frontend: Dashboard view: stats, recurring errors and charts.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

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
        loadSystemErrors().catch(e => console.error('Failed to load system errors:', e)),
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
        <div class="recurring-error-item" data-click="showRecurringErrorDetail" data-args="${uiArgs(i)}">
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
            : '<span class="text-muted">Unknown</span>';
    document.getElementById('recurring-error-modal-services').innerHTML =
        svcs.length
            ? svcs.map(s => `<span class="rerr-service-chip">${escapeHtml(s)}</span>`).join('')
            : '<span class="text-muted">Unknown</span>';
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

async function loadSystemErrors() {
    const data = await apiGet('/dashboard/system-errors?limit=10');
    const el = document.getElementById('system-errors-list');
    if (!el) return;

    const card = el.closest('.system-errors-card');
    if (!data || data.length === 0) {
        if (card) card.style.display = 'none';
        return;
    }

    const categoryLabels = {
        llm: 'LLM',
        mcp: 'MCP Discovery',
        mcp_tool: 'MCP Tool',
        agent: 'Agent',
        opensearch: 'OpenSearch',
        error_detector: 'Error Detector',
        host_agent: 'Host Agent',
    };

    el.innerHTML = data.map(e => {
        const cat = categoryLabels[e.category] || e.category;
        const age = formatRelativeTime(e.timestamp);
        const ctx = [];
        if (e.model) ctx.push(e.model);
        if (e.server) ctx.push(e.server);
        if (e.tool) ctx.push(e.tool);
        if (e.action) ctx.push(e.action);
        if (e.repo) ctx.push(e.repo);
        if (e.stage) ctx.push(e.stage);
        const ctxStr = ctx.length ? ' — ' + escapeHtml(ctx.join(', ')) : '';
        return `
        <div class="system-error-item">
            <div class="system-error-header">
                <span class="system-error-category">${escapeHtml(cat)}</span>
                <span class="system-error-type">${escapeHtml(e.error_type || '')}</span>
                <span class="system-error-age">${age}</span>
            </div>
            <div class="system-error-message">${escapeHtml((e.error || '').substring(0, 200))}${ctxStr}</div>
        </div>`;
    }).join('');
    if (card) card.style.display = '';
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
