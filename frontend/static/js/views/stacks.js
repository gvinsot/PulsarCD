/**
 * PulsarCD frontend: Stacks view: repositories, polling, rendering and stack removal.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Stacks (GitHub Integration) ==============

let stacksRepos = [];
let stacksDeployedTags = {};
let stacksQaDeployedTags = {};
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
        apiGet('/stacks/buildable'),
    ]);

    if (!reposData || !reposData.repos) {
        listEl.innerHTML = '<div class="error-placeholder">Failed to load repositories</div>';
        return;
    }

    stacksRepos = reposData.repos;
    stacksDeployedTags = (tagsData && tagsData.tags) ? tagsData.tags : {};
    stacksQaDeployedTags = (tagsData && tagsData.qa_tags) ? tagsData.qa_tags : {};
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
const STACKS_POLL_INTERVAL = 2000; // 2 seconds
let _versionPollCounter = 0;
const VERSION_POLL_EVERY = 3; // Re-fetch versions every 3rd poll (~6s)

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
            apiGet('/containers/grouped?group_by=stack'),
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
            const oldQa = JSON.stringify(stacksQaDeployedTags);
            const oldBuilt = JSON.stringify(stacksLatestBuilt);
            stacksDeployedTags = (tagsData.tags) ? tagsData.tags : {};
            stacksQaDeployedTags = (tagsData.qa_tags) ? tagsData.qa_tags : {};
            stacksLatestBuilt = (tagsData.latest_built) ? tagsData.latest_built : {};
            if (JSON.stringify(stacksDeployedTags) !== oldDeployed || JSON.stringify(stacksQaDeployedTags) !== oldQa || JSON.stringify(stacksLatestBuilt) !== oldBuilt) {
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
            const qaStackName = 'qa-' + stackName;
            const newStackContainers = containersData[stackName] || {};
            const newQaStackContainers = containersData[qaStackName] || {};

            // If this stack had no entry in the response at all but had containers before,
            // keep the old data — it's likely a transient fetch failure, not a real removal
            if (stackName in containersData || !(Object.keys(stacksContainers[stackName] || {}).length > 0)) {
                stacksContainers[stackName] = newStackContainers;
            }

            // Track QA stack containers separately. Trigger rerender when the
            // set of QA services changes (added/removed) so the QA section appears.
            if (qaStackName in containersData || !(Object.keys(stacksContainers[qaStackName] || {}).length > 0)) {
                const oldQaKeys = JSON.stringify(Object.keys(stacksContainers[qaStackName] || {}).sort());
                const newQaKeys = JSON.stringify(Object.keys(newQaStackContainers).sort());
                if (oldQaKeys !== newQaKeys) needsRerender = true;
                stacksContainers[qaStackName] = newQaStackContainers;
            }
        }

        if (needsRerender) {
            renderStacksList();
            return;
        }

        // Incremental DOM update for each stack (merge prod + QA services so
        // QA compose-groups also receive live container updates).
        for (const repo of stacksRepos) {
            const stackName = repoToStackName(repo.name);
            const qaStackName = 'qa-' + stackName;
            const merged = Object.assign(
                {},
                stacksContainers[stackName] || {},
                stacksContainers[qaStackName] || {}
            );
            updateStackDom(repo.name, stackName, merged);
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

        const containerListEl = groupEl.querySelector('.container-list');
        if (!containerListEl) return;

        // Identify which service this compose-group corresponds to via data attribute
        const matchedServiceName = groupEl.dataset.service;
        if (!matchedServiceName || !(matchedServiceName in services)) return;
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
        if (el.dataset.containerId) existingById[el.dataset.containerId] = el;
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
                            <div class="container-name text-muted">No running containers</div>
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

// Per-stack sub-section expansion (prod/qa) used when QA is active.
const STACKS_SUBSECTION_KEY = 'pulsarcd_stacks_subsections';

function getAllExpandedStackSubSections() {
    try {
        const stored = localStorage.getItem(STACKS_SUBSECTION_KEY);
        return stored ? JSON.parse(stored) : {};
    } catch {
        return {};
    }
}

function getExpandedStackSubSections(stackName) {
    const all = getAllExpandedStackSubSections();
    return all[stackName] || {};
}

function toggleStackSubSection(stackName, section) {
    const all = getAllExpandedStackSubSections();
    const current = all[stackName] || {};
    // Default state is open (true), so first toggle should close it.
    const wasOpen = current[section] !== false;
    current[section] = !wasOpen;
    all[stackName] = current;
    try {
        localStorage.setItem(STACKS_SUBSECTION_KEY, JSON.stringify(all));
    } catch (e) {
        console.error('Failed to save stack subsections:', e);
    }

    const bodyEl = document.getElementById(`stack-subsection-${section}-${stackName}`);
    if (bodyEl) {
        bodyEl.style.display = current[section] ? 'block' : 'none';
        const wrapper = bodyEl.parentElement;
        if (wrapper) wrapper.classList.toggle('collapsed', !current[section]);
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
        const qaStackName = 'qa-' + stackName;
        const stackContainers = stacksContainers[stackName] || {};
        const qaStackContainers = stacksContainers[qaStackName] || {};
        const qaDeployedTag = (pipelineData && pipelineData.stages && pipelineData.stages.qa && pipelineData.stages.qa.current_version)
            ? pipelineData.stages.qa.current_version
            : stacksQaDeployedTags[repo.name];
        const isQaDeployed = !!qaDeployedTag || Object.keys(qaStackContainers).length > 0;
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
        // Stage order: build → test → (qa) → deploy → done. QA is skipped unless enabled.
        const stageOrder = { build: 1, test: 2, qa: 3, deploy: 4, done: 5 };
        const isBuildable = stacksBuildable[repo.name] !== false;  // default true if unknown
        // QA stage is enabled per-project via the test_to_deploy transition config
        const qaEnabled = !!(pipeline && pipeline.transition_configs
                          && pipeline.transition_configs.test_to_deploy
                          && pipeline.transition_configs.test_to_deploy.qa_enabled);
        let versionStep = 'idle', buildStep = isBuildable ? 'idle' : 'skipped',
            testStep = 'idle', qaStep = qaEnabled ? 'idle' : 'skipped', deployStep = 'idle';

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
        // QA step state derives from stageOrder index 3.
        const _qa = (state) => qaEnabled ? state : 'skipped';
        if (pipeline && pipeline.status === 'running') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (cs === 1 ? 'running' : (effectiveHadBuild && cs > 1 ? 'success' : (cs > 1 ? 'idle' : 'pending')));
            testStep = cs === 2 ? 'running' : (effectiveHadTest && cs > 2 ? 'success' : (cs > 2 ? 'idle' : 'pending'));
            qaStep = _qa(cs === 3 ? 'running' : (cs > 3 ? 'success' : 'pending'));
            deployStep = cs === 4 ? 'running' : (cs > 4 ? 'success' : 'pending');
        } else if (pipeline && pipeline.status === 'gate_rejected') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (cs >= 1 ? 'success' : 'pending');
            testStep = cs === 1 ? 'gate_rejected' : (cs >= 2 ? 'success' : 'pending');
            // qa rejected = QA succeeded but qa→deploy manual gate is waiting
            qaStep = _qa(cs === 3 ? 'success' : (cs > 3 ? 'success' : 'pending'));
            deployStep = (cs === 2 || cs === 3) ? 'gate_rejected' : 'pending';
        } else if (pipeline && pipeline.status === 'failed') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (cs === 1 ? 'failed' : (effectiveHadBuild && cs > 1 ? 'success' : (cs > 1 ? 'idle' : 'pending')));
            testStep = cs === 2 ? 'failed' : (effectiveHadTest && cs > 2 ? 'success' : (cs > 2 ? 'idle' : 'pending'));
            qaStep = _qa(cs === 3 ? 'failed' : (cs > 3 ? 'success' : 'pending'));
            deployStep = cs === 4 ? 'failed' : (cs > 4 ? 'success' : 'pending');
        } else if (pipeline && pipeline.stage === 'done') {
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (effectiveHadBuild ? 'success' : 'idle');
            // Use actual per-stage status: if test failed but deploy was forced, show warning
            const testStageStatus = pipeline.stages && pipeline.stages.test ? pipeline.stages.test.status : null;
            testStep = testStageStatus === 'failed' ? 'warning' : (effectiveHadTest ? 'success' : 'idle');
            const qaStageStatus = pipeline.stages && pipeline.stages.qa ? pipeline.stages.qa.status : null;
            qaStep = _qa(qaStageStatus === 'success' ? 'success' : (qaStageStatus === 'failed' ? 'warning' : 'idle'));
            deployStep = 'success';
        } else if (pipeline && pipeline.status === 'success') {
            const cs = stageOrder[pipeline.stage] || 0;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (effectiveHadBuild && cs >= 1 ? 'success' : (cs >= 1 ? 'idle' : 'pending'));
            testStep = effectiveHadTest && cs >= 2 ? 'success' : (cs >= 2 ? 'idle' : 'pending');
            qaStep = _qa(cs >= 3 ? 'success' : 'pending');
            deployStep = cs >= 4 ? 'success' : 'pending';
        } else if (pipeline && pipeline.status === 'idle' && !isDeployed) {
            // After removal only the build result survives. Do not infer test
            // success from an image tag or turn cleared stages into pending ones.
            const bs = pipeline.stages && pipeline.stages.build;
            buildStep = skipBuild ? 'skipped' : (bs ? bs.status : 'idle');
        } else if (hasUpdate) {
            versionStep = 'success'; buildStep = skipBuild ? 'skipped' : 'success'; testStep = 'success';
            qaStep = _qa('pending'); deployStep = 'pending';
        } else if (untaggedCount > 0) {
            versionStep = 'pending'; buildStep = skipBuild ? 'skipped' : 'pending'; testStep = 'pending';
            qaStep = _qa('pending'); deployStep = 'pending';
        } else if (pipeline && pipeline.status === 'idle' && isDeployed) {
            // Pipeline exists but is idle (e.g. after restart) — show per-stage statuses or success fallback
            const ds = pipeline.stages && pipeline.stages.deploy ? pipeline.stages.deploy.status : null;
            const bs = pipeline.stages && pipeline.stages.build ? pipeline.stages.build.status : null;
            const ts = pipeline.stages && pipeline.stages.test ? pipeline.stages.test.status : null;
            const qs = pipeline.stages && pipeline.stages.qa ? pipeline.stages.qa.status : null;
            versionStep = 'success';
            buildStep = skipBuild ? 'skipped' : (bs === 'failed' ? 'failed' : (effectiveHadBuild ? 'success' : 'idle'));
            testStep = ts === 'failed' ? 'failed' : (effectiveHadTest ? 'success' : 'idle');
            qaStep = _qa(qs === 'failed' ? 'failed' : (qs === 'success' ? 'success' : 'idle'));
            deployStep = ds === 'failed' ? 'failed' : 'success';
        } else if (isDeployed) {
            versionStep = 'success'; buildStep = skipBuild ? 'skipped' : 'success'; testStep = 'success';
            qaStep = _qa('success'); deployStep = 'success';
        }

        // Stages run independently (e.g. a deploy started while a build is in
        // progress): the overall stage/status only reflects the last update, so
        // overlay every stage whose own status is still running.
        const isStageRunning = (s) => !!(pipeline && pipeline.stages && pipeline.stages[s]
                                         && pipeline.stages[s].status === 'running');
        if (isStageRunning('build')) { versionStep = 'success'; buildStep = 'running'; }
        if (isStageRunning('test')) { versionStep = 'success'; testStep = 'running'; }
        if (isStageRunning('qa')) { versionStep = 'success'; qaStep = _qa('running'); }
        if (isStageRunning('deploy')) { versionStep = 'success'; deployStep = 'running'; }

        const pipelineVersion = (pipeline && pipeline.version) ? pipeline.version : (latestBuilt ? normalizeVersion(latestBuilt) : (deployedTag || '–'));
        const buildActionId = pipeline ? pipeline.build_action_id : null;
        const testActionId = pipeline ? pipeline.test_action_id : null;
        const qaActionId = pipeline ? pipeline.qa_action_id : null;
        const deployActionId = pipeline ? pipeline.deploy_action_id : null;

        // Per-stage version annotations shown next to each pipeline step
        const stageVersions = (pipeline && pipeline.stages) ? pipeline.stages : {};
        const buildStageVersion = (stageVersions.build && stageVersions.build.current_version)
            || (latestBuilt ? normalizeVersion(latestBuilt) : null);
        const testStageVersion = stageVersions.test && stageVersions.test.current_version;
        const qaStageVersion = (stageVersions.qa && stageVersions.qa.current_version) || qaDeployedTag || null;
        const deployStageVersion = (stageVersions.deploy && stageVersions.deploy.current_version) || deployedTag || null;
        const stageVersionLabel = (v, titleText) => v
            ? `<span class="pipeline-step-version" title="${escapeHtml(titleText)}: ${escapeHtml(v)}">${escapeHtml(v)}</span>`
            : '';

        // Build containers HTML (similar to Computers view compose-group style)
        let containersHtml = '';
        const serviceCount = Object.keys(stackContainers).length;
        const qaServiceCount = Object.keys(qaStackContainers).length;
        const showContainerSection = (isDeployed && serviceCount > 0) || (isQaDeployed && qaServiceCount > 0);
        if (showContainerSection) {
            containersHtml = `<div class="host-content${isExpanded ? '' : ' start-hidden'}" id="stack-containers-${escapeHtml(repo.name)}">`;

            const renderServicesBlock = (servicesObj, parentStackName, isQa) => {
                let html = '';
                for (const [serviceName, containers] of Object.entries(servicesObj)) {
                // serviceName is the full swarm service name (e.g., "pulsarcd_backend")
                // Extract short display name by removing the stack prefix
                let displayServiceName = serviceName;
                if (serviceName === '_standalone') {
                    displayServiceName = 'Standalone';
                } else if (serviceName.startsWith(parentStackName + '_')) {
                    displayServiceName = serviceName.substring(parentStackName.length + 1);
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
                
                html += `
                    <div class="compose-group${hasContainers ? '' : ' compose-group-empty'}${isQa ? ' compose-group-qa' : ''}" data-service="${escapeHtml(serviceName)}">
                        <div class="compose-header">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                            </svg>
                            ${isQa ? '<span class="stack-badge stack-badge-qa" title="QA service">QA</span> ' : ''}${escapeHtml(displayServiceName)}
                            <span class="group-count">${containers.length}</span>
                            ${!hasContainers ? '<span class="service-no-replicas">0 replicas</span>' : ''}
                            ${serviceMemoryDisplay ? `<span class="group-stat group-memory" title="Total memory usage">💾 ${serviceMemoryDisplay}</span>` : ''}
                            ${serviceCpuDisplay ? `<span class="group-stat group-cpu ${serviceCpuClass}" title="Max CPU usage">⚡ ${serviceCpuDisplay}</span>` : ''}
                            <button class="btn btn-sm btn-ghost service-logs-btn" data-click="openServiceLogs" data-args="${uiArgs(fullServiceName)}" data-click-stop title="View service logs">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                                    <polyline points="14 2 14 8 20 8"/>
                                    <line x1="16" y1="13" x2="8" y2="13"/>
                                    <line x1="16" y1="17" x2="8" y2="17"/>
                                    <polyline points="10 9 9 9 8 9"/>
                                </svg>
                                Logs
                            </button>
                            <button class="btn btn-sm btn-ghost service-status-btn" data-click="openServiceStatus" data-args="${uiArgs(fullServiceName)}" data-click-stop title="Service task status (docker service ps)">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                    <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
                                </svg>
                                Status
                            </button>
                            ${isQa ? '' : `<button class="btn btn-sm btn-primary service-deploy-btn" data-click="openServiceDeploy" data-args="${uiArgs(fullServiceName, repo.name, repo.ssh_url, firstContainerImage)}" data-click-stop title="Deploy new version">
                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                    <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                                </svg>
                                Deploy
                            </button>`}
                            <button class="btn btn-sm btn-danger service-remove-btn" data-click="removeService" data-args="${uiArgs(fullServiceName)}" data-click-stop title="Remove service">
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
                    html += `
                        <div class="container-item container-item-empty">
                            <div class="container-info">
                                <span class="container-status exited"></span>
                                <div>
                                    <div class="container-name text-muted">No running containers</div>
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
                    
                    html += `
                        <div class="container-item" data-container-id="${escapeHtml(c.id)}" data-click="openContainer" data-args="${uiArgs(c.host, c.id, c)}">
                            <div class="container-info">
                                <span class="container-status ${c.status}"></span>
                                <div>
                                    <div class="container-name">${escapeHtml(c.name)} <span class="container-host-hint">(${escapeHtml(c.host)})</span></div>
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
                            </div>
                        </div>
                    `;
                }
                
                html += `
                            </div>
                        </div>
                    </div>
                `;
                }
                return html;
            };

            // When QA is active, render Production and QA as independently
            // collapsible sub-sections. When QA is not active, render production
            // services flat (the stack-containers block is already toggled by
            // the stack header).
            const qaActive = qaServiceCount > 0;
            if (qaActive) {
                const subExpanded = getExpandedStackSubSections(repo.name);
                const prodOpen = subExpanded.prod !== false; // default open
                const qaOpen = subExpanded.qa !== false;     // default open
                const chevronSvg = `<svg class="section-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><polyline points="6 9 12 15 18 9"/></svg>`;

                if (serviceCount > 0) {
                    containersHtml += `
                    <div class="stack-subsection ${prodOpen ? '' : 'collapsed'}" data-subsection="prod">
                        <div class="stack-section-divider stack-section-toggle" data-click="toggleStackSubSection" data-args="${uiArgs(repo.name, 'prod')}">
                            ${chevronSvg}
                            <span class="stack-badge deployed">Production</span>
                            ${deployedTag ? `<span class="stack-divider-version" title="Production deployed version">${escapeHtml(deployedTag)}</span>` : ''}
                            <span class="group-count">${serviceCount} svc</span>
                            <span class="stack-divider-name">${escapeHtml(stackName)}</span>
                        </div>
                        <div class="stack-subsection-body${prodOpen ? '' : ' start-hidden'}" id="stack-subsection-prod-${escapeHtml(repo.name)}">
                            ${renderServicesBlock(stackContainers, stackName, false)}
                        </div>
                    </div>`;
                }
                containersHtml += `
                    <div class="stack-subsection ${qaOpen ? '' : 'collapsed'}" data-subsection="qa">
                        <div class="stack-section-divider stack-section-divider-qa stack-section-toggle" data-click="toggleStackSubSection" data-args="${uiArgs(repo.name, 'qa')}">
                            ${chevronSvg}
                            <span class="stack-badge stack-badge-qa">QA Environment</span>
                            ${qaDeployedTag ? `<span class="stack-divider-version" title="QA deployed version">${escapeHtml(qaDeployedTag)}</span>` : ''}
                            <span class="group-count">${qaServiceCount} svc</span>
                            <span class="stack-divider-name">qa-${escapeHtml(stackName)}</span>
                        </div>
                        <div class="stack-subsection-body${qaOpen ? '' : ' start-hidden'}" id="stack-subsection-qa-${escapeHtml(repo.name)}">
                            ${renderServicesBlock(qaStackContainers, qaStackName, true)}
                        </div>
                    </div>
                `;
            } else {
                containersHtml += renderServicesBlock(stackContainers, stackName, false);
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
            <div class="host-header ${healthClass}" ${(isDeployed || isQaDeployed) ? `data-click="toggleStackExpand" data-args="${uiArgs(repo.name)}"` : ''}>
                <span class="host-name">
                    ${(isDeployed || isQaDeployed) ? `
                    <svg class="chevron-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="6 9 12 15 18 9"/>
                    </svg>
                    ` : ''}
                    ${stackIcon}
                    ${escapeHtml(repo.name)}
                    ${!isDeployed ? '<span class="stack-badge stack-badge-not-deployed">Not deployed</span>' : ''}
                    ${pipeline && pipeline.last_deployed_at ? `<span class="stack-deployed-ago" title="${new Date(pipeline.last_deployed_at).toLocaleString()}">${formatTimeAgo(pipeline.last_deployed_at)}</span>` : ''}
                    ${isDeployed ? `<span class="group-count">${Object.keys(stackContainers).length} svc, ${containerCount} ct</span>` : ''}
                    ${isDeployed && tooltipLines.length > 0 ? `
                    <span class="stack-monitoring-tooltip">
                        <span class="health-dot ${healthClass ? (healthClass === 'health-critical' ? 'dot-critical' : 'dot-warning') : 'dot-ok'}"></span>
                        <span class="tooltip-content">${tooltipLines.map(l => `<div>${l}</div>`).join('')}</span>
                    </span>` : ''}
                </span>
                <div class="host-header-actions" data-click-stop>
                    <button class="btn btn-sm btn-ghost" data-click="showStackActivity" data-args="${uiArgs(repo.owner, repo.name)}" title="View git activity">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <circle cx="18" cy="18" r="3"/>
                            <circle cx="6" cy="6" r="3"/>
                            <path d="M6 21V9a9 9 0 0 0 9 9"/>
                        </svg>
                        <span>Activity</span>
                    </button>
                    <button class="btn btn-sm btn-ghost" data-click="editStackEnv" data-args="${uiArgs(repo.name, qaEnabled)}" title="${qaEnabled ? 'Edit .env and .env.qa files' : 'Edit .env file'}">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                        </svg>
                        <span>Env</span>
                    </button>

                    <div class="pipeline-flow">
                        <div class="pipeline-step step-${versionStep}" data-click="pipelineStepClick" data-args="${uiArgs(repo.name, repo.ssh_url, 'version')}" data-click-stop title="Latest available version: ${escapeHtml(pipelineVersion)} (click to run full pipeline)">
                            ${stepIcon(versionStep)}
                            <span>Version</span>
                            <span class="pipeline-step-version" title="Latest available: ${escapeHtml(pipelineVersion)}">${escapeHtml(pipelineVersion)}</span>
                        </div>
                        <span class="pipeline-transition-btn ${_gateArrowClass(pipeline, 'version', versionStep, buildStep)}${gateVersionBuild ? ' has-gate' : ''}${_transitionModeClass(pipeline, 'version_to_build')}" data-click="openTransitionConfig" data-args="${uiArgs(repo.name, 'version_to_build')}" data-click-stop title="Version → Build transition (click to configure)">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
                            ${_transitionModeIcon(pipeline, 'version_to_build')}
                        </span>
                        <div class="pipeline-step step-${buildStep}" ${skipBuild ? 'title="Build: skipped (no build config)"' : `data-click="pipelineStepClick" data-args="${uiArgs(repo.name, repo.ssh_url, 'build')}" data-click-stop title="Build${buildStageVersion ? ' — last built: ' + escapeHtml(buildStageVersion) : ''}"`}>
                            ${skipBuild ? `<span class="step-icon">–</span>` : stepIcon(buildStep)}
                            <span>Build</span>
                            ${!skipBuild ? stageVersionLabel(buildStageVersion, 'Last built') : ''}
                            ${!skipBuild && buildActionId ? `<span class="pipeline-log-btn" data-click="openActionLogs" data-args="${uiArgs(buildActionId, 'Build Logs', repo.name)}" data-click-stop title="View build logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                            ${buildStep === 'running' && buildActionId ? `<span class="pipeline-stop-btn" data-click="cancelAction" data-args="${uiArgs(buildActionId)}" data-click-stop title="Stop build"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="10" height="10"><rect x="6" y="6" width="12" height="12" rx="1"/></svg></span>` : ''}
                        </div>
                        <span class="pipeline-transition-btn ${_gateArrowClass(pipeline, 'build', buildStep, testStep)}${gateBuildTest ? ' has-gate' : ''}${_transitionModeClass(pipeline, 'build_to_test')}" data-click="openTransitionConfig" data-args="${uiArgs(repo.name, 'build_to_test')}" data-click-stop title="Build → Test transition (click to configure)">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
                            ${_transitionModeIcon(pipeline, 'build_to_test')}
                        </span>
                        <div class="pipeline-step step-${testStep}" data-click="pipelineStepClick" data-args="${uiArgs(repo.name, repo.ssh_url, 'test')}" data-click-stop title="Test">
                            ${stepIcon(testStep)}
                            <span>Test</span>
                            ${testActionId ? `<span class="pipeline-log-btn" data-click="openActionLogs" data-args="${uiArgs(testActionId, 'Test Logs', repo.name)}" data-click-stop title="View test logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                            ${testStep === 'running' && testActionId ? `<span class="pipeline-stop-btn" data-click="cancelAction" data-args="${uiArgs(testActionId)}" data-click-stop title="Stop test"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="10" height="10"><rect x="6" y="6" width="12" height="12" rx="1"/></svg></span>` : ''}
                        </div>
                        <span class="pipeline-transition-btn ${_gateArrowClass(pipeline, 'test', testStep, qaEnabled ? qaStep : deployStep)}${gateTestDeploy ? ' has-gate' : ''}${_transitionModeClass(pipeline, 'test_to_deploy')}" data-click="openTransitionConfig" data-args="${uiArgs(repo.name, 'test_to_deploy')}" data-click-stop title="Test → ${qaEnabled ? 'QA → ' : ''}Deploy transition (click to configure)">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
                            ${_transitionModeIcon(pipeline, 'test_to_deploy')}
                            ${qaEnabled ? '<span class="transition-badge badge-qa" title="QA pre-deploy enabled">QA</span>' : ''}
                        </span>
                        ${qaEnabled ? `
                        <div class="pipeline-step step-${qaStep}" data-click="pipelineStepClick" data-args="${uiArgs(repo.name, repo.ssh_url, 'qa')}" data-click-stop title="QA deploy (qa-${escapeHtml(stackName)} on qa.&lt;domain&gt;)${qaStageVersion ? ' — current: ' + escapeHtml(qaStageVersion) : ''}">
                            ${stepIcon(qaStep)}
                            <span>QA</span>
                            ${stageVersionLabel(qaStageVersion, 'QA deployed')}
                            ${qaActionId ? `<span class="pipeline-log-btn" data-click="openActionLogs" data-args="${uiArgs(qaActionId, 'QA Deploy Logs', repo.name)}" data-click-stop title="View QA deploy logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                            ${qaStep === 'running' && qaActionId ? `<span class="pipeline-stop-btn" data-click="cancelAction" data-args="${uiArgs(qaActionId)}" data-click-stop title="Stop QA deploy"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="10" height="10"><rect x="6" y="6" width="12" height="12" rx="1"/></svg></span>` : ''}
                        </div>
                        <span class="pipeline-transition-btn ${_gateArrowClass(pipeline, 'qa', qaStep, deployStep)}${_transitionModeClass(pipeline, 'qa_to_deploy')}" data-click="openTransitionConfig" data-args="${uiArgs(repo.name, 'qa_to_deploy')}" data-click-stop title="QA → Deploy transition (click to configure)">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
                            ${_transitionModeIcon(pipeline, 'qa_to_deploy')}
                        </span>` : ''}
                        <div class="pipeline-step step-${deployStep}" data-click="pipelineStepClick" data-args="${uiArgs(repo.name, repo.ssh_url, 'deploy')}" data-click-stop title="Deploy (prod)${deployStageVersion ? ' — current: ' + escapeHtml(deployStageVersion) : ''}">
                            ${stepIcon(deployStep)}
                            <span>Deploy</span>
                            ${stageVersionLabel(deployStageVersion, 'Prod deployed')}
                            ${deployActionId ? `<span class="pipeline-log-btn" data-click="openActionLogs" data-args="${uiArgs(deployActionId, 'Deploy Logs', repo.name)}" data-click-stop title="View deploy logs"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>` : ''}
                            ${deployStep === 'running' && deployActionId ? `<span class="pipeline-stop-btn" data-click="cancelAction" data-args="${uiArgs(deployActionId)}" data-click-stop title="Stop deploy"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="10" height="10"><rect x="6" y="6" width="12" height="12" rx="1"/></svg></span>` : ''}
                        </div>
                    </div>

                    ${isDeployed ? `
                    <button class="btn btn-sm btn-danger" data-click="removeDeployedStack" data-args="${uiArgs(repo.name)}" title="Remove deployed stack">
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
    adoptStartHidden(listEl);
}

// ============== Pipeline Step Click ==============

function pipelineStepClick(repoName, sshUrl, step) {
    const pipeline = stacksPipelineState[repoName];

    // If the step is currently running, show live logs instead of opening the modal
    // (several stages can run at once, so check the step's own status too)
    const stageRunning = !!(pipeline && pipeline.stages && pipeline.stages[step]
                            && pipeline.stages[step].status === 'running');
    if (pipeline && (stageRunning || (pipeline.status === 'running' && pipeline.stage === step))) {
        const actionId = pipeline[`${step}_action_id`] || null;

        if (actionId) {
            const label = step === 'qa' ? 'QA' : (step.charAt(0).toUpperCase() + step.slice(1));
            openActionLogs(actionId, `${label} Logs`, repoName);
            return;
        }
    }

    // Otherwise, always open the action modal (past logs accessible via the log icon button)
    if (step === 'version') pipelineStack(repoName, sshUrl);
    else if (step === 'build') buildStack(repoName, sshUrl);
    else if (step === 'test') testStack(repoName, sshUrl);
    else if (step === 'qa') deployStack(repoName, sshUrl, true);
    else if (step === 'deploy') deployStack(repoName, sshUrl);
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
            await refreshStacks(); // Reload versions and discard removed containers immediately.
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
