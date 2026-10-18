/**
 * PulsarCD frontend: Build, test, deploy and single-service deploy modals.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

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
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" data-click="selectBuildTag" data-args="${uiArgs(tag.name)}">
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
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" data-click="selectTestTag" data-args="${uiArgs(tag.name)}">
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
let currentDeployIsQa = false;
let selectedDeployTag = null;

async function deployStack(repoName, sshUrl, qa = false) {
    currentDeployRepo = repoName;
    currentDeploySshUrl = sshUrl;
    currentDeployIsQa = !!qa;
    selectedDeployTag = null;

    const modal = document.getElementById('stack-deploy-modal');
    const title = document.getElementById('stack-deploy-title');
    const tagsList = document.getElementById('deploy-tags-list');
    const tagInput = document.getElementById('deploy-tag-input');
    const selectedDisplay = document.getElementById('deploy-selected-tag');

    title.textContent = currentDeployIsQa ? `QA Deploy: ${repoName}` : `Deploy: ${repoName}`;
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
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" data-click="selectDeployTag" data-args="${uiArgs(tag.name)}">
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
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" data-click="selectServiceDeployTag" data-args="${uiArgs(tag.name)}">
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
        if (currentDeployIsQa) {
            url += `&qa=true`;
        }

        const repoName = currentDeployRepo;
        const isQa = currentDeployIsQa;
        const response = await fetch(`${API_BASE}${url}`, { method: 'POST', headers: authHeaders() });
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }
        const data = await response.json();
        if (data && data.action_id) {
            closeDeployModal();
            trackBackgroundAction(data.action_id, isQa ? 'QA Deploy' : 'Deploy', repoName);
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
