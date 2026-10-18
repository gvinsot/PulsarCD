/**
 * PulsarCD frontend: Pipeline gates, transition config and the pipeline modal.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

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

    const label = transition === 'build_to_test' ? 'Build → Test' : transition === 'qa_to_deploy' ? 'QA → Deploy' : 'Test → Deploy';
    const ts = decision.timestamp ? new Date(decision.timestamp).toLocaleString() : '';

    const modal = document.getElementById('gate-decision-modal');
    if (!modal) {
        const m = document.createElement('div');
        m.id = 'gate-decision-modal';
        m.className = 'modal';
        m.onclick = (e) => { if (e.target === m) m.classList.remove('active'); };
        m.innerHTML = `
            <div class="modal-content modal-gate">
                <div class="modal-header">
                    <h3 id="gate-decision-title">Gate Decision</h3>
                    <button class="modal-close" data-click="closeGateDecisionModal">&times;</button>
                </div>
                <div class="modal-body modal-body-compact">
                    <div id="gate-decision-info" class="decision-info">
                        <div id="gate-decision-status" class="decision-status"></div>
                        <div class="decision-separator"></div>
                        <div id="gate-decision-meta" class="decision-meta"></div>
                    </div>
                    <div class="section-caption">AI Analysis</div>
                    <div id="gate-decision-reason" class="markdown-body decision-reason"></div>
                </div>
                <div class="modal-footer">
                    <button class="btn btn-secondary" data-click="closeGateDecisionModal">Close</button>
                </div>
            </div>`;
        document.body.appendChild(m);
    }

    const statusIcon = decision.approved
        ? '<span class="decision-verdict decision-approved"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="16" height="16"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Approved</span>'
        : '<span class="decision-verdict decision-rejected"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="16" height="16"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Rejected</span>';

    document.getElementById('gate-decision-title').textContent = `Gate: ${label}`;
    document.getElementById('gate-decision-meta').innerHTML = `<strong>${escapeHtml(repoName)}</strong>${pipeline.project_name && pipeline.project_name !== repoName ? ` (${escapeHtml(pipeline.project_name)})` : ''}${pipeline.stack_name ? ` &mdash; Stack: <code>${escapeHtml(pipeline.stack_name)}</code>` : ''}${ts ? ` &mdash; <span title="${escapeHtml(decision.timestamp || '')}">${ts}</span>` : ''}`;
    document.getElementById('gate-decision-status').innerHTML = statusIcon;
    document.getElementById('gate-decision-reason').innerHTML = simpleMarkdown(decision.reason || 'No reason provided');
    document.getElementById('gate-decision-modal').classList.add('active');
}

// ============== Transition Config Modal ==============

async function openTransitionConfig(repoName, transition) {
    let label = transition === 'version_to_build' ? 'Version → Build' : transition === 'build_to_test' ? 'Build → Test' : transition === 'qa_to_deploy' ? 'QA → Deploy' : 'Test → Deploy';
    // When QA is enabled on the test_to_deploy transition, the configured mode applies
    // to Test → QA (not Test → Prod) — QA → Prod is always manual.
    if (transition === 'test_to_deploy') {
        const pipeline = stacksPipelineState && stacksPipelineState[repoName];
        const qaEnabled = pipeline && pipeline.transition_configs
            && pipeline.transition_configs.test_to_deploy
            && pipeline.transition_configs.test_to_deploy.qa_enabled;
        if (qaEnabled) label = 'Test → QA';
    }

    // Create modal if not exists
    let modal = document.getElementById('transition-config-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'transition-config-modal';
        modal.className = 'modal';
        modal.onclick = (e) => { if (e.target === modal) modal.classList.remove('active'); };
        modal.innerHTML = `
            <div class="modal-content modal-transition">
                <div class="modal-header">
                    <h3 id="transition-config-title">Transition Config</h3>
                    <button class="modal-close" data-click="closeTransitionConfigModal">&times;</button>
                </div>
                <div class="modal-body modal-body-compact">
                    <div id="transition-config-loading" class="loading-placeholder">Loading...</div>
                    <div id="transition-config-content" class="start-hidden">
                        <div class="transition-config-intro">
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
                        <div id="transition-qa-section" class="transition-qa-section start-hidden">
                            <label class="transition-qa-label">
                                <input type="checkbox" id="transition-qa-enabled">
                                <span>
                                    <div class="transition-qa-title">Enable QA pre-deploy step</div>
                                    <div class="transition-qa-help">
                                        Before the production deploy, also deploy an isolated QA copy of the stack.
                                        The QA stack name is prefixed with <code>qa-</code> and any
                                        <code>TRAEFIK_HOST</code> / <code>*_HOST</code> / <code>*_DOMAIN</code>
                                        env vars are prefixed with <code>qa.</code>
                                        (e.g. <code>https://pulsarteam.io</code> → <code>https://qa.pulsarteam.io</code>).
                                        <br><br>
                                        When enabled, the mode selected above applies to the
                                        <strong>Test → QA</strong> transition. The
                                        <strong>QA → Deploy</strong> transition has its own
                                        mode (click the arrow between QA and Deploy; manual by default).
                                    </div>
                                </span>
                            </label>
                        </div>
                        <div id="transition-ai-logs-section" class="transition-ai-logs-section start-hidden">
                            <div class="section-caption">Last AI Decision</div>
                            <div id="transition-ai-decision-info" class="decision-info decision-info-compact">
                                <div id="transition-ai-decision-status" class="decision-status decision-status-compact"></div>
                                <div class="decision-separator decision-separator-compact"></div>
                                <div id="transition-ai-decision-meta" class="decision-meta decision-meta-compact"></div>
                            </div>
                            <div id="transition-ai-decision-reason" class="markdown-body decision-reason decision-reason-compact"></div>
                        </div>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn btn-secondary" data-click="closeTransitionConfigModal">Cancel</button>
                    <button class="btn btn-primary" id="transition-config-save" data-click="saveTransitionConfig">Save</button>
                </div>
            </div>`;
        adoptStartHidden(modal);
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
        const currentMode = config.mode || (transition === 'qa_to_deploy' ? 'manual' : 'auto_with_success');
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

        // QA option only applies to the test_to_deploy transition
        const qaSection = document.getElementById('transition-qa-section');
        const qaCheckbox = document.getElementById('transition-qa-enabled');
        if (transition === 'test_to_deploy') {
            qaSection.style.display = '';
            qaCheckbox.checked = !!config.qa_enabled;
        } else {
            qaSection.style.display = 'none';
            qaCheckbox.checked = false;
        }

        // Show AI decision logs if available
        const aiSection = document.getElementById('transition-ai-logs-section');
        if (lastDecision) {
            aiSection.style.display = '';
            const statusIcon = lastDecision.approved
                ? '<span class="decision-verdict decision-verdict-compact decision-approved"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="14" height="14"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg> Approved</span>'
                : '<span class="decision-verdict decision-verdict-compact decision-rejected"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Rejected</span>';
            document.getElementById('transition-ai-decision-status').innerHTML = statusIcon;
            const ts = lastDecision.timestamp ? new Date(lastDecision.timestamp).toLocaleString() : '';
            const versionTag = lastDecision.version ? `<span class="decision-version">v${escapeHtml(lastDecision.version)}</span>` : '';
            document.getElementById('transition-ai-decision-meta').innerHTML = [versionTag, ts ? `<span>${ts}</span>` : ''].filter(Boolean).join(' <span class="decision-inline-separator"></span> ');
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
        const body = { mode: selected.value };
        if (transition === 'test_to_deploy') {
            const qaCb = document.getElementById('transition-qa-enabled');
            body.qa_enabled = !!(qaCb && qaCb.checked);
        }
        await pipelineRequest(`/stacks/pipeline/${encodeURIComponent(repoName)}/transition/${encodeURIComponent(transition)}`, 'PUT', body);
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

async function pipelineRequest(endpoint, method, body) {
    const response = await fetch(API_BASE + endpoint, { method,
        headers: { ...authHeaders(), 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || result.detail || `HTTP ${response.status}`);
    return result;
}

// ============== Pipeline Modal (Version → Build → Test → Deploy) ==============

let currentPipelineRepo = null;
let currentPipelineSshUrl = null;
let currentPipelineOwner = null;
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
    const branchSelect = document.getElementById('pipeline-branch-select');

    title.textContent = `Pipeline: ${repoName}`;
    tagsList.innerHTML = '<div class="loading-placeholder">Loading...</div>';
    untaggedSection.style.display = 'none';
    untaggedList.innerHTML = '';
    selectedDisplay.style.display = 'none';
    branchSelect.innerHTML = '<option value="">Loading branches...</option>';

    modal.classList.add('active');

    // Extract owner from ssh_url
    const ownerMatch = sshUrl.match(/[:/]([^/]+)\/[^/]+\.git$/);
    if (!ownerMatch) {
        tagsList.innerHTML = '<div class="error-placeholder">Failed to parse repository URL</div>';
        return;
    }
    const owner = ownerMatch[1];
    currentPipelineOwner = owner;

    // Load tags, untagged commits and branches in parallel
    try {
        const [tagsResult, untaggedResult, branchesResult] = await Promise.allSettled([
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/tags?limit=20`),
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/untagged-commits?limit=10`),
            apiGet(`/stacks/${encodeURIComponent(owner)}/${encodeURIComponent(repoName)}/branches`),
        ]);

        const tagsData = tagsResult.status === 'fulfilled' ? tagsResult.value : null;
        const untaggedData = untaggedResult.status === 'fulfilled' ? untaggedResult.value : null;
        const branchesData = branchesResult.status === 'fulfilled' ? branchesResult.value : null;

        // Populate branch selector
        const defaultBranch = (tagsData && tagsData.default_branch) || 'main';
        if (branchesData && branchesData.branches && branchesData.branches.length > 0) {
            branchSelect.innerHTML = branchesData.branches.map(b =>
                `<option value="${escapeHtml(b.name)}" ${b.name === defaultBranch ? 'selected' : ''}>
                    ${escapeHtml(b.name)}${b.protected ? ' \uD83D\uDD12' : ''}
                </option>`
            ).join('');
        } else {
            branchSelect.innerHTML = `<option value="${escapeHtml(defaultBranch)}">${escapeHtml(defaultBranch)}</option>`;
        }

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
        branchSelect.innerHTML = '<option value="main">main (default)</option>';
    }
}

function renderPipelineUntaggedList(commits) {
    const list = document.getElementById('pipeline-untagged-list');

    let html = `<div class="tags-group"><div class="tags-group-items">`;
    for (const commit of commits) {
        const timeAgo = formatTimeAgo(commit.date);
        const msgFirstLine = (commit.message || '').split('\n')[0].substring(0, 60);
        html += `
            <div class="tag-item untagged-commit-item" data-commit="${escapeHtml(commit.sha)}" data-click="selectPipelineCommit" data-args="${uiArgs(commit.sha)}">
                <span class="tag-name untagged-commit-name">
                    <span class="tag-sha untagged-commit-sha">${escapeHtml(commit.short_sha)}</span>
                    <span class="untagged-commit-message">${escapeHtml(msgFirstLine)}</span>
                </span>
                <span class="tag-meta">
                    ${timeAgo ? `<span class="tag-age">${timeAgo}</span>` : ''}
                    <span class="untagged-commit-author">${escapeHtml(commit.author_name || '')}</span>
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
            <div class="tag-item" data-tag="${escapeHtml(tag.name)}" data-click="selectPipelineTag" data-args="${uiArgs(tag.name)}">
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
    currentPipelineOwner = null;
    selectedPipelineTag = null;
    selectedPipelineCommit = null;
}

async function onPipelineBranchChange() {
    if (!currentPipelineOwner || !currentPipelineRepo) return;

    const branch = document.getElementById('pipeline-branch-select').value;
    const untaggedSection = document.getElementById('pipeline-untagged-section');
    const untaggedList = document.getElementById('pipeline-untagged-list');
    const selectedDisplay = document.getElementById('pipeline-selected-tag');

    selectedPipelineTag = null;
    selectedPipelineCommit = null;
    selectedDisplay.style.display = 'none';
    untaggedSection.style.display = 'none';
    untaggedList.innerHTML = '<div class="loading-placeholder">Loading...</div>';

    try {
        const branchParam = branch ? `&branch=${encodeURIComponent(branch)}` : '';
        const untaggedData = await apiGet(
            `/stacks/${encodeURIComponent(currentPipelineOwner)}/${encodeURIComponent(currentPipelineRepo)}/untagged-commits?limit=10${branchParam}`
        );

        const untaggedCommits = (untaggedData && untaggedData.untagged_commits) || [];
        if (untaggedCommits.length > 0) {
            untaggedSection.style.display = '';
            document.getElementById('pipeline-untagged-count').textContent = untaggedCommits.length;
            renderPipelineUntaggedList(untaggedCommits);
            selectPipelineCommit(untaggedCommits[0].sha);
        } else {
            untaggedSection.style.display = 'none';
            untaggedList.innerHTML = '';
        }
    } catch (e) {
        console.error('Failed to load untagged commits for branch:', e);
        untaggedSection.style.display = 'none';
        untaggedList.innerHTML = '';
    }
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
