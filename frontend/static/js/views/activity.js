/**
 * PulsarCD frontend: Activity modal: commit graph and diff viewer.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Activity Modal ==============

let activityOwner = null;
let activityRepo = null;
let activityCommits = [];
let activityBranches = [];
let activityBranchTipMap = {};
let activityTagMap = {};
let activityCommitBranches = {};

const BRANCH_COLORS = ['#00d4aa', '#0ea5e9', '#8b5cf6', '#f59e0b', '#ef4444', '#ec4899', '#14b8a6', '#f97316'];

async function cleanupRepoTags(owner, repo) {
    if (!confirm(`Delete all old tags and their Docker registry images for "${repo}"?\n\nThe 5 most recent tags, currently deployed tags, and tags newer than 30 days will be preserved.`)) {
        return;
    }
    try {
        const resp = await fetch(`${API_BASE}/admin/tag-cleanup/repo/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}?dry_run=false`, { method: 'POST', headers: authHeaders() });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            alert(`Tag cleanup failed: ${err.detail || resp.statusText}`);
            return;
        }
        const result = await resp.json();
        const deleted = result.deleted || [];
        const deletedImages = result.deleted_images || [];
        if (deleted.length === 0) {
            showNotification('success', `No tags to clean up for ${repo}`);
        } else {
            let msg = `Deleted ${deleted.length} tag(s) for ${repo}: ${deleted.join(', ')}`;
            if (deletedImages.length > 0) {
                msg += `\n🐳 ${deletedImages.length} registry image(s) removed`;
            }
            showNotification('success', msg);
        }
    } catch (e) {
        alert('Tag cleanup failed: ' + (e.message || e));
    }
}

async function showStackActivity(owner, repo) {
    activityOwner = owner;
    activityRepo = repo;
    activityCommits = [];

    const modal = document.getElementById('stack-activity-modal');
    const title = document.getElementById('stack-activity-title');
    const graphContainer = document.getElementById('activity-graph-container');
    const diffPanel = document.getElementById('activity-diff-panel');

    title.textContent = `Activity: ${repo}`;

    // Populate header action buttons (GitHub + Tags cleanup)
    const headerActions = document.querySelector('#stack-activity-modal .modal-header-actions');
    const githubUrl = `https://github.com/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}`;
    headerActions.innerHTML = `
        <a class="btn btn-sm btn-ghost" href="${githubUrl}" target="_blank" rel="noopener" title="Open on GitHub">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22"/>
            </svg>
            <span>GitHub</span>
        </a>
        <button class="btn btn-sm btn-ghost" data-click="cleanupRepoTags" data-args="${uiArgs(owner, repo)}" title="Clean up old tags (keep latest 5)">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
            </svg>
            <span>Tags</span>
        </button>
        <button class="modal-close" data-click="closeActivityModal">&times;</button>
    `;
    graphContainer.innerHTML = '<div class="loading-placeholder">Loading activity...</div>';
    diffPanel.style.display = 'none';
    modal.classList.add('active');

    await loadActivityData();
}

async function loadActivityData() {
    const graphContainer = document.getElementById('activity-graph-container');

    const data = await apiGet(`/stacks/${encodeURIComponent(activityOwner)}/${encodeURIComponent(activityRepo)}/activity?per_page=30`);

    if (!data) {
        graphContainer.innerHTML = '<div class="loading-placeholder text-error">Failed to load activity data. Check browser console for details.</div>';
        return;
    }

    activityCommits = data.commits || [];
    activityBranches = data.branches || [];
    activityBranchTipMap = data.branch_tip_map || {};
    activityTagMap = data.tag_map || {};
    activityCommitBranches = data.commit_branches || {};

    if (data.error && activityCommits.length === 0) {
        graphContainer.innerHTML = `<div class="loading-placeholder text-error">${escapeHtml(data.error)}</div>`;
        return;
    }

    if (activityCommits.length === 0) {
        // No error from backend but also no commits — run a diagnostic check
        graphContainer.innerHTML = '<div class="loading-placeholder">No commits found. Checking permissions...</div>';
        const diagData = await apiGet(`/stacks/test-permissions/${encodeURIComponent(activityOwner)}/${encodeURIComponent(activityRepo)}`);
        if (diagData && diagData.summary) {
            graphContainer.innerHTML = `<div class="loading-placeholder text-error">${escapeHtml(diagData.summary)}</div>`;
        } else {
            graphContainer.innerHTML = '<div class="loading-placeholder text-error">No commits found. Ensure your GitHub token has \'Contents: Read\' permission.</div>';
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
        svgCircles += `<circle cx="${cx}" cy="${cy}" r="${RADIUS}" fill="${color}" stroke="var(--bg-card)" stroke-width="2" class="activity-commit-dot" data-click="showCommitDiff" data-args="${uiArgs(commit.sha)}"/>`;
    }

    // Build commit rows
    let rowsHtml = '';
    for (let i = 0; i < activityCommits.length; i++) {
        const commit = activityCommits[i];

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
        <div class="activity-row" data-sha="${commit.sha}" data-click="showCommitDiff" data-args="${uiArgs(commit.sha)}">
            <div class="activity-row-info">
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
        <div class="activity-graph">
            <svg class="activity-svg" width="${graphWidth}" height="${totalHeight}">
                ${svgLines}
                ${svgCircles}
            </svg>
            <div class="activity-rows">
                ${rowsHtml}
            </div>
        </div>
    `;

    // Geometry goes through the CSSOM: the CSP refuses inline style attributes.
    container.querySelector('.activity-graph').style.minHeight = `${totalHeight}px`;
    container.querySelectorAll('.activity-row').forEach((row, i) => {
        const cy = 22 + i * ROW_HEIGHT;
        row.style.height = `${ROW_HEIGHT}px`;
        row.style.top = `${cy - ROW_HEIGHT / 2 + RADIUS}px`;
        row.querySelector('.activity-row-info').style.paddingLeft = `${graphWidth}px`;
    });
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
        diffContent.innerHTML = '<div class="loading-placeholder text-error">Failed to load diff</div>';
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
                <div class="diff-file-header" data-click="toggleDiffFile">
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
