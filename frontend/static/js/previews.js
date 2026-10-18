/**
 * Preview environments panel (Stacks view).
 *
 * Lists per-branch previews (GET /api/previews), deploys a new one
 * (POST /api/previews) and tears one down (DELETE /api/previews/{stack}).
 * Deploys run as regular background actions, so their progress shows in the
 * same action toasts and log modal as builds and deploys.
 *
 * Kept out of app.js on purpose: it only relies on globals of the core/ and views/ scripts
 * (API_BASE, authHeaders, escapeHtml, showNotification, formatTimeAgo,
 * trackBackgroundAction, openActionLogs) and uses its own event delegation
 * (data-preview-action) instead of the frozen UI_HANDLERS registry.
 */
(function () {
    'use strict';

    let reposCache = null;

    function panel() {
        return document.getElementById('previews-panel');
    }

    async function request(method, endpoint) {
        const response = await fetch(`${API_BASE}${endpoint}`, { method, headers: authHeaders() });
        if (response.status === 401) { showLogin(); throw new Error('Not authenticated'); }
        if (response.status === 403) throw new Error('This action is reserved for administrators.');
        const body = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(body.detail || `HTTP ${response.status}`);
        return body;
    }

    async function loadRepos() {
        if (reposCache) return reposCache;
        const data = await request('GET', '/stacks/repos');
        reposCache = (data && data.repos) || [];
        const select = document.getElementById('preview-repo');
        select.innerHTML = reposCache.length
            ? reposCache.map((r, i) => `<option value="${i}">${escapeHtml(r.name)}</option>`).join('')
            : '<option value="">No starred repositories</option>';
        return reposCache;
    }

    function statusBadge(p) {
        const cls = { active: 'ok', building: 'busy', deploying: 'busy', removing: 'busy', failed: 'err' }[p.status] || '';
        const running = p.running === true ? ' · running' : (p.running === false ? ' · not running' : '');
        return `<span class="preview-status preview-status-${cls}">${escapeHtml(p.status || '?')}${running}</span>`;
    }

    function renderList(data) {
        const listEl = document.getElementById('previews-list');
        const previews = (data && data.previews) || [];
        const unmanaged = (data && data.unmanaged) || [];
        const summary = document.getElementById('previews-summary-count');
        if (summary) summary.textContent = `${previews.length}`;

        if (!previews.length && !unmanaged.length) {
            listEl.innerHTML = '<div class="empty-placeholder">No preview environments.</div>';
            return;
        }
        const rows = previews.map((p) => `
            <tr>
                <td><code>${escapeHtml(p.stack)}</code></td>
                <td>${escapeHtml(p.repo || '')}</td>
                <td><code>${escapeHtml(p.branch || '')}</code></td>
                <td>${statusBadge(p)}${p.error ? `<div class="preview-error">${escapeHtml(p.error)}</div>` : ''}</td>
                <td><code>${escapeHtml(p.host_prefix || '')}&lt;host&gt;</code></td>
                <td>${p.last_deployed_at ? escapeHtml(formatTimeAgo(p.last_deployed_at)) : '-'}</td>
                <td>${p.expires_at ? escapeHtml(new Date(p.expires_at).toLocaleString()) : 'never'}</td>
                <td class="preview-actions">
                    ${p.action_id ? `<button class="btn btn-sm btn-secondary" data-preview-action="logs"
                        data-action-id="${escapeHtml(p.action_id)}" data-repo="${escapeHtml(p.repo || '')}">Logs</button>` : ''}
                    <button class="btn btn-sm btn-secondary" data-preview-action="redeploy"
                        data-repo="${escapeHtml(p.repo || '')}" data-branch="${escapeHtml(p.branch || '')}">Redeploy</button>
                    <button class="btn btn-sm btn-danger" data-preview-action="teardown"
                        data-stack="${escapeHtml(p.stack)}">Tear down</button>
                </td>
            </tr>`).join('');
        listEl.innerHTML = `
            <table class="previews-table">
                <thead><tr><th>Stack</th><th>Repository</th><th>Branch</th><th>Status</th>
                    <th>Host</th><th>Deployed</th><th>Expires</th><th></th></tr></thead>
                <tbody>${rows}</tbody>
            </table>
            ${unmanaged.length ? `<p class="hint">Running <code>pv-</code> stacks without a PulsarCD record
                (never touched by previews): ${unmanaged.map((s) => `<code>${escapeHtml(s)}</code>`).join(', ')}</p>` : ''}`;
    }

    async function refresh() {
        const listEl = document.getElementById('previews-list');
        try {
            renderList(await request('GET', '/previews'));
        } catch (e) {
            listEl.innerHTML = `<div class="error-placeholder">${escapeHtml(e.message || 'Failed to load previews')}</div>`;
        }
    }

    async function deploy(repoName, sshUrl, branch, ttl) {
        let url = `/previews?repo_name=${encodeURIComponent(repoName)}&ssh_url=${encodeURIComponent(sshUrl)}`
            + `&branch=${encodeURIComponent(branch)}`;
        if (ttl !== '' && ttl !== null && ttl !== undefined) url += `&ttl_hours=${encodeURIComponent(ttl)}`;
        const data = await request('POST', url);
        trackBackgroundAction(data.action_id, 'Preview Deploy', repoName);
        showNotification('success', `Preview ${data.stack} started (host ${data.host_prefix}…)`);
        refresh();
    }

    async function onSubmit() {
        const repos = await loadRepos();
        const repo = repos[Number(document.getElementById('preview-repo').value)];
        const branch = document.getElementById('preview-branch').value.trim();
        const ttl = document.getElementById('preview-ttl').value.trim();
        if (!repo || !branch) {
            showNotification('error', 'Pick a repository and a branch');
            return;
        }
        try {
            await deploy(repo.name, repo.ssh_url, branch, ttl);
        } catch (e) {
            showNotification('error', e.message || 'Preview deploy failed');
        }
    }

    async function onAction(el) {
        const action = el.getAttribute('data-preview-action');
        try {
            if (action === 'submit') {
                await onSubmit();
            } else if (action === 'refresh') {
                await refresh();
            } else if (action === 'logs') {
                openActionLogs(el.getAttribute('data-action-id'), 'Preview Deploy', el.getAttribute('data-repo'));
            } else if (action === 'redeploy') {
                const repos = await loadRepos();
                const repo = repos.find((r) => r.name === el.getAttribute('data-repo'));
                if (!repo) throw new Error('Repository is no longer starred');
                await deploy(repo.name, repo.ssh_url, el.getAttribute('data-branch'), '');
            } else if (action === 'teardown') {
                const stack = el.getAttribute('data-stack');
                if (!confirm(`Tear down preview stack ${stack}?`)) return;
                el.disabled = true;
                await request('DELETE', `/previews/${encodeURIComponent(stack)}`);
                showNotification('success', `Preview ${stack} removed`);
                await refresh();
            }
        } catch (e) {
            el.disabled = false;
            showNotification('error', e.message || 'Preview action failed');
        }
    }

    document.addEventListener('click', (event) => {
        const root = panel();
        if (!root) return;
        const el = event.target.closest('[data-preview-action]');
        if (el && root.contains(el) && !el.disabled) onAction(el);
    });

    document.addEventListener('toggle', (event) => {
        if (event.target === panel() && panel().open) {
            loadRepos().catch(() => {});
            refresh();
        }
    }, true);
})();
