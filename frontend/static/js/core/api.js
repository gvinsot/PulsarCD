/**
 * PulsarCD frontend: API helpers (GET/POST/PUT with auth) and notification toasts.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== API Helpers ==============

async function apiGet(endpoint) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, { headers: authHeaders() });
        if (response.status === 401) { showLogin(); return null; }
        // A handful of GETs disclose secrets or infrastructure detail and are
        // admin-only (config, .env contents, GitHub token probes).
        if (response.status === 403) { await _reportForbidden(response, endpoint); return null; }
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
/**
 * GET that reports why it failed instead of collapsing everything to null.
 * apiGet() suits polling views, where a silent null is fine; a screen the user
 * opened on purpose (the .env editor) has to be able to say "the build host
 * refused the SSH connection" rather than show what looks like an empty file.
 * Returns {data, error} - exactly one of the two is set.
 */
async function apiGetOrError(endpoint) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, { headers: authHeaders() });
        if (response.status === 401) { showLogin(); return { error: 'Not authenticated' }; }
        if (response.status === 403) {
            return { error: await _reportForbidden(response, endpoint) };
        }
        if (!response.ok) {
            const body = await response.json().catch(() => ({}));
            return { error: body.detail || `HTTP ${response.status}` };
        }
        return { data: await response.json() };
    } catch (error) {
        console.error(`API Error (${endpoint}):`, error);
        return { error: error.message || 'Connection error' };
    }
}

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
        if (response.status === 403) { await _reportForbidden(response, endpoint); return null; }
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
        if (response.status === 403) { await _reportForbidden(response, endpoint); return null; }
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
                <button class="action-toast-close" data-click="dismissToast">&times;</button>
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
