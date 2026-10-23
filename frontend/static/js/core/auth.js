/**
 * PulsarCD frontend: Authentication, sign-in (Google / break-glass password) and the user menu.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Authentication ==============

function getAuthToken() { return localStorage.getItem('pulsarcd_token'); }
function setAuthToken(token) { localStorage.setItem('pulsarcd_token', token); }
function clearAuthToken() { localStorage.removeItem('pulsarcd_token'); }

function authHeaders() {
    const t = getAuthToken();
    return t ? { 'Authorization': `Bearer ${t}` } : {};
}

function showLogin() {
    document.getElementById('login-overlay').style.display = 'flex';
    document.querySelector('.app').style.display = 'none';
    clearLoginError();
    initSignIn();
}

function clearLoginError() {
    const el = document.getElementById('login-error');
    el.textContent = '';
    el.style.display = 'none';
}

function showLoginError(message) {
    const el = document.getElementById('login-error');
    el.textContent = message;
    el.style.display = 'block';
}

function hideLogin() {
    document.getElementById('login-overlay').style.display = 'none';
    document.querySelector('.app').style.display = '';
}

function logout() {
    clearAuthToken();
    // Stop Google from silently handing the same account back on the next
    // sign-in: logging out of a console is meant to mean something.
    try { google.accounts.id.disableAutoSelect(); } catch { /* GIS not loaded */ }
    showLogin();
}

let _currentUserRole = 'viewer';

/**
 * Whether the signed-in account may trigger state-changing operations.
 * The backend enforces this centrally (every mutating /api/ route and a few
 * secret-bearing GETs are admin-only); this is only so the UI stops offering
 * actions that would come back as a 403.
 */
function isAdmin() {
    return _currentUserRole === 'admin';
}

/** Reflect the role on <body> so stylesheets can hide admin-only controls. */
function applyRoleToDocument() {
    if (document.body) {
        document.body.classList.toggle('role-viewer', !isAdmin());
        document.body.classList.toggle('role-admin', isAdmin());
    }
}

/**
 * Turn a 403 from the API into a message that explains itself.
 * Without this the viewer sees a bare "HTTP 403" for every action button.
 */
function _notifyForbidden(endpoint) {
    console.warn(`Forbidden (${endpoint}): admin role required`);
    showNotification('warning',
        'This action is reserved for administrators (your account is read-only).');
}

const EDGE_REFUSAL_MESSAGE = 'Refused at the edge (HTTP 403): this address is blocked, or the WAF '
    + 'rejected the request. Not a permission issue.';
let _lastEdgeRefusalNotice = 0;

/**
 * Report a 403 and return its message. PulsarCD's own refusals carry a JSON
 * "detail"; a 403 without one comes from in front of it: the edge blocklist
 * (an automatic ban, say) or the WAF. Calling that "reserved for
 * administrators" sent an administrator looking for a role problem.
 */
async function _reportForbidden(response, endpoint) {
    const body = await response.json().catch(() => null);
    if (body && typeof body.detail === 'string') {
        _notifyForbidden(endpoint);
        return 'This action is reserved for administrators.';
    }
    console.warn(`Forbidden (${endpoint}): refused before reaching PulsarCD`);
    // Polling views hit it every few seconds: one notice a minute is enough.
    if (Date.now() - _lastEdgeRefusalNotice > 60000) {
        _lastEdgeRefusalNotice = Date.now();
        showNotification('error', EDGE_REFUSAL_MESSAGE);
    }
    return EDGE_REFUSAL_MESSAGE;
}

async function checkAuth() {
    const token = getAuthToken();
    if (!token) {
        showLogin();
        return;
    }
    try {
        const response = await fetch(`${API_BASE}/auth/me`, { headers: authHeaders() });
        if (response.status === 401) {
            showLogin();
            return;
        }
        const data = await response.json();
        _currentUserRole = data.role || 'viewer';
        applyRoleToDocument();
        updateUserMenu(data.username || 'user', data.role || 'viewer');
        hideLogin();
        switchView(getViewFromHash() || 'dashboard');
    } catch {
        showLogin();
    }
}

// ============== Sign-in ==============

const GOOGLE_GSI_SRC = 'https://accounts.google.com/gsi/client';

// /api/auth/config is fetched once and reused: showLogin() runs again after
// every 401, and re-rendering the Google button on each of those would restart
// the whole GIS handshake.
let _signInConfig = null;
let _signInReady = false;

/**
 * Prepare whichever sign-in methods this deployment offers.
 * Google is the normal path; the password form is the break-glass account and
 * is only offered when the backend says one exists.
 */
async function initSignIn() {
    if (_signInReady) return;
    try {
        const response = await fetch(`${API_BASE}/auth/config`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        _signInConfig = await response.json();
    } catch {
        showLoginError('Cannot reach the server.');
        return;
    }
    _signInReady = true;

    if (_signInConfig.password_login_enabled) {
        document.getElementById('login-fallback').style.display = '';
    }
    if (_signInConfig.google_enabled && _signInConfig.google_client_id) {
        document.getElementById('login-google').style.display = '';
        try {
            await loadGoogleIdentityServices();
            renderGoogleButton(_signInConfig.google_client_id);
        } catch {
            showLoginError('Could not load Google sign-in.');
            // Without Google there may be nothing else on screen, so open the
            // password form rather than leaving a dead card.
            if (_signInConfig.password_login_enabled) togglePasswordLogin(true);
        }
    } else if (!_signInConfig.password_login_enabled) {
        showLoginError('No sign-in method is configured on this deployment.');
    } else {
        togglePasswordLogin(true);
    }
}

/** Load Google Identity Services on demand, once. */
function loadGoogleIdentityServices() {
    if (window.google && window.google.accounts) return Promise.resolve();
    return new Promise((resolve, reject) => {
        const existing = document.querySelector(`script[src="${GOOGLE_GSI_SRC}"]`);
        if (existing) {
            existing.addEventListener('load', resolve);
            existing.addEventListener('error', reject);
            return;
        }
        const script = document.createElement('script');
        script.src = GOOGLE_GSI_SRC;
        // GIS copies the nonce of its own <script> onto the <style> it injects
        // for the button; without it the CSP drops that stylesheet.
        script.nonce = cspNonce();
        script.async = true;
        script.defer = true;
        script.onload = resolve;
        script.onerror = reject;
        document.head.appendChild(script);
    });
}

function renderGoogleButton(clientId) {
    google.accounts.id.initialize({
        client_id: clientId,
        callback: onGoogleCredential,
        // No auto-select: this is an infrastructure console, so signing in is a
        // deliberate act rather than something that happens on page load.
        auto_select: false,
        cancel_on_tap_outside: true,
    });
    google.accounts.id.renderButton(
        document.getElementById('login-google-button'),
        { theme: 'filled_black', size: 'large', text: 'signin_with',
          shape: 'pill', width: 280 }
    );
}

/**
 * Exchange the Google ID token for a PulsarCD session token.
 * The credential proves who the visitor is; the backend decides whether that
 * address is on the allowlist, so a 403 here is "not invited", not "wrong
 * password".
 */
async function onGoogleCredential(response) {
    clearLoginError();
    try {
        const result = await fetch(`${API_BASE}/auth/google`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ credential: response.credential })
        });
        if (result.ok) {
            const data = await result.json();
            setAuthToken(data.token);
            await checkAuth();
            return;
        }
        const error = await result.json().catch(() => ({}));
        showLoginError(error.detail || 'Google sign-in failed.');
    } catch {
        showLoginError('Connection error');
    }
}

/** Show or hide the break-glass password form. */
function togglePasswordLogin(forceOpen) {
    const form = document.getElementById('login-form');
    const toggle = document.getElementById('login-fallback-toggle');
    const open = forceOpen === true || form.style.display === 'none';
    form.style.display = open ? '' : 'none';
    toggle.style.display = open ? 'none' : '';
    if (open) document.getElementById('login-username').focus();
}

function initLoginForm() {
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = document.getElementById('login-username').value;
        const password = document.getElementById('login-password').value;

        try {
            const response = await fetch(`${API_BASE}/auth/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password })
            });
            if (response.ok) {
                const data = await response.json();
                setAuthToken(data.token);
                clearLoginError();
                await checkAuth();
            } else {
                const error = await response.json().catch(() => ({}));
                showLoginError(response.status === 401
                    ? 'Invalid username or password'
                    : (error.detail || 'Sign-in failed'));
            }
        } catch {
            showLoginError('Connection error');
        }
    });
}

// ============== User Menu ==============

function updateUserMenu(username, role) {
    const nameEl = document.getElementById('user-menu-name');
    const usernameEl = document.getElementById('user-menu-username');
    const roleEl = document.getElementById('user-menu-role');
    if (nameEl) nameEl.textContent = username;
    if (usernameEl) usernameEl.textContent = username;
    if (roleEl) roleEl.textContent = role;

    // Only show settings for admins
    const settingsItem = document.getElementById('settings-menu-item');
    if (settingsItem) settingsItem.style.display = role === 'admin' ? '' : 'none';

    // Load GitHub status in user menu
    apiGet('/stacks/status').then(s => _updateGitHubBadge(s));
}

function _updateGitHubBadge(status) {
    const textEl = document.getElementById('user-menu-github-text');
    const dotEl = document.getElementById('user-menu-github-dot');
    if (!textEl || !dotEl) return;

    if (status && status.configured) {
        textEl.textContent = status.username ? `@${status.username}` : 'Connected';
        dotEl.className = 'user-menu-github-dot connected';
    } else {
        textEl.textContent = 'Not configured';
        dotEl.className = 'user-menu-github-dot disconnected';
    }
}

function toggleUserMenu() {
    const menu = document.getElementById('user-menu');
    menu.classList.toggle('open');
}

function closeUserMenu() {
    const menu = document.getElementById('user-menu');
    menu.classList.remove('open');
}

// Close user menu when clicking outside
document.addEventListener('click', (e) => {
    const menu = document.getElementById('user-menu');
    if (menu && !menu.contains(e.target)) {
        menu.classList.remove('open');
    }
});
