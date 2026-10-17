// Settings > Notifications: read-only view of the outbound notification
// configuration (PULSARCD_NOTIFY__*) and a "send test notification" button.
//
// Kept out of app.js on purpose: it wires its own listeners instead of going
// through UI_HANDLERS, and only relies on the apiGet/apiPost helpers.
(function () {
    'use strict';

    const EVENT_LABELS = {
        pipeline_failure: 'Pipeline failure (any stage)',
        deploy_success: 'Successful production deploy',
        rollback: 'Automatic rollback',
        auto_ban: 'Automatic edge ban',
        recurring_error: 'Recurring error pattern',
    };

    function esc(value) {
        return String(value ?? '').replace(/[&<>"']/g, (c) => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
        }[c]));
    }

    function channelLine(name, ch) {
        if (!ch || !ch.configured) {
            return `<li><strong>${esc(name)}</strong>: not configured</li>`;
        }
        let detail = '';
        if (name === 'ntfy') {
            detail = `${esc(ch.server)}${ch.token_set ? ', access token set' : ''}`;
        } else if (name === 'webhook') {
            detail = `${esc(ch.host)} (${esc(ch.format)} format)`;
        } else if (name === 'email') {
            detail = `${esc(ch.server)}:${esc(ch.port)}, ${esc(ch.tls)}, ${esc(ch.recipients)} recipient(s)`;
        }
        return `<li><strong>${esc(name)}</strong>: ${detail}</li>`;
    }

    async function loadNotificationSettings() {
        const box = document.getElementById('notifications-status');
        if (!box) return;
        box.innerHTML = '<div class="loading-placeholder">Loading...</div>';
        const data = await apiGet('/admin/notifications');
        if (!data) {
            box.innerHTML = '<p class="settings-status error">Failed to load the notification settings.</p>';
            return;
        }
        const channels = data.channels || {};
        const events = Object.entries(data.events || {})
            .map(([kind, on]) => `<li>${esc(EVENT_LABELS[kind] || kind)}: ${on ? 'on' : 'off'}</li>`)
            .join('');
        const last = data.last_results && data.last_results.channels
            ? Object.entries(data.last_results.channels)
                .map(([n, r]) => `${esc(n)} ${r.ok ? 'ok' : 'failed (' + esc(r.error) + ')'}`)
                .join(', ')
            : '';
        box.innerHTML = `
            <h4>Channels</h4>
            <ul>${['ntfy', 'webhook', 'email'].map((n) => channelLine(n, channels[n])).join('')}</ul>
            <h4>Events</h4>
            <ul>${events}</ul>
            <p class="settings-hint">
                Same event at most once every ${esc(data.dedupe_seconds)} s, at most
                ${esc(data.max_per_hour)} notifications per hour.
                Sent: ${esc(data.sent)}.
                ${last ? `Last delivery (${esc(data.last_results.event)}): ${last}.` : ''}
            </p>`;
        const button = document.getElementById('notifications-test-btn');
        if (button) button.disabled = !data.enabled;
    }

    async function sendTestNotification() {
        const result = document.getElementById('notifications-test-result');
        result.textContent = 'Sending...';
        result.className = 'settings-status';
        const data = await apiPost('/admin/notifications/test');
        if (!data) {
            result.textContent = 'Request failed';
            result.className = 'settings-status error';
            return;
        }
        if (data.detail) {
            result.textContent = data.detail;
            result.className = 'settings-status error';
            return;
        }
        const parts = Object.entries(data.channels || {})
            .map(([n, r]) => `${n}: ${r.ok ? 'sent' : 'failed (' + r.error + ')'}`);
        result.textContent = parts.join(' / ') || 'No channel configured';
        result.className = 'settings-status ' + (data.sent ? 'success' : 'error');
        loadNotificationSettings();
    }

    document.addEventListener('click', (event) => {
        const target = event.target instanceof Element ? event.target : null;
        if (!target) return;
        if (target.closest('.settings-tab[data-tab="notifications"]')) {
            loadNotificationSettings();
        } else if (target.closest('#notifications-test-btn:not([disabled])')) {
            sendTestNotification();
        }
    });
})();
