/**
 * PulsarCD frontend: Content Security Policy helpers: start-hidden markup and the style nonce.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Content Security Policy ==============
//
// The CSP accepts no inline script and no inline style, so the markup (the
// page and every template in the core/ and views/ scripts) carries neither on*
// handler attributes nor style attributes. What those attributes used to do
// happens here and in the UI_HANDLERS registry (app.js) instead.

/**
 * Move the initial display:none of each .start-hidden element into its inline
 * style. The class keeps the element hidden until this runs; afterwards the
 * show/hide code finds the inline display it has always toggled, so setting
 * style.display = '' still shows the element. Run it on markup right after
 * inserting it.
 */
function adoptStartHidden(root) {
    root.querySelectorAll('.start-hidden').forEach(el => {
        el.style.display = 'none';
        el.classList.remove('start-hidden');
    });
}

/**
 * The per-response nonce the CSP accepts on <style> elements. It is read
 * through the property: the browser blanks the attribute once the page loads.
 */
function cspNonce() {
    return document.querySelector('meta[name="csp-nonce"]')?.nonce || '';
}

/**
 * Run fn while document.createElement stamps new <style> elements with the
 * page's nonce. xterm's DOM renderer injects two of them and offers no way to
 * pass a nonce; without one the CSP drops them and the terminal loses its
 * colours and cell layout. The override lasts for the call only.
 */
function withStyleNonce(fn) {
    const nonce = cspNonce();
    const createElement = document.createElement;
    document.createElement = function (tagName, options) {
        const el = createElement.call(this, tagName, options);
        if (nonce && String(tagName).toLowerCase() === 'style') el.nonce = nonce;
        return el;
    };
    try {
        return fn();
    } finally {
        delete document.createElement;
    }
}
