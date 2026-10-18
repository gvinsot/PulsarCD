/**
 * PulsarCD frontend: Web terminal (xterm.js over a WebSocket).
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Terminal ==============

let terminalInstance = null;
let terminalFitAddon = null;
let terminalSocket = null;
let terminalInitialized = false;
let terminalDataDisposable = null;
let terminalResizeDisposable = null;

function initTerminal() {
    if (terminalInitialized) return;

    const container = document.getElementById('terminal-container');
    if (!container) return;

    terminalInstance = new Terminal({
        cursorBlink: true,
        fontSize: 14,
        fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
        theme: {
            background: '#0a0e14',
            foreground: '#e6e6e6',
            cursor: '#00d4aa',
            cursorAccent: '#0a0e14',
            selectionBackground: 'rgba(0, 212, 170, 0.3)',
            black: '#0a0e14',
            red: '#ef4444',
            green: '#22c55e',
            yellow: '#f59e0b',
            blue: '#3b82f6',
            magenta: '#8b5cf6',
            cyan: '#00d4aa',
            white: '#e6e6e6',
            brightBlack: '#6e7681',
            brightRed: '#f87171',
            brightGreen: '#4ade80',
            brightYellow: '#fbbf24',
            brightBlue: '#60a5fa',
            brightMagenta: '#a78bfa',
            brightCyan: '#2dd4bf',
            brightWhite: '#f5f5f5',
        },
        scrollback: 10000,
        convertEol: true,
    });

    terminalFitAddon = new FitAddon.FitAddon();
    terminalInstance.loadAddon(terminalFitAddon);
    terminalInstance.loadAddon(new WebLinksAddon.WebLinksAddon());

    withStyleNonce(() => terminalInstance.open(container));
    loadTerminalWebglRenderer();
    terminalFitAddon.fit();

    terminalInitialized = true;

    window.addEventListener('resize', () => {
        if (currentView === 'terminal' && terminalFitAddon) {
            terminalFitAddon.fit();
        }
    });

    const resizeObserver = new ResizeObserver(() => {
        if (currentView === 'terminal' && terminalFitAddon) {
            terminalFitAddon.fit();
        }
    });
    resizeObserver.observe(container);

    connectTerminal();
}

/**
 * Draw the terminal with WebGL instead of xterm's default DOM renderer, which
 * colours 24-bit text through inline style attributes: the CSP refuses those, and
 * a nonce only vouches for <style> elements. Without WebGL, or once its
 * context is lost, xterm is back on the DOM renderer, where such text shows in
 * the default colour.
 */
function loadTerminalWebglRenderer() {
    try {
        const webgl = new WebglAddon.WebglAddon();
        // Disposing the addon rebuilds the DOM renderer, <style> elements included.
        webgl.onContextLoss(() => withStyleNonce(() => webgl.dispose()));
        terminalInstance.loadAddon(webgl);
    } catch (e) {
        console.warn('WebGL terminal renderer unavailable, using the DOM renderer:', e);
    }
}

function connectTerminal() {
    if (terminalSocket && terminalSocket.readyState === WebSocket.OPEN) return;

    updateTerminalStatus('connecting');

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const cols = terminalInstance ? terminalInstance.cols : 80;
    const rows = terminalInstance ? terminalInstance.rows : 24;
    const token = getAuthToken() || '';
    terminalSocket = new WebSocket(
        `${protocol}//${window.location.host}/api/terminal/ws?cols=${cols}&rows=${rows}&token=${encodeURIComponent(token)}`
    );

    terminalSocket.binaryType = 'arraybuffer';

    terminalSocket.onopen = () => {
        updateTerminalStatus('connected');
        if (terminalDataDisposable) terminalDataDisposable.dispose();
        if (terminalResizeDisposable) terminalResizeDisposable.dispose();
        terminalDataDisposable = terminalInstance.onData((data) => {
            if (terminalSocket && terminalSocket.readyState === WebSocket.OPEN) {
                terminalSocket.send(data);
            }
        });
        terminalResizeDisposable = terminalInstance.onResize(({ cols, rows }) => {
            if (terminalSocket && terminalSocket.readyState === WebSocket.OPEN) {
                terminalSocket.send(JSON.stringify({ type: 'resize', cols, rows }));
            }
        });
        terminalInstance.focus();
    };

    terminalSocket.onmessage = (event) => {
        if (event.data instanceof ArrayBuffer) {
            terminalInstance.write(new Uint8Array(event.data));
        } else {
            terminalInstance.write(event.data);
        }
    };

    terminalSocket.onclose = (event) => {
        updateTerminalStatus('disconnected');
        if (!event.wasClean) {
            terminalInstance.writeln('\r\n\x1b[31m[Connection lost. Click Reconnect to restore.]\x1b[0m');
        }
    };

    terminalSocket.onerror = () => {
        updateTerminalStatus('disconnected');
    };
}

function reconnectTerminal() {
    if (terminalSocket) {
        terminalSocket.close();
        terminalSocket = null;
    }
    if (terminalInstance) {
        terminalInstance.clear();
    }
    connectTerminal();
}

function updateTerminalStatus(status) {
    const el = document.getElementById('terminal-status');
    if (!el) return;
    el.className = `terminal-status ${status}`;
    el.textContent = status.charAt(0).toUpperCase() + status.slice(1);
}
