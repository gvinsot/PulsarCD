/* Pure, best-effort log indexing. Reporter summaries never become named tests. */
(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    else root.TestLogModel = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';

    const STATUSES = ['passed', 'failed', 'skipped', 'error', 'unknown'];
    const STATUS = { pass: 'passed', passed: 'passed', fail: 'failed', failed: 'failed', error: 'error',
        skipped: 'skipped', skip: 'skipped', pending: 'skipped', todo: 'skipped', xfail: 'skipped', xpass: 'passed' };
    const FILE = /(?:[^\s()]+\.(?:[cm]?[jt]sx?|py|cs|go|feature|rb))(?:\b|$)/i;
    const STATUS_SYMBOLS = { '✓': 'passed', '✔': 'passed', '√': 'passed', '×': 'failed', '✕': 'failed', '✗': 'failed', '✖': 'failed', '↓': 'skipped', '○': 'skipped', '-': 'skipped' };

    function cleanLine(value) {
        let text = String(value == null ? '' : value);
        text = text.replace(/\x1b\][^\x07]*(?:\x07|\x1b\\)/g, '').replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, '');
        // Terminal progress output overwrites the same physical line.
        text = text.split('\r').filter(part => part.length).pop() || '';
        return text
            .replace(/^\s*#\d+\s+\d+(?:\.\d+)?\s/, '') // Docker BuildKit output.
            .replace(/^\s*(?:\[?\d{4}-\d{2}-\d{2}[T ][\d:.]+(?:Z|[+-]\d{2}:?\d{2})?\]?\s+)/, '')
            .replace(/^\s*[\w.-]+\s*\|\s?/, ''); // Docker Compose service prefix.
    }

    function classify(value) {
        const name = String(value).replace(/([a-z])([A-Z])/g, '$1 $2').toLowerCase();
        let type = 'Other';
        if (/\b(e2e|end[ _-]to[ _-]end|playwright|cypress|selenium)\b/.test(name)) type = 'End-to-end';
        else if (/\b(integration|integrations)\b/.test(name)) type = 'Integration';
        else if (/\b(security|vulnerabilit\w*|csrf|xss|injection|permission\w*)\b/.test(name)) type = 'Security';
        else if (/\b(performance|benchmark\w*|load[ _-]?test|latency)\b/.test(name)) type = 'Performance';
        else if (/\b(contract|schema)\b/.test(name)) type = 'Contract';
        else if (/\b(accessibility|a11y)\b/.test(name)) type = 'Accessibility';
        else if (/\b(unit|units)\b/.test(name)) type = 'Unit';

        let theme = 'General';
        if (/\b(auth\w*|login|logout|session\w*|token\w*|permission\w*|security|csrf|xss|injection)\b/.test(name)) theme = 'Authentication & security';
        else if (/\b(deploy\w*|pipeline\w*|docker|swarm|build\w*|release\w*|swiftproof)\b/.test(name)) theme = 'Build & deployment';
        else if (/\b(database\w*|db|sql\w*|mongo\w*|storage|persist\w*|migration\w*|cache\w*)\b/.test(name)) theme = 'Data & storage';
        else if (/\b(api|http|route\w*|endpoint\w*|request\w*|response\w*|webhook\w*)\b/.test(name)) theme = 'API & networking';
        else if (/\b(ui|component\w*|render\w*|button\w*|modal\w*|browser\w*|accessibility|a11y)\b/.test(name)) theme = 'Interface & accessibility';
        else if (/\b(performance|benchmark\w*|latency|memory|concurren\w*|timeout\w*)\b/.test(name)) theme = 'Performance & reliability';
        return { type, theme };
    }

    function counts(entries) {
        const result = { total: 0, passed: 0, failed: 0, skipped: 0, error: 0, unknown: 0, suites: 0 };
        for (const entry of entries) {
            if (entry.kind === 'suite') { result.suites++; continue; }
            result.total++;
            result[STATUSES.includes(entry.status) ? entry.status : 'unknown']++;
        }
        return result;
    }

    function parse(input, options) {
        const source = Array.isArray(input) ? input : input ? String(input).split(/\n/) : [];
        const lines = source.map(value => cleanLine(value && typeof value === 'object' ? value.line : value));
        const offset = Math.max(0, Number(options && options.lineOffset) || 0);
        const entries = [], summaries = [], sections = [];
        const byName = new Map();
        let framework = '', currentFile = '', suiteName = '';
        let lastEntry = null;
        const tapSubtests = new Map();

        function add(entry, index, duplicate) {
            const file = entry.file || '';
            const category = classify([file, entry.name].join(' ').replace(/[_/\\.:-]/g, ' '));
            const record = Object.assign({ id: 'test-' + (index + offset), line: index + offset,
                endLine: index + offset, file, kind: 'test', status: 'unknown', framework: framework || 'Other',
                type: category.type, theme: category.theme }, entry);
            const key = record.framework + ':' + record.kind + ':' + record.name;
            const existing = duplicate && byName.get(key);
            if (existing) {
                existing.status = record.status;
                existing.detailLine = record.line;
                if (record.diagnostic) existing.diagnostic = record.diagnostic;
                return existing;
            }
            if (lastEntry) lastEntry.endLine = Math.max(lastEntry.line, index + offset - 1);
            entries.push(record);
            byName.set(key, record);
            lastEntry = record;
            return record;
        }

        function section(name, index) {
            const previous = sections[sections.length - 1];
            if (previous && previous.line === index + offset) return;
            if (previous) previous.endLine = index + offset - 1;
            sections.push({ id: 'section-' + (index + offset), name, title: name,
                line: index + offset, endLine: index + offset, framework: framework || 'Other' });
        }

        function summary(text, index, reporter) {
            const totals = {};
            const matcher = /\b(\d+)\s+(passed|failed|skipped|errors?|xfailed|xpassed|todo|pending)\b/gi;
            for (const match of text.matchAll(matcher)) {
                const status = { errors: 'error', xfailed: 'skipped', xpassed: 'passed' }[match[2].toLowerCase()] || STATUS[match[2].toLowerCase()] || match[2].toLowerCase();
                totals[status] = (totals[status] || 0) + Number(match[1]);
            }
            // TAP and dotnet put the count after the status word.
            if (!Object.keys(totals).length) {
                for (const match of text.matchAll(/\b(passed|failed|skipped|pass|fail|todo)\s*:?\s+(\d+)\b/gi)) {
                    const status = STATUS[match[1].toLowerCase()];
                    totals[status] = (totals[status] || 0) + Number(match[2]);
                }
            }
            summaries.push({ text: text.trim(), line: index + offset, framework: reporter, counts: totals });
        }

        if (lines.length) section('Output', 0);
        lines.forEach((line, index) => {
            const text = line.trim();
            if (!text) return;
            let match;
            if (/\btest session starts\b/.test(text)) { framework = 'pytest'; currentFile = ''; section('pytest session', index); return; }
            if (/^(?:RUN|DEV)\s+v\d|\bvitest\b/i.test(text) && !/::/.test(text)) framework = 'Vitest';
            if (/^TAP version \d+/.test(text)) { framework = 'TAP'; section('TAP tests', index); return; }

            // Preserve execution phases and failure/traceback headings as raw-log shortcuts.
            if ((match = text.match(/^(?:={3,}|_{3,})\s*(.+?)\s*(?:={3,}|_{3,})$/)) && /[a-z]/i.test(match[1])) {
                section(match[1], index);
            } else if (/^\[(?:INFO|ERROR|WARNING|SUCCESS)\]\s/.test(text) || /^SwiftProof:/i.test(text)) {
                section(text, index);
            } else if (index - (sections[sections.length - 1]?.line - offset || 0) >= 100) {
                section('Output · line ' + (index + offset + 1), index);
            }

            // pytest -v/--tb: names are available only with explicit node ids.
            match = text.match(/^(.+?\.py::.+?)\s+(PASSED|FAILED|SKIPPED|ERROR|XFAIL|XPASS)\b/i);
            if (match) {
                framework = 'pytest';
                add({ name: match[1], file: match[1].split('::')[0], status: STATUS[match[2].toLowerCase()] }, index);
                return;
            }
            match = text.match(/^(FAILED|ERROR|PASSED|SKIPPED|XFAIL|XPASS)\s+(.+?\.py::.+?)(?:\s+-\s+(.+))?$/);
            if (match) {
                framework = 'pytest';
                add({ name: match[2], file: match[2].split('::')[0], status: STATUS[match[1].toLowerCase()], diagnostic: match[3] || '' }, index, true);
                return;
            }
            // Quiet pytest reports file-level progress; never expand dots into fictional named tests.
            match = text.match(/^([^\s]+\.py)\s+([.FsxXE]+)(?:\s+\[\s*\d+%\])?$/);
            if (match) {
                framework = 'pytest';
                add({ name: match[1], file: match[1], kind: 'suite', status: /[FE]/.test(match[2]) ? 'failed' : /[.X]/.test(match[2]) ? 'passed' : 'skipped' }, index);
                return;
            }
            if (/\b\d+ (?:passed|failed|skipped|errors?|xfailed|xpassed)\b/.test(text) && /\bin \d|[=]{3}/.test(text)) {
                summary(text, index, framework || 'pytest'); return;
            }

            // Jest and Vitest file headers; a suite is kept separate from its named tests.
            match = text.match(/^(PASS|FAIL)\s+(.+\.(?:[cm]?[jt]sx?))(?:\s+\(.*\))?$/);
            if (match) {
                framework = 'Jest'; currentFile = match[2]; suiteName = '';
                add({ name: currentFile, file: currentFile, kind: 'suite', status: STATUS[match[1].toLowerCase()] }, index);
                return;
            }
            match = text.match(/^[✓✔√×✕✗✖↓○❯]\s+(.+\.(?:[cm]?[jt]sx?))\s+\(\d+ tests?[^)]*\)(.*)$/);
            if (match) {
                framework = 'Vitest'; currentFile = match[1]; suiteName = '';
                add({ name: currentFile, file: currentFile, kind: 'suite', status: /\d+ failed/.test(text) ? 'failed' : (STATUS_SYMBOLS[text[0]] || 'unknown') }, index);
                return;
            }
            if (/^(?:Test Suites:|Tests:|Test Files\s|Tests\s+\d)/.test(text)) {
                summary(text, index, framework || 'Jest'); return;
            }
            match = text.match(/^([✓✔√×✕✗✖↓○])\s+(.+?)(?:\s+\((\d+(?:\.\d+)?\s*m?s)\)|\s+(\d+(?:\.\d+)?\s*m?s))?$/);
            if (match && (currentFile || framework === 'Jest' || framework === 'Vitest')) {
                add({ name: suiteName ? suiteName + ' › ' + match[2] : match[2], file: currentFile,
                    status: STATUS_SYMBOLS[match[1]], duration: match[3] || match[4] || '' }, index);
                return;
            }
            if (currentFile && /^\s{2,}\S/.test(line) && !/^[✓✔√×✕✗✖↓○>❯]|^(?:at |Error|AssertionError|Expected|Received|\d|[+\-{}|])/.test(text)) {
                // Jest describe() headings are indented but have no status marker.
                if (framework === 'Jest' && !/[=:]$/.test(text)) suiteName = text;
            }

            // TAP, including nested subtests. Parent results are suites, not additional leaf tests.
            match = line.match(/^(\s*)# Subtest:\s*(.+)$/);
            if (match) {
                framework = 'TAP';
                const depth = match[1].length;
                for (const [level, item] of tapSubtests) {
                    if (level < depth) item.hasChildren = true;
                    if (level >= depth) tapSubtests.delete(level);
                }
                tapSubtests.set(depth, { name: match[2], hasChildren: false });
                return;
            }
            match = line.match(/^(\s*)(not ok|ok)\s+(\d+)(?:\s+-)?(?:\s+(.+?))?(?:\s+#\s*(SKIP|TODO)\b.*)?$/i);
            if (match) {
                framework = 'TAP';
                const depth = match[1].length;
                const subtest = tapSubtests.get(depth);
                const name = match[4] || subtest?.name || 'Test ' + match[3];
                add({ name, status: match[5] ? 'skipped' : match[2].toLowerCase() === 'ok' ? 'passed' : 'failed',
                    kind: subtest?.hasChildren ? 'suite' : 'test' }, index);
                tapSubtests.delete(depth);
                return;
            }
            if (/^# (?:tests|pass|fail|cancelled|skipped|todo)\s+\d+/.test(text)) { summary(text, index, 'TAP'); return; }

            // dotnet test / xUnit verbose console output.
            match = text.match(/^(Passed|Failed|Skipped)\s+(.+?)\s+\[([^\]]+)\]$/);
            if (match) {
                framework = '.NET';
                add({ name: match[2], status: STATUS[match[1].toLowerCase()], duration: match[3] }, index);
                return;
            }
            if (/^(?:Passed!|Failed!|Total tests:|Test Run (?:Successful|Failed))/.test(text)) { summary(text, index, '.NET'); return; }

            match = text.match(/^--- (PASS|FAIL|SKIP):\s+(.+?)\s+\(([^)]+)\)$/);
            if (match) {
                framework = 'Go';
                add({ name: match[2], status: STATUS[match[1].toLowerCase()], duration: match[3] }, index);
            }
        });
        const finalLine = Math.max(offset, lines.length + offset - 1);
        // Go reports a result for both parent tests and their subtests.
        const goParents = new Set();
        for (const entry of entries) {
            if (entry.framework !== 'Go') continue;
            const parts = entry.name.split('/');
            while (parts.length > 1) { parts.pop(); goParents.add(parts.join('/')); }
        }
        for (const entry of entries) {
            if (entry.framework === 'Go' && goParents.has(entry.name)) entry.kind = 'suite';
        }
        if (lastEntry) lastEntry.endLine = finalLine;
        if (sections.length) sections[sections.length - 1].endLine = finalLine;
        return {
            entries, sections, summaries, counts: counts(entries), lineCount: lines.length,
            frameworks: [...new Set(entries.map(entry => entry.framework))].sort(),
            types: [...new Set(entries.map(entry => entry.type))].sort(),
            themes: [...new Set(entries.map(entry => entry.theme))].sort(),
        };
    }

    function group(entries, mode) {
        const field = ['type', 'theme', 'framework', 'file', 'status'].includes(mode) ? mode : 'type';
        const results = { failed: 'ERROR', error: 'ERROR', passed: 'SUCCESS', skipped: 'SKIPPED', unknown: 'UNKNOWN' };
        const groups = new Map();
        for (const entry of entries) {
            const key = mode === 'result' ? (results[entry.status] || 'UNKNOWN')
                : entry[field] || (field === 'file' ? 'No file reported' : 'Other');
            if (!groups.has(key)) groups.set(key, { key, label: key, entries: [] });
            groups.get(key).entries.push(entry);
        }
        const ordered = [...groups.values()];
        if (mode === 'result') {
            const order = ['ERROR', 'SUCCESS', 'SKIPPED', 'UNKNOWN'];
            ordered.sort((first, second) => order.indexOf(first.key) - order.indexOf(second.key));
        }
        return ordered.map(value => Object.assign(value, { counts: counts(value.entries) }));
    }

    return { parse, group, cleanLine, classify };
});
