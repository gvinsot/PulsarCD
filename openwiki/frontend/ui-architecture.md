---
type: architecture
title: Frontend
description: How PulsarCD's unbundled UI works, using classic scripts sharing globals in a fixed load order, a strict Content Security Policy with an event handler registry instead of inline code, a JWT in local storage, and rules for rendering and testing without a build step.
tags: [frontend, javascript, csp, no-build, ui]
verified:
  - by: openwiki/0.9.0
    at: 2026-11-06T10:52:04.857Z
sources:
  - id: openwiki-source-9600acfef75e03bd87301993
    resource: repo://backend/security_headers.py
  - id: openwiki-source-e483fd3285d99d05c7b265cf
    resource: repo://frontend/AGENTS.md
  - id: openwiki-source-1195d204477ba1ef200811b5
    resource: repo://frontend/index.html
  - id: openwiki-source-18c3f1c4948d46ef68388230
    resource: repo://frontend/static/js/app.js
  - id: openwiki-source-36713e08302b62f986790ca7
    resource: repo://frontend/static/js/core/api.js
  - id: openwiki-source-7872cfbcbf4a7e0eb585b202
    resource: repo://frontend/static/js/core/auth.js
  - id: openwiki-source-352e6d891aba997ef6263bd2
    resource: repo://frontend/static/js/core/csp.js
  - id: openwiki-source-cfa9935307a56f4a5cc2b6f3
    resource: repo://tests/test_security_view.cjs
generated: { by: "claude-code", at: "2026-11-06T10:52:04.857Z" }
---

# Frontend

The UI has **no framework, bundler or package manager**. `frontend/index.html` loads classic scripts that share one global scope, and `frontend/static/` is served directly by the backend at `/static`. Don't introduce a build step for an ordinary change.

Navigation uses the URL hash: `core/state.js` maps hashes to views, such as `#Computers` to `containers`.

## Script order is the module system

Scripts run in the order listed at the bottom of `index.html`, so **a name must be defined before it's used at load time**. Check that order and existing globals before adding or moving a function, since a duplicate name silently overrides an earlier one.

| Order | Files | Content |
|---|---|---|
| 1 | CDN scripts | Chart.js and xterm, pinned with integrity hashes |
| 2 | `test-log-model.js`, `test-log-viewer.js`, `run-history.js` | Log viewers |
| 3 | `core/csp.js`, `utils.js`, `state.js`, `api.js`, `auth.js`, `navigation.js` | Shared plumbing |
| 4 | `views/*.js` | One per view or modal family |
| 5 | `app.js` | Key bindings and `UI_HANDLERS`, which names functions from all the others |
| 6 | `notifications-settings.js`, `previews.js` | Later additions |

Styles are one sheet per area in `static/css/`, linked in cascade order.

## Content Security Policy

`backend/security_headers.py` sets a strict policy, and it **must stay in sync with `index.html`**:

- **Scripts:** files only, with no `unsafe-inline`.
- **Styles:** files only, including `style=""` attributes.
- **Fonts** are self-hosted, and images are limited to `self` and `data:`.
- **Google sign-in** directives are added only when it's configured, using its narrowest documented paths.

So the markup and all templates can't carry `on*` attributes or inline styles.

### Wiring events

Use `data-click`, `data-change` and similar attributes, read arguments with `uiArgs()`, and register the handler in `UI_HANDLERS` in `app.js`:

```html
<button data-click="refreshStacks" data-arg="prod">Refresh</button>
```

Dispatch looks up names **in `UI_HANDLERS` only, never on `window`**, so markup that got past escaping can't call arbitrary functions. A new handler that isn't registered simply does nothing.

### Other CSP helpers

- `adoptStartHidden` converts the `.start-hidden` class to an inline display state after insertion. Run it on new markup.
- `withStyleNonce` stamps a page nonce on `<style>` elements for the call's duration, because xterm's DOM renderer injects them and offers no way to pass a nonce. Without it, the terminal loses its colours and layout.

## Escaping

Views build HTML from strings, and much of what they show comes from logs, which **anyone able to reach a monitored service can influence**. Always escape displayed data with the helpers in `core/utils.js`. The CSP blocks inline script, but it doesn't stop injected markup from misleading a user or calling a registered handler.

## Authentication

`core/auth.js` keeps the JWT in `localStorage` as `pulsarcd_token` and sends it as a Bearer header. This is a design difference from PulsarTeam: an XSS here could read the token, which raises the stakes of correct escaping.

Two browser clients can't set headers, the SSE action log stream and the terminal WebSocket, so they pass the token in the query string. The backend redacts it from logs; see [Authentication and access control](../security/authentication-and-access.md).

## API helpers

| Helper | Use |
|---|---|
| `apiGet` | Polling views. Returns `null` on any failure, and shows the login on 401 |
| `apiGetOrError` | Screens opened on purpose. Returns `{data}` or `{error}`, so the UI can say why, such as a refused SSH connection |

Some GETs are admin-only, so a viewer gets a 403. `_reportForbidden` handles this.

## Performance

A view that refreshes periodically keeps running whether or not anything changed, so:

- **Clean up timers, subscriptions and resources when a view closes.**
- Avoid infinite CSS animations outside ongoing work, and `backdrop-filter` on persistent elements. An opacity animation on the top bar's status dot once kept the compositor active on every view.
- Spinners for ongoing work are fine.

`docs/FRONTEND_PERFORMANCE.md` has the investigation and how to check with a Performance trace.

## Testing

`bash scripts/dev.sh frontend` runs `tests/test_*.cjs` with Node's built-in runner, no npm install needed. Tests load script source into a `vm` context with fake DOM elements, as `tests/test_security_view.cjs` shows.

**These don't replace looking at the result.** State explicitly when a presentation change hasn't been checked visually.
