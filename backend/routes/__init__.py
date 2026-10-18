"""FastAPI routers, one module per API domain.

backend.api includes them in order: keep that order in mind when adding a
route whose path can overlap another one.

App state (settings, the service clients, the action registries, ...) lives in
backend.api and is read as ``api.<name>`` at call time, so the lifespan hook
and tests patching backend.api reach this code too.
"""
