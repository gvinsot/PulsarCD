"""FastAPI routers, one module per API domain.

backend.api includes them in order: keep that order in mind when adding a
route whose path can overlap another one.

App state (settings, the service clients, the action registries, ...) lives in
backend.api and is read as ``api.<name>`` at call time, so the lifespan hook
and tests patching backend.api reach this code too.
"""

# Load the app first: backend.api includes every router, and the routers read
# it at call time.  Importing one of them directly (before backend.api) would
# otherwise let backend.api find that router only half-initialized.
from .. import api  # noqa: E402,F401
