"""src package for Synthos Validation Engine.

This file purposefully keeps top-level imports minimal so importing
``src`` in lightweight environments (CI, linters) won't try to import
heavy ML dependencies like ``torch`` or ``pandas``.

Import concrete modules explicitly when you need them, e.g.::

    from src.orchestrator import SynthosOrchestrator

This makes the package safe to import in CI jobs that only need
to verify packaging, metadata, or run quick unit tests.
"""

__version__ = "1.0.0"

# Keep a minimal public surface. Heavy submodules should be imported
# explicitly by callers to avoid pulling large optional dependencies
# at import time.
__all__ = [
    "__version__",
]
