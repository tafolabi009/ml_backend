"""
Verified runtime binding to the Temporal Eigenstate Network (TEN) package.

Every number Synthos produces — collapse scores, approval decisions, PDF
certificates, warranties — is only meaningful if a genuine TEN model computed
it. Two defects previously made that unsafe, and this module closes both.

1. Name collision (supply chain).
   The distribution name ``ten`` on PyPI belongs to an unrelated third-party
   project: a web exploit framework. ``pip install ten`` therefore pulls foreign
   code onto the machine, and a bare ``from ten import TEN`` would import and
   execute it. This module resolves the top-level ``ten`` module to its owning
   distribution *before* importing anything, and refuses to import a ``ten``
   that is not ours. Provenance is then double-checked with a marker attribute
   that only our package sets.

   Install the real package with::

       pip install "genovo-ten @ git+https://github.com/genovotechnologies/temporal-eigenstate-networks.git"

2. Silent degradation (correctness).
   When the import failed, the model wrappers fell back to a mock whose
   ``forward()`` returns zeros — and the pipeline carried on and emitted a
   score. A validation product that issues a certificate derived from zeros is
   worse than one that refuses to run. Scoring paths now fail closed through
   :func:`require_ten`.

Mock behaviour is still available for offline unit tests, but only when
explicitly requested via ``SYNTHOS_ALLOW_MOCK_TEN=1``, and every use is logged
loudly so it can never pass unnoticed.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

__all__ = [
    "TEN",
    "TEN_AVAILABLE",
    "TEN_IMPORT_ERROR",
    "TENUnavailableError",
    "mock_allowed",
    "require_ten",
    "ten_status",
]

#: The only distribution permitted to provide the top-level ``ten`` module.
EXPECTED_DISTRIBUTION = "genovo-ten"

#: The top-level module name the distribution installs.
TEN_MODULE = "ten"

#: Environment variable that opts in to mock scoring. Tests only.
MOCK_ENV_VAR = "SYNTHOS_ALLOW_MOCK_TEN"

INSTALL_HINT = (
    'pip install "genovo-ten @ '
    'git+https://github.com/genovotechnologies/temporal-eigenstate-networks.git"'
)


class TENUnavailableError(RuntimeError):
    """Raised when a scoring path needs TEN and no verified TEN is installed.

    This is a fail-closed refusal, not a crash. It means Synthos would have had
    to produce a score from a model that computed nothing, so it declined.
    """


def mock_allowed() -> bool:
    """True when the operator has explicitly opted in to mock scoring."""
    return os.getenv(MOCK_ENV_VAR, "").strip().lower() in {"1", "true", "yes", "on"}


def _normalise(name: str) -> str:
    """Normalise a distribution name for comparison (PEP 503-ish)."""
    return name.replace("_", "-").strip().lower()


def _owning_distributions(module_name: str) -> List[str]:
    """Which installed distributions provide ``module_name``.

    Returns an empty list when the mapping cannot be determined, which is
    treated as "unknown" rather than "wrong" — the provenance marker check
    below is the backstop in that case.
    """
    try:
        from importlib.metadata import packages_distributions
    except ImportError:  # pragma: no cover - Python < 3.10
        return []
    try:
        return list(packages_distributions().get(module_name, []) or [])
    except Exception:  # pragma: no cover - defensive
        return []


def _load_ten() -> Tuple[Optional[Any], Optional[str]]:
    """Import TEN only if the installed ``ten`` module is genuinely ours."""
    owners = [_normalise(d) for d in _owning_distributions(TEN_MODULE)]
    if owners and _normalise(EXPECTED_DISTRIBUTION) not in owners:
        return None, (
            f"refusing to import module '{TEN_MODULE}': it is provided by "
            f"{sorted(owners)}, not by '{EXPECTED_DISTRIBUTION}'. The name "
            f"'{TEN_MODULE}' on PyPI belongs to an unrelated third-party project, "
            f"so this is almost certainly the wrong package and importing it would "
            f"execute foreign code. Install ours with: {INSTALL_HINT}"
        )

    try:
        import ten as ten_module  # noqa: WPS433 - guarded, deliberate
    except ImportError as exc:
        return None, f"{exc}. Install with: {INSTALL_HINT}"

    if not getattr(ten_module, "__genovo_ten__", False):
        return None, (
            f"imported a module named '{TEN_MODULE}' that does not carry the Genovo "
            f"provenance marker (__genovo_ten__). Refusing to score with it. "
            f"Install ours with: {INSTALL_HINT}"
        )

    ten_cls = getattr(ten_module, "TEN", None)
    if ten_cls is None:
        return None, (
            f"module '{TEN_MODULE}' is ours but exposes no 'TEN' class; the "
            f"installation looks broken. Reinstall with: {INSTALL_HINT}"
        )

    return ten_cls, None


TEN, TEN_IMPORT_ERROR = _load_ten()
TEN_AVAILABLE = TEN is not None

if TEN_AVAILABLE:
    logger.info(
        f"TEN loaded and provenance-verified (distribution '{EXPECTED_DISTRIBUTION}')."
    )
elif mock_allowed():
    logger.warning(
        f"TEN unavailable ({TEN_IMPORT_ERROR}) and {MOCK_ENV_VAR} is set. "
        "Mock scoring is enabled: any result produced is NOT a valid validation "
        "and must never be certified, warranted, or shown to a customer."
    )
else:
    logger.error(
        f"TEN unavailable: {TEN_IMPORT_ERROR} Scoring paths will fail closed."
    )


def require_ten(context: str) -> Optional[Any]:
    """Return the verified TEN class, or refuse to continue.

    Args:
        context: What needed TEN, used in the error message. For example
            ``"collapse detection"`` or ``"cascade training"``.

    Returns:
        The verified ``TEN`` class, or ``None`` when mock scoring has been
        explicitly enabled for offline tests.

    Raises:
        TENUnavailableError: When TEN is unavailable and mock scoring has not
            been explicitly enabled.
    """
    if TEN_AVAILABLE:
        return TEN

    if mock_allowed():
        logger.warning(
            f"{MOCK_ENV_VAR} is set — {context} is running against a mock model "
            "that returns zeros. Results are NOT valid and must never be issued "
            "to a customer, certified, or warranted."
        )
        return None

    raise TENUnavailableError(
        f"{context} requires a verified TEN installation and none is available: "
        f"{TEN_IMPORT_ERROR} Refusing to produce a score from a model that "
        f"computed nothing. Set {MOCK_ENV_VAR}=1 only for offline tests."
    )


def ten_status() -> Dict[str, Any]:
    """Machine-readable TEN binding status, for health checks and diagnostics."""
    return {
        "available": TEN_AVAILABLE,
        "expected_distribution": EXPECTED_DISTRIBUTION,
        "module": TEN_MODULE,
        "error": TEN_IMPORT_ERROR,
        "mock_allowed": mock_allowed(),
        "install_hint": INSTALL_HINT,
    }
