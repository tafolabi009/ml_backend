"""Synthos SDK — dataset validation, collapse-risk scoring, privacy screening."""

from .client import SynthosClient, SynthosError, ValidationFailedError, QualityGateError

__version__ = "0.1.0"
__all__ = [
    "SynthosClient",
    "SynthosError",
    "ValidationFailedError",
    "QualityGateError",
    "__version__",
]
