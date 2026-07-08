"""
Privacy / PII-leakage screening for tabular datasets.

Answers the #1 synthetic-data buyer question — "does this leak real data?" —
with three CPU-only heuristic analyses:

  1. PII detection      — regex screening (email, phone, SSN, credit card with
                          Luhn check, IPv4, IBAN) plus column-name hints.
  2. Memorization risk  — exact-duplicate rate and DCR (distance to closest
                          record) statistics on standardized numeric columns;
                          near-clone rows in synthetic data are the classic
                          signature of a generator memorizing training rows.
  3. k-anonymity        — minimum equivalence-class size over low-cardinality
                          quasi-identifier candidates.

This is heuristic screening, NOT a formal differential-privacy audit; the
report says so explicitly. Everything is bounded (row/value sampling) and
deterministic (fixed random_state) so repeated runs agree.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PII patterns (precompiled). Deliberately conservative to limit false hits.
# ---------------------------------------------------------------------------
_PII_PATTERNS: Dict[str, re.Pattern] = {
    "email": re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
    "phone": re.compile(r"^\+?[0-9][0-9 ().-]{7,18}[0-9]$"),
    "ssn": re.compile(r"^\d{3}-\d{2}-\d{4}$"),
    "credit_card": re.compile(r"^(?:\d[ -]?){13,19}$"),
    "ipv4": re.compile(
        r"^(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)$"
    ),
    "iban": re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$"),
}

# Severity weight of each PII type when scoring (higher = worse to leak).
_PII_SEVERITY: Dict[str, int] = {
    "ssn": 30,
    "credit_card": 30,
    "iban": 25,
    "email": 15,
    "phone": 15,
    "ipv4": 8,
    "name_column": 8,
}

# Column-name hints that suggest direct identifiers even without regex hits.
_NAME_HINTS = (
    "first_name", "last_name", "full_name", "surname", "given_name",
    "date_of_birth", "birth_date", "dob", "address", "street", "zip_code",
    "postcode", "passport", "national_id", "tax_id", "email", "phone", "ssn",
)


def _luhn_ok(digits: str) -> bool:
    """Luhn checksum — filters out random digit strings from credit-card hits."""
    total, alt = 0, False
    for ch in reversed(digits):
        d = ord(ch) - 48
        if alt:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        alt = not alt
    return total % 10 == 0


class PrivacyAnalyzer:
    """Heuristic privacy screening over a (sampled) DataFrame."""

    def __init__(self, max_rows: int = 50_000, max_values_per_column: int = 5_000):
        self.max_rows = max_rows
        self.max_values_per_column = max_values_per_column

    # ------------------------------------------------------------------
    def analyze(self, df: pd.DataFrame, sampled: bool = False) -> Dict[str, Any]:
        """Run all analyses and return a JSON-serializable report."""
        if len(df) > self.max_rows:
            df = df.sample(n=self.max_rows, random_state=42)
            sampled = True

        report: Dict[str, Any] = {
            "version": "1.0",
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "rows_analyzed": int(len(df)),
            "columns_analyzed": int(len(df.columns)),
            "sampled": bool(sampled),
        }

        pii = self._detect_pii(df)
        memorization = self._memorization_risk(df)
        k_anon = self._k_anonymity(df)

        report["pii"] = pii
        report["memorization"] = memorization
        report["k_anonymity"] = k_anon

        # ---- overall score: 100 = safest ------------------------------
        risk = 0
        risk += min(60, sum(_PII_SEVERITY.get(c["type"], 10) for c in pii["columns"]))
        risk += min(25, int(round(memorization.get("risk_contribution", 0))))
        risk += min(15, int(round(k_anon.get("risk_contribution", 0))))
        risk = max(0, min(100, risk))

        score = 100 - risk
        level = "low"
        if score < 40:
            level = "high"
        elif score < 70:
            level = "moderate"

        report["privacy_score"] = int(score)
        report["risk_level"] = level
        report["disclaimer"] = (
            "Heuristic screening on a data sample; not a formal "
            "differential-privacy or re-identification audit."
        )
        return report

    # ------------------------------------------------------------------
    def _detect_pii(self, df: pd.DataFrame) -> Dict[str, Any]:
        findings: List[Dict[str, Any]] = []
        try:
            for col in df.columns:
                series = df[col]
                lowered = str(col).lower()

                # Column-name hint (works even for hashed/encoded content).
                if any(h in lowered for h in _NAME_HINTS):
                    findings.append({
                        "column": str(col),
                        "type": "name_column",
                        "match_rate": None,
                        "basis": "column name suggests a direct identifier",
                    })

                if series.dtype != object and not pd.api.types.is_string_dtype(series):
                    continue
                values = series.dropna().astype(str)
                if values.empty:
                    continue
                if len(values) > self.max_values_per_column:
                    values = values.sample(n=self.max_values_per_column, random_state=42)

                n = len(values)
                for ptype, pattern in _PII_PATTERNS.items():
                    matched = values.str.match(pattern)
                    hits = int(matched.sum())
                    if ptype == "credit_card" and hits:
                        # Confirm with Luhn to kill random-number false positives.
                        cand = values[matched].str.replace(r"[ -]", "", regex=True)
                        hits = int(sum(_luhn_ok(v) for v in cand if v.isdigit()))
                    rate = hits / n
                    if hits >= 3 and rate >= 0.01:
                        findings.append({
                            "column": str(col),
                            "type": ptype,
                            "match_rate": round(rate, 4),
                            "basis": f"{hits} of {n} sampled values match",
                        })
        except Exception as e:  # pragma: no cover — never break the pipeline
            logger.warning(f"PII detection failed: {e}")

        return {
            "detected": bool(findings),
            "columns": findings,
            "types_found": sorted({f["type"] for f in findings}),
        }

    # ------------------------------------------------------------------
    def _memorization_risk(self, df: pd.DataFrame) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "exact_duplicate_rate": None,
            "median_dcr": None,
            "clone_fraction": None,
            "risk_contribution": 0.0,
        }
        try:
            dup_rate = float(df.duplicated().mean())
            out["exact_duplicate_rate"] = round(dup_rate, 4)
            risk = min(15.0, dup_rate * 150)  # 10% duplicates -> full 15 points

            numeric = df.select_dtypes(include=[np.number]).dropna(axis=1, how="all")
            if numeric.shape[1] >= 2 and len(numeric) >= 100:
                sample = numeric.dropna()
                if len(sample) > 5_000:
                    sample = sample.sample(n=5_000, random_state=42)
                if len(sample) >= 100:
                    from sklearn.neighbors import NearestNeighbors
                    from sklearn.preprocessing import StandardScaler

                    x = StandardScaler().fit_transform(sample.values)
                    nn = NearestNeighbors(n_neighbors=2).fit(x)
                    dist, _ = nn.kneighbors(x)
                    dcr = dist[:, 1]  # distance to closest OTHER record
                    clone_fraction = float((dcr < 1e-6).mean())
                    out["median_dcr"] = round(float(np.median(dcr)), 6)
                    out["clone_fraction"] = round(clone_fraction, 4)
                    risk += min(10.0, clone_fraction * 100)
            out["risk_contribution"] = round(risk, 2)
        except Exception as e:  # pragma: no cover
            logger.warning(f"Memorization analysis failed: {e}")
        return out

    # ------------------------------------------------------------------
    def _k_anonymity(self, df: pd.DataFrame) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "quasi_identifiers": [],
            "k": None,
            "rows_in_small_groups_pct": None,
            "risk_contribution": 0.0,
        }
        try:
            qi: List[str] = []
            for col in df.columns:
                series = df[col]
                if series.dtype == object or pd.api.types.is_string_dtype(series) \
                        or pd.api.types.is_integer_dtype(series):
                    nunique = series.nunique(dropna=True)
                    if 2 <= nunique <= 20:
                        qi.append(str(col))
                if len(qi) >= 5:
                    break
            if not qi:
                return out

            groups = df.groupby(qi, dropna=False, observed=True).size()
            k = int(groups.min())
            small = float(groups[groups < 5].sum() / max(1, len(df)))
            out["quasi_identifiers"] = qi
            out["k"] = k
            out["rows_in_small_groups_pct"] = round(small * 100, 2)

            risk = 0.0
            if k < 2:
                risk += 10.0
            elif k < 5:
                risk += 5.0
            risk += min(5.0, small * 20)
            out["risk_contribution"] = round(risk, 2)
        except Exception as e:  # pragma: no cover
            logger.warning(f"k-anonymity analysis failed: {e}")
        return out
