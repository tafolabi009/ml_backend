"""Synthos API client.

Authentication: an API key (``sk_...``, created in the dashboard under
Settings → API Keys) or an email/password pair. Both are sent as
``Authorization: Bearer <token>``.

Typical use — validate a file and gate on risk::

    from synthos import SynthosClient

    client = SynthosClient(api_key="sk_...")
    result = client.validate_file("training_data.csv", max_risk=50)
    print(result["risk_score"], result["risk_level"])
"""

from __future__ import annotations

import json
import mimetypes
import os
import time
import uuid
from typing import Any, Dict, Optional

import requests

DEFAULT_BASE_URL = "https://api.synthos.dev/api/v1"


class SynthosError(RuntimeError):
    """API returned an error response."""

    def __init__(self, message: str, code: str = "", status: int = 0):
        super().__init__(message)
        self.code = code
        self.status = status


class ValidationFailedError(SynthosError):
    """The validation job itself ended in a failed/cancelled state."""


class QualityGateError(SynthosError):
    """The validation completed but the risk score breached the gate."""

    def __init__(self, message: str, risk_score: int, max_risk: int, result: Dict[str, Any]):
        super().__init__(message, code="QUALITY_GATE_FAILED")
        self.risk_score = risk_score
        self.max_risk = max_risk
        self.result = result


class SynthosClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._token: Optional[str] = None

        api_key = api_key or os.environ.get("SYNTHOS_API_KEY")
        if api_key:
            self._token = api_key
        elif email and password:
            self._login(email, password)
        elif os.environ.get("SYNTHOS_EMAIL") and os.environ.get("SYNTHOS_PASSWORD"):
            self._login(os.environ["SYNTHOS_EMAIL"], os.environ["SYNTHOS_PASSWORD"])
        else:
            raise SynthosError(
                "No credentials: pass api_key= (or set SYNTHOS_API_KEY), "
                "or provide email= and password=."
            )

    # ------------------------------------------------------------------
    # HTTP plumbing
    # ------------------------------------------------------------------
    def _login(self, email: str, password: str) -> None:
        resp = self._session.post(
            f"{self.base_url}/auth/login",
            json={"email": email, "password": password},
            timeout=self.timeout,
        )
        data = self._parse(resp)
        token = data.get("access_token") or data.get("data", {}).get("access_token")
        if not token:
            raise SynthosError("Login succeeded but no access_token in response")
        self._token = token

    @staticmethod
    def _parse(resp: requests.Response) -> Dict[str, Any]:
        try:
            data = resp.json()
        except ValueError:
            data = {}
        if resp.status_code >= 400:
            err = data.get("error") or {}
            raise SynthosError(
                err.get("message") or f"HTTP {resp.status_code}",
                code=err.get("code", ""),
                status=resp.status_code,
            )
        return data

    def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        headers = kwargs.pop("headers", {})
        headers.setdefault("Authorization", f"Bearer {self._token}")
        resp = self._session.request(
            method,
            f"{self.base_url}{path}",
            headers=headers,
            timeout=kwargs.pop("timeout", self.timeout),
            **kwargs,
        )
        return self._parse(resp)

    def _get(self, path: str, **kw) -> Dict[str, Any]:
        return self._request("GET", path, **kw)

    def _post(self, path: str, body: Optional[dict] = None, **kw) -> Dict[str, Any]:
        return self._request("POST", path, json=body or {}, **kw)

    # ------------------------------------------------------------------
    # Datasets
    # ------------------------------------------------------------------
    def upload_dataset(self, path: str, description: str = "", wait_processed: bool = True,
                       timeout: float = 600.0, group_name: str = "") -> str:
        """Upload a local file and return its dataset_id. When group_name is
        set, the file is attached to that dataset group (created on demand)."""
        filename = os.path.basename(path)
        file_size = os.path.getsize(path)
        file_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        payload = {
            "filename": filename,
            "file_size": file_size,
            "file_type": file_type,
            "description": description,
        }
        if group_name:
            payload["group_name"] = group_name
        init = self._post("/datasets/upload", payload)
        dataset_id = init["dataset_id"]
        upload_url = init["upload_url"]

        with open(path, "rb") as fh:
            put = requests.put(upload_url, data=fh,
                               headers={"Content-Type": file_type}, timeout=timeout)
        if put.status_code >= 300:
            raise SynthosError(f"Upload to storage failed: HTTP {put.status_code}")

        etag = (put.headers.get("ETag") or "").strip('"') or "uploaded"
        self._post(f"/datasets/{dataset_id}/complete", {"etag": etag})

        if wait_processed:
            deadline = time.time() + timeout
            while time.time() < deadline:
                ds = self._get(f"/datasets/{dataset_id}")
                status = ds.get("status") or ds.get("dataset", {}).get("status")
                if status in ("processed", "ready"):
                    break
                if status in ("failed", "error"):
                    raise SynthosError(f"Dataset processing failed (status={status})")
                time.sleep(3)
        return dataset_id

    # ------------------------------------------------------------------
    # Validations
    # ------------------------------------------------------------------
    def create_validation(self, dataset_id: str, validation_type: str = "comprehensive",
                          priority: str = "standard",
                          idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        headers = {"Idempotency-Key": idempotency_key or str(uuid.uuid4())}
        return self._post("/validations/create", {
            "dataset_id": dataset_id,
            "validation_type": validation_type,
            "options": {"priority": priority},
        }, headers=headers)

    def get_validation(self, validation_id: str) -> Dict[str, Any]:
        return self._get(f"/validations/{validation_id}")

    def wait_for_validation(self, validation_id: str, timeout: float = 3600.0,
                            poll_interval: float = 10.0) -> Dict[str, Any]:
        """Block until the validation completes; returns the final record."""
        deadline = time.time() + timeout
        while True:
            v = self.get_validation(validation_id)
            status = v.get("status")
            if status == "completed":
                return v
            if status in ("failed", "cancelled"):
                raise ValidationFailedError(
                    f"Validation {validation_id} ended with status={status}", status=0)
            if time.time() > deadline:
                raise SynthosError(f"Timed out waiting for validation {validation_id}")
            time.sleep(poll_interval)

    def cancel_validation(self, validation_id: str) -> Dict[str, Any]:
        return self._post(f"/validations/{validation_id}/cancel")

    def rename_validation(self, validation_id: str, name: str) -> Dict[str, Any]:
        """Set a custom display name (1-120 chars). Propagates to reports and
        certificates. Uses PATCH /validations/{id}."""
        return self._request("PATCH", f"/validations/{validation_id}", json={"name": name})

    def list_validations(self, page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        return self._get("/validations", params={"page": page, "per_page": per_page})

    def get_findings(self, validation_id: str, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        return self._get(f"/validations/{validation_id}/findings",
                         params={"page": page, "page_size": page_size})

    # ------------------------------------------------------------------
    # Results & artifacts
    # ------------------------------------------------------------------
    def get_report_pdf(self, validation_id: str, out_path: str) -> str:
        resp = self._session.get(
            f"{self.base_url}/validations/{validation_id}/report",
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=max(self.timeout, 120),
        )
        if resp.status_code >= 400:
            self._parse(resp)
        with open(out_path, "wb") as fh:
            fh.write(resp.content)
        return out_path

    def get_privacy(self, validation_id: str) -> Dict[str, Any]:
        return self._get(f"/validations/{validation_id}/privacy")

    def get_datasheet(self, validation_id: str) -> Dict[str, Any]:
        return self._get(f"/validations/{validation_id}/datasheet")

    def get_signed_certificate(self, validation_id: str) -> Dict[str, Any]:
        return self._get(f"/validations/{validation_id}/certificate.json")

    def get_history(self, dataset_id: str) -> Dict[str, Any]:
        return self._get(f"/datasets/{dataset_id}/history")

    def record_outcome(self, validation_id: str, outcome: str,
                       actual_metric: Optional[float] = None, notes: str = "") -> Dict[str, Any]:
        """Report what actually happened downstream ('healthy'|'degraded'|'collapsed')."""
        return self._post(f"/validations/{validation_id}/outcome", {
            "outcome": outcome, "actual_metric": actual_metric, "notes": notes,
        })

    def create_share(self, validation_id: str, expires_in_hours: int = 168) -> Dict[str, Any]:
        return self._post(f"/validations/{validation_id}/share",
                          {"expires_in_hours": expires_in_hours})

    # ------------------------------------------------------------------
    # Monitors
    # ------------------------------------------------------------------
    def create_monitor(self, dataset_id: str, interval_hours: int = 24,
                       max_risk_score: int = 50, name: str = "",
                       validation_type: str = "comprehensive") -> Dict[str, Any]:
        return self._post("/monitors", {
            "dataset_id": dataset_id, "interval_hours": interval_hours,
            "max_risk_score": max_risk_score, "name": name,
            "validation_type": validation_type,
        })

    def list_monitors(self) -> Dict[str, Any]:
        return self._get("/monitors")

    # ------------------------------------------------------------------
    # Certificates
    # ------------------------------------------------------------------
    def verify_certificate(self, bundle: Dict[str, Any], offline: bool = True) -> bool:
        """Verify a signed certificate bundle (as returned by
        get_signed_certificate, or saved to a file). Tries offline Ed25519
        verification when the 'cryptography' package is installed; otherwise
        falls back to the public API endpoint."""
        cert = bundle.get("certificate")
        signature_b64 = bundle.get("signature", "")
        if not cert or not signature_b64:
            raise SynthosError("Bundle must contain 'certificate' and 'signature'")

        if offline:
            try:
                import base64
                from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

                payload = json.dumps(cert, sort_keys=True, separators=(",", ":")).encode()
                pub = Ed25519PublicKey.from_public_bytes(
                    base64.b64decode(bundle["public_key"]))
                pub.verify(base64.b64decode(signature_b64), payload)
                return True
            except ImportError:
                pass  # cryptography not installed → API fallback
            except Exception:
                return False

        resp = requests.post(
            f"{self.base_url}/certificates/verify",
            json={"certificate": cert, "signature": signature_b64},
            timeout=self.timeout,
        )
        return bool(self._parse(resp).get("valid"))

    def create_group_validation(self, group_id: str, validation_type: str = "comprehensive",
                                priority: str = "standard") -> Dict[str, Any]:
        """Validate a whole dataset group as one logical dataset."""
        headers = {"Idempotency-Key": str(uuid.uuid4())}
        return self._post("/validations/create", {
            "group_id": group_id,
            "validation_type": validation_type,
            "options": {"priority": priority},
        }, headers=headers)

    # ------------------------------------------------------------------
    # One-call quality gate
    # ------------------------------------------------------------------
    def validate_file(self, path: str, validation_type: str = "comprehensive",
                      max_risk: Optional[int] = None, timeout: float = 3600.0,
                      description: str = "") -> Dict[str, Any]:
        """Upload → validate → wait → (optionally) gate.

        `path` may be a file or a directory. A directory uploads every
        supported file into a dataset group (named after the folder) and
        validates the group as one logical dataset.

        Returns the completed validation record. Raises QualityGateError when
        max_risk is set and the risk score exceeds it — the CLI maps that to
        a non-zero exit code, which is what fails a CI build.
        """
        if os.path.isdir(path):
            created = self._validate_directory(path, validation_type, timeout)
        else:
            dataset_id = self.upload_dataset(path, description=description, timeout=timeout)
            created = self.create_validation(dataset_id, validation_type=validation_type)

        result = self.wait_for_validation(created["validation_id"], timeout=timeout)
        risk = result.get("risk_score")
        if max_risk is not None and isinstance(risk, int) and risk > max_risk:
            raise QualityGateError(
                f"Risk score {risk} exceeds gate ({max_risk})",
                risk_score=risk, max_risk=max_risk, result=result)
        return result

    _SUPPORTED_EXT = {".csv", ".tsv", ".json", ".jsonl", ".ndjson", ".parquet",
                      ".h5", ".hdf5", ".arrow", ".feather", ".xlsx", ".xls", ".txt"}

    def _validate_directory(self, path: str, validation_type: str, timeout: float) -> Dict[str, Any]:
        group_name = os.path.basename(os.path.normpath(path)) or "dataset-group"
        files = sorted(
            os.path.join(path, f) for f in os.listdir(path)
            if os.path.isfile(os.path.join(path, f))
            and os.path.splitext(f)[1].lower() in self._SUPPORTED_EXT
        )
        if not files:
            raise SynthosError(f"No supported data files found in directory {path!r}")

        group_id = None
        for fp in files:
            self.upload_dataset(fp, timeout=timeout, group_name=group_name)
            # The group id comes back on the first upload response; fetch it once.
            if group_id is None:
                for g in self._get("/dataset-groups").get("groups", []):
                    if g.get("name") == group_name:
                        group_id = g["id"]
                        break
        if group_id is None:
            raise SynthosError("Uploaded files but could not resolve the dataset group")
        return self.create_group_validation(group_id, validation_type=validation_type)
