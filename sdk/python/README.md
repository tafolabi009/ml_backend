# Synthos Python SDK & CLI

Validate datasets, score model-collapse risk, screen for PII leakage, and
gate CI pipelines on data quality.

Quickstart:

```python
from synthos import SynthosClient
client = SynthosClient(api_key="sk_...")          # or SYNTHOS_API_KEY
result = client.validate_file("data/", max_risk=50)   # dir -> dataset group
client.rename_validation(result["validation_id"], "Q3 training set")
print(result["risk_score"], result["risk_level"])
```

CLI:

```bash
synthos validate ./data --max-risk 50   # file or directory (dir = one group)
synthos rename val_abc123 "Q3 training set"
```

## Install

```bash
pip install ./sdk/python              # from this repo
pip install "./sdk/python[crypto]"    # + offline certificate verification
```

## Authentication

Create an API key in the dashboard (Settings → API Keys), then:

```bash
export SYNTHOS_API_KEY=sk_...
```

## CLI

```bash
# Validate a file and fail (exit 1) if risk > 50 — the CI quality gate
synthos validate training_data.csv --max-risk 50

# Check status / fetch artifacts
synthos status val_a1b2c3d4
synthos report val_a1b2c3d4 -o report.pdf
synthos privacy val_a1b2c3d4
synthos datasheet val_a1b2c3d4 > datasheet.json

# Share a report with someone who has no Synthos account (7-day link)
synthos share val_a1b2c3d4 --hours 168

# Continuous drift monitoring (re-validates every 24h, alerts above risk 50)
synthos monitor ds_e5f6a7b8 --interval-hours 24 --max-risk 50

# Close the calibration loop: tell Synthos what actually happened downstream
synthos outcome val_a1b2c3d4 healthy --metric 0.93

# Verify a signed certificate (offline with [crypto], else via API)
synthos verify-cert certificate.json
```

Exit codes: `0` pass · `1` quality gate failed · `2` error.

## Python API

```python
from synthos import SynthosClient, QualityGateError

client = SynthosClient()  # reads SYNTHOS_API_KEY

try:
    result = client.validate_file("training_data.csv", max_risk=50)
    print(result["risk_score"], result["risk_level"])
except QualityGateError as e:
    print(f"Blocked: risk {e.risk_score} > {e.max_risk}")
    raise
```

## GitHub Actions quality gate

```yaml
- uses: tafolabi009/synthos-ml-backend/.github/actions/synthos-gate@main
  with:
    api-key: ${{ secrets.SYNTHOS_API_KEY }}
    file: data/training_data.csv
    max-risk: "50"
```

## Certificate verification (offline)

Signed certificates use Ed25519 over the canonical JSON of the
`certificate` object (compact separators, sorted keys):

```python
import base64, json
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

bundle = json.load(open("certificate.json"))
payload = json.dumps(bundle["certificate"], sort_keys=True, separators=(",", ":")).encode()
Ed25519PublicKey.from_public_bytes(base64.b64decode(bundle["public_key"])) \
    .verify(base64.b64decode(bundle["signature"]), payload)  # raises if tampered
```
