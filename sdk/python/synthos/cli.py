"""Synthos CLI — validate datasets from a terminal or CI pipeline.

Exit codes (CI contract):
  0  success / gate passed
  1  quality gate failed (risk score above --max-risk)
  2  error (auth, network, validation job failed, bad arguments)
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from .client import QualityGateError, SynthosClient, SynthosError


def _client(args: argparse.Namespace) -> SynthosClient:
    return SynthosClient(api_key=args.api_key, base_url=args.base_url)


def _emit(data: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(data, indent=2, default=str))


def cmd_validate(args: argparse.Namespace) -> int:
    client = _client(args)
    try:
        result = client.validate_file(
            args.file,
            validation_type=args.type,
            max_risk=args.max_risk,
            timeout=args.timeout,
        )
    except QualityGateError as e:
        _emit(e.result, args.json)
        print(f"❌ GATE FAILED: risk score {e.risk_score} > allowed {e.max_risk}", file=sys.stderr)
        return 1
    _emit(result, args.json)
    risk = result.get("risk_score")
    level = result.get("risk_level", "unknown")
    print(f"✅ Validation complete: risk={risk} ({level}) id={result.get('validation_id')}")
    if args.max_risk is not None:
        print(f"   Gate passed (threshold {args.max_risk})")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    result = _client(args).get_validation(args.validation_id)
    _emit(result, args.json)
    print(f"{result.get('validation_id')}: {result.get('status')} "
          f"risk={result.get('risk_score')} ({result.get('risk_level')})")
    return 0


def cmd_rename(args: argparse.Namespace) -> int:
    result = _client(args).rename_validation(args.validation_id, args.name)
    _emit(result, args.json)
    print(f"Renamed {result.get('validation_id')} -> {result.get('name')!r}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    out = _client(args).get_report_pdf(args.validation_id, args.output)
    print(f"Report saved to {out}")
    return 0


def cmd_privacy(args: argparse.Namespace) -> int:
    result = _client(args).get_privacy(args.validation_id)
    _emit(result, True)  # privacy is detail-heavy; always print JSON
    p = result.get("privacy", {})
    print(f"privacy_score={p.get('privacy_score')} risk={p.get('risk_level')}", file=sys.stderr)
    return 0


def cmd_datasheet(args: argparse.Namespace) -> int:
    result = _client(args).get_datasheet(args.validation_id)
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_share(args: argparse.Namespace) -> int:
    result = _client(args).create_share(args.validation_id, expires_in_hours=args.hours)
    _emit(result, args.json)
    print(f"Share link (expires {result.get('expires_at')}):\n{result.get('share_url')}")
    return 0


def cmd_outcome(args: argparse.Namespace) -> int:
    result = _client(args).record_outcome(
        args.validation_id, args.outcome,
        actual_metric=args.metric, notes=args.notes or "")
    _emit(result, args.json)
    print("Outcome recorded — thank you, this improves calibration.")
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    result = _client(args).create_monitor(
        args.dataset_id, interval_hours=args.interval_hours,
        max_risk_score=args.max_risk, name=args.name or "")
    _emit(result, args.json)
    print(f"Monitor {result.get('monitor_id')} created "
          f"(every {result.get('interval_hours')}h, alert above risk {result.get('max_risk_score')})")
    return 0


def cmd_verify_cert(args: argparse.Namespace) -> int:
    with open(args.cert_file) as fh:
        bundle = json.load(fh)
    client = _client(args)
    ok = client.verify_certificate(bundle)
    if ok:
        print("✅ Certificate signature is VALID")
        return 0
    print("❌ Certificate signature is INVALID", file=sys.stderr)
    return 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="synthos",
        description="Synthos — dataset validation, collapse-risk scoring and privacy screening.",
    )
    p.add_argument("--api-key", default=None,
                   help="API key (sk_...); defaults to $SYNTHOS_API_KEY")
    p.add_argument("--base-url", default="https://api.synthos.dev/api/v1",
                   help="API base URL")
    p.add_argument("--json", action="store_true", help="print full JSON responses")

    sub = p.add_subparsers(dest="command", required=True)

    v = sub.add_parser("validate", help="upload a file or directory, run a validation, optionally gate on risk")
    v.add_argument("file", help="path to a data file, or a directory to validate as a dataset group")
    v.add_argument("--type", default="comprehensive",
                   choices=["comprehensive", "distribution", "correlation", "temporal", "full"])
    v.add_argument("--max-risk", type=int, default=None,
                   help="fail (exit 1) if risk score exceeds this — the CI gate")
    v.add_argument("--timeout", type=float, default=3600)
    v.set_defaults(func=cmd_validate)

    s = sub.add_parser("status", help="get a validation's status")
    s.add_argument("validation_id")
    s.set_defaults(func=cmd_status)

    rn = sub.add_parser("rename", help="set a validation's display name")
    rn.add_argument("validation_id")
    rn.add_argument("name")
    rn.set_defaults(func=cmd_rename)

    r = sub.add_parser("report", help="download the PDF report")
    r.add_argument("validation_id")
    r.add_argument("-o", "--output", default="synthos_report.pdf")
    r.set_defaults(func=cmd_report)

    pr = sub.add_parser("privacy", help="get the privacy / PII analysis")
    pr.add_argument("validation_id")
    pr.set_defaults(func=cmd_privacy)

    d = sub.add_parser("datasheet", help="get the compliance datasheet (model card)")
    d.add_argument("validation_id")
    d.set_defaults(func=cmd_datasheet)

    sh = sub.add_parser("share", help="create a read-only share link for a report")
    sh.add_argument("validation_id")
    sh.add_argument("--hours", type=int, default=168, help="expiry in hours (default 168 = 7 days)")
    sh.set_defaults(func=cmd_share)

    o = sub.add_parser("outcome", help="report the actual downstream outcome (calibration)")
    o.add_argument("validation_id")
    o.add_argument("outcome", choices=["healthy", "degraded", "collapsed"])
    o.add_argument("--metric", type=float, default=None)
    o.add_argument("--notes", default="")
    o.set_defaults(func=cmd_outcome)

    m = sub.add_parser("monitor", help="create a scheduled drift monitor for a dataset")
    m.add_argument("dataset_id")
    m.add_argument("--interval-hours", type=int, default=24)
    m.add_argument("--max-risk", type=int, default=50)
    m.add_argument("--name", default="")
    m.set_defaults(func=cmd_monitor)

    vc = sub.add_parser("verify-cert", help="verify a signed certificate JSON bundle")
    vc.add_argument("cert_file")
    vc.set_defaults(func=cmd_verify_cert)

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except SynthosError as e:
        print(f"synthos: error: {e}" + (f" [{e.code}]" if e.code else ""), file=sys.stderr)
        return 2
    except FileNotFoundError as e:
        print(f"synthos: error: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 2


if __name__ == "__main__":
    sys.exit(main())
