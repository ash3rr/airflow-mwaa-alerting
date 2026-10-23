#!/usr/bin/env python3
"""Manage maintenance windows for Airflow DAG alerts.

A window is an SSM parameter <prefix>/<dag_id> holding
{"start", "end", "reason", "set_by"}. While a window is active the silencer
Lambda drops failure alerts for that DAG instead of posting them to Slack.

Examples:
    python maintenance.py set example_hourly --for 3d --reason "Upstream API migration"
    python maintenance.py set example_hourly --start 2026-06-01T06:00Z \
        --until 2026-06-01T18:00Z --reason "Planned reload"
    python maintenance.py list
    python maintenance.py list --all
    python maintenance.py clear example_hourly
    python maintenance.py clear --expired

Uses the normal AWS credential chain (AWS_PROFILE, AWS_REGION and so on).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from silencer import core  # noqa: E402

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
DEFAULT_PREFIX = os.environ.get("MAINTENANCE_PREFIX", "/airflow/maintenance")
# Windows longer than this are refused unless --allow-long is passed. A
# silence that never ends is the same as deleting the alert.
MAX_WINDOW = timedelta(days=int(os.environ.get("MAINTENANCE_MAX_DAYS", "14")))


def _clients():
    try:
        import boto3
    except ImportError:
        sys.exit("boto3 is required: pip install boto3")
    return boto3.client("ssm"), boto3.client("sts")


def _list_windows(ssm, prefix: str) -> list[tuple[str, str]]:
    """Return (dag_id, raw_value) for every parameter under the prefix."""
    out = []
    paginator = ssm.get_paginator("get_parameters_by_path")
    for page in paginator.paginate(Path=prefix.rstrip("/"), Recursive=False):
        for param in page["Parameters"]:
            out.append((param["Name"].rsplit("/", 1)[-1], param["Value"]))
    return sorted(out)


def cmd_set(args) -> int:
    ssm, sts = _clients()
    now = datetime.now(timezone.utc)
    set_by = args.set_by or core.principal_name(sts.get_caller_identity()["Arn"])
    try:
        window = core.build_window(
            now=now,
            reason=args.reason,
            set_by=set_by,
            start=core.parse_utc(args.start) if args.start else None,
            end=core.parse_utc(args.until) if args.until else None,
            duration=core.parse_duration(args.duration) if args.duration else None,
            max_duration=None if args.allow_long else MAX_WINDOW,
        )
    except core.WindowError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    name = core.parameter_name(args.prefix, args.dag_id)
    ssm.put_parameter(
        Name=name,
        Value=window.to_json(),
        Type="String",
        Overwrite=True,
        Description=f"Alert maintenance window: {window.reason}"[:1024],
    )
    print(
        f"{args.dag_id}: silenced {core.format_utc(window.start)} to {core.format_utc(window.end)} "
        f"({core.humanise(window.end - window.start)}) by {window.set_by}: {window.reason}"
    )
    return 0


def cmd_list(args) -> int:
    ssm, _ = _clients()
    now = datetime.now(timezone.utc)
    rows = []
    for dag_id, raw in _list_windows(ssm, args.prefix):
        try:
            window = core.parse_window(raw)
        except core.WindowError as exc:
            rows.append((dag_id, "invalid", "", "", str(exc)))
            continue
        state = core.window_state(window, now)
        if state == core.STATE_EXPIRED and not args.all:
            continue
        remaining = core.humanise(window.end - now) if state != core.STATE_EXPIRED else "-"
        rows.append(
            (dag_id, state, core.format_utc(window.end), remaining, f"{window.reason} ({window.set_by})")
        )

    if not rows:
        print("No maintenance windows." if args.all else "No active or pending maintenance windows.")
        return 0
    headers = ("DAG_ID", "STATE", "ENDS_UTC", "LEFT", "REASON (SET_BY)")
    widths = [max(len(str(r[i])) for r in rows + [headers]) for i in range(4)]
    print("  ".join(h.ljust(widths[i]) if i < 4 else h for i, h in enumerate(headers)))
    for row in rows:
        print("  ".join(str(c).ljust(widths[i]) if i < 4 else str(c) for i, c in enumerate(row)))
    return 0


def cmd_clear(args) -> int:
    ssm, _ = _clients()
    if args.expired:
        now = datetime.now(timezone.utc)
        removed = 0
        for dag_id, raw in _list_windows(ssm, args.prefix):
            try:
                window = core.parse_window(raw)
            except core.WindowError:
                continue  # leave malformed entries for a human to look at
            if core.window_state(window, now) == core.STATE_EXPIRED:
                ssm.delete_parameter(Name=core.parameter_name(args.prefix, dag_id))
                print(f"{dag_id}: removed expired window")
                removed += 1
        print(f"{removed} expired window(s) removed")
        return 0

    if not args.dag_id:
        print("error: give a DAG id or --expired", file=sys.stderr)
        return 2
    try:
        ssm.delete_parameter(Name=core.parameter_name(args.prefix, args.dag_id))
    except ssm.exceptions.ParameterNotFound:
        print(f"error: no maintenance window for {args.dag_id}", file=sys.stderr)
        return 1
    print(f"{args.dag_id}: maintenance window cleared, alerts resume")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="SSM parameter prefix (default %(default)s)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="create or replace a window for one DAG")
    p_set.add_argument("dag_id")
    when = p_set.add_mutually_exclusive_group(required=True)
    when.add_argument("--for", dest="duration", help="duration such as 90m, 12h, 3d, 1d12h")
    when.add_argument("--until", help="end time, ISO 8601, UTC if no offset given")
    p_set.add_argument("--start", help="start time, ISO 8601, UTC if no offset given (default now)")
    p_set.add_argument("--reason", required=True, help="why the alerts are silenced, shown in list output")
    p_set.add_argument("--set-by", help="who set it (default: your AWS caller identity)")
    p_set.add_argument("--allow-long", action="store_true", help=f"allow windows longer than {MAX_WINDOW.days} days")
    p_set.set_defaults(func=cmd_set)

    p_list = sub.add_parser("list", help="show active and pending windows")
    p_list.add_argument("--all", action="store_true", help="include expired and invalid windows")
    p_list.set_defaults(func=cmd_list)

    p_clear = sub.add_parser("clear", help="remove a window so alerts resume")
    p_clear.add_argument("dag_id", nargs="?")
    p_clear.add_argument("--expired", action="store_true", help="remove every expired window")
    p_clear.set_defaults(func=cmd_clear)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
