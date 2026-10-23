"""Handler wiring tests with the AWS clients and Slack call replaced by stubs."""

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("boto3")

from silencer import core, handler  # noqa: E402

NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


def sns_event(message, subject="Airflow task failed"):
    return {"Sns": {"Message": message, "Subject": subject}}


@pytest.fixture
def stubs(monkeypatch):
    state = {"windows": {}, "posted": [], "deleted": []}

    def read_window(dag_id):
        raw = state["windows"].get(dag_id)
        return (raw, 1) if raw else (None, None)

    monkeypatch.setattr(handler, "read_window", read_window)
    monkeypatch.setattr(handler, "post_to_slack", lambda payload: state["posted"].append(payload))
    monkeypatch.setattr(handler, "delete_window_if_unchanged", lambda d, v: state["deleted"].append(d))
    return state


def test_suppresses_and_emits_metric(stubs, capsys):
    stubs["windows"]["sales_daily"] = json.dumps(
        {"start": "2026-05-10T00:00:00Z", "end": "2026-05-11T00:00:00Z", "reason": "migration", "set_by": "me"}
    )
    action = handler.handle_record(sns_event(json.dumps({"dag_id": "sales_daily"})), NOW)
    assert action == core.ACTION_SUPPRESS
    assert stubs["posted"] == []
    emf = json.loads(capsys.readouterr().out.strip())
    assert emf["SuppressedAlerts"] == 1
    assert emf["DagId"] == "sales_daily"
    assert emf["reason"] == "migration"


def test_forwards_when_no_window(stubs):
    action = handler.handle_record(sns_event("DAG ID: orders_hourly"), NOW)
    assert action == core.ACTION_FORWARD
    assert len(stubs["posted"]) == 1


def test_expired_window_forwarded_and_cleaned_up_when_enabled(stubs, monkeypatch):
    monkeypatch.setattr(handler, "DELETE_EXPIRED", True)
    stubs["windows"]["sales_daily"] = json.dumps(
        {"start": "2026-05-01T00:00:00Z", "end": "2026-05-02T00:00:00Z", "reason": "old", "set_by": "me"}
    )
    action = handler.handle_record(sns_event(json.dumps({"dag_id": "sales_daily"})), NOW)
    assert action == core.ACTION_FORWARD
    assert stubs["deleted"] == ["sales_daily"]


def test_expired_window_kept_when_cleanup_disabled(stubs, monkeypatch):
    monkeypatch.setattr(handler, "DELETE_EXPIRED", False)
    stubs["windows"]["sales_daily"] = json.dumps(
        {"start": "2026-05-01T00:00:00Z", "end": "2026-05-02T00:00:00Z", "reason": "old", "set_by": "me"}
    )
    handler.handle_record(sns_event(json.dumps({"dag_id": "sales_daily"})), NOW)
    assert stubs["deleted"] == []


def test_lookup_error_fails_open(stubs, monkeypatch):
    from botocore.exceptions import ClientError

    def broken(dag_id):
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetParameter")

    monkeypatch.setattr(handler, "read_window", broken)
    action = handler.handle_record(sns_event(json.dumps({"dag_id": "sales_daily"})), NOW)
    assert action == core.ACTION_FORWARD
    assert "lookup failed" in json.dumps(stubs["posted"][0])
