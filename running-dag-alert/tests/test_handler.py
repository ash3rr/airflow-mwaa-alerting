"""Handler wiring with a stubbed MWAA client. No AWS calls are made."""

from datetime import datetime, timezone

import pytest

pytest.importorskip("boto3")

from running_dag_alert import core, handler  # noqa: E402

NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


class FakeMwaa:
    def __init__(self, runs, version="2.10.3"):
        self.runs = runs
        self.version = version
        self.requests = []

    def get_environment(self, Name):
        return {"Environment": {"AirflowVersion": self.version, "WebserverUrl": "example.com"}}

    def invoke_rest_api(self, Name, Path, Method, QueryParameters):
        self.requests.append((Path, QueryParameters))
        offset, limit = QueryParameters["offset"], QueryParameters["limit"]
        page = self.runs[offset : offset + limit]
        return {"RestApiStatusCode": 200, "RestApiResponse": {"dag_runs": page, "total_entries": len(self.runs)}}


@pytest.fixture
def wired(monkeypatch):
    posted, recorded = [], []
    monkeypatch.setattr(handler, "post_to_slack", posted.append)
    monkeypatch.setattr(handler, "record_alert", lambda label, when: recorded.append(label))
    monkeypatch.setattr(handler, "last_alert_at", lambda label: None)
    monkeypatch.setattr(handler, "PAGE_SIZE", 5)
    monkeypatch.setattr(handler, "THRESHOLD", 10)
    return posted, recorded


@pytest.mark.parametrize("version", ["2.10.3", "3.2.1"])
def test_alerts_above_threshold_on_both_major_versions(monkeypatch, wired, capsys, version):
    posted, recorded = wired
    runs = [{"dag_id": f"d{i}", "state": "running", "start_date": "2026-05-10T11:00:00Z"} for i in range(12)]
    fake = FakeMwaa(runs, version)
    monkeypatch.setattr(handler, "_mwaa", fake)

    result = handler.check_environment(core.Environment("dev", "analytics-dev"), NOW)

    assert result == {"runs": 12, "dags": 12, "over_threshold": True, "alerted": True}
    assert len(posted) == 1 and recorded == ["dev"]
    # Same version-free path for Airflow 2 and 3, paged 5 at a time.
    assert [r[0] for r in fake.requests] == ["/dags/~/dagRuns"] * 3
    assert '"RunningDagRuns": 12' in capsys.readouterr().out


def test_quiet_below_threshold(monkeypatch, wired):
    posted, _ = wired
    monkeypatch.setattr(handler, "_mwaa", FakeMwaa([{"dag_id": "a", "state": "running"}]))
    result = handler.check_environment(core.Environment("dev", "analytics-dev"), NOW)
    assert result["alerted"] is False and posted == []


def test_non_200_from_airflow_raises(monkeypatch, wired):
    class Forbidden(FakeMwaa):
        def invoke_rest_api(self, **kwargs):
            return {"RestApiStatusCode": 403, "RestApiResponse": {"detail": "forbidden"}}

    monkeypatch.setattr(handler, "_mwaa", Forbidden([]))
    with pytest.raises(core.ApiError):
        handler.check_environment(core.Environment("dev", "analytics-dev"), NOW)


def test_one_environment_failing_does_not_hide_the_other(monkeypatch, wired):
    posted, _ = wired
    monkeypatch.setattr(handler, "ENVIRONMENTS", "dev=broken,preprod=ok")
    calls = []

    def fake_check(env, now):
        calls.append(env.label)
        if env.label == "dev":
            raise core.ApiError("boom")
        return {"runs": 0}

    monkeypatch.setattr(handler, "check_environment", fake_check)
    with pytest.raises(RuntimeError, match="dev"):
        handler.lambda_handler({}, None)
    assert calls == ["dev", "preprod"]
