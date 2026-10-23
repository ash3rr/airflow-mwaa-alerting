from datetime import datetime, timedelta, timezone

import pytest

from running_dag_alert import core

UTC = timezone.utc
NOW = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)


def run(dag_id, start="2026-05-10T11:00:00Z", state="running", **extra):
    data = {"dag_id": dag_id, "state": state, "start_date": start}
    data.update(extra)
    return data


# --- config -----------------------------------------------------------------


def test_parse_environments_pairs_and_bare_names():
    envs = core.parse_environments(" dev=analytics-dev , preprod=analytics-preprod,sandbox ")
    assert envs == [
        core.Environment("dev", "analytics-dev"),
        core.Environment("preprod", "analytics-preprod"),
        core.Environment("sandbox", "sandbox"),
    ]


@pytest.mark.parametrize("text", ["", " , ", "dev=", "=env", "dev=a,dev=b"])
def test_parse_environments_rejects(text):
    with pytest.raises(core.ConfigError):
        core.parse_environments(text)


@pytest.mark.parametrize("version,major", [("2.10.3", 2), ("2.11.0", 2), ("3.0.6", 3), ("3.2.1", 3)])
def test_airflow_major(version, major):
    assert core.airflow_major(version) == major


@pytest.mark.parametrize("version", ["1.10.12", "4.0.0", "latest", ""])
def test_airflow_major_rejects(version):
    with pytest.raises(core.ConfigError):
        core.airflow_major(version)


def test_rest_base_path():
    assert core.rest_base_path(2) == "/api/v1"
    assert core.rest_base_path(3) == "/api/v2"


def test_running_runs_request_shape():
    req = core.running_runs_request(offset=200, limit=100)
    assert req["Path"] == "/dags/~/dagRuns"
    assert req["Method"] == "GET"
    assert req["QueryParameters"] == {"state": "running", "limit": 100, "offset": 200, "order_by": "start_date"}
    # InvokeRestApi limits Path to 64 characters and takes it without the /api/vN prefix.
    assert len(req["Path"]) <= 64
    assert not req["Path"].startswith("/api/")


# --- pagination ---------------------------------------------------------------


def make_pages(all_runs, report_total=True):
    calls = []

    def fetch(offset, limit):
        calls.append((offset, limit))
        page = all_runs[offset : offset + limit]
        body = {"dag_runs": page}
        if report_total:
            body["total_entries"] = len(all_runs)
        return body

    return fetch, calls


def test_iter_dag_runs_single_page():
    runs = [run(f"d{i}") for i in range(3)]
    fetch, calls = make_pages(runs)
    assert list(core.iter_dag_runs(fetch, page_size=10)) == runs
    assert calls == [(0, 10)]


def test_iter_dag_runs_multiple_pages_exact_multiple():
    runs = [run(f"d{i}") for i in range(20)]
    fetch, calls = make_pages(runs)
    assert len(list(core.iter_dag_runs(fetch, page_size=10))) == 20
    # total_entries stops it after the second page, without an empty third call.
    assert calls == [(0, 10), (10, 10)]


def test_iter_dag_runs_without_total_stops_on_short_page():
    runs = [run(f"d{i}") for i in range(25)]
    fetch, calls = make_pages(runs, report_total=False)
    assert len(list(core.iter_dag_runs(fetch, page_size=10))) == 25
    assert calls == [(0, 10), (10, 10), (20, 10)]


def test_iter_dag_runs_server_caps_page_size():
    # Server returns at most 5 per page even though 10 were asked for.
    runs = [run(f"d{i}") for i in range(12)]

    def fetch(offset, limit):
        return {"dag_runs": runs[offset : offset + 5], "total_entries": len(runs)}

    assert len(list(core.iter_dag_runs(fetch, page_size=10))) == 12


def test_iter_dag_runs_empty():
    fetch, _ = make_pages([])
    assert list(core.iter_dag_runs(fetch)) == []


def test_iter_dag_runs_missing_field():
    with pytest.raises(core.ApiError):
        list(core.iter_dag_runs(lambda offset, limit: {"detail": "forbidden"}))


def test_iter_dag_runs_page_cap():
    def endless(offset, limit):
        return {"dag_runs": [run("d")] * limit}

    with pytest.raises(core.ApiError):
        list(core.iter_dag_runs(endless, page_size=2, max_pages=3))


# --- counting -----------------------------------------------------------------


def test_summarise_groups_sorts_and_ignores_other_states():
    runs = [
        run("b_dag", "2026-05-10T10:00:00Z"),
        run("a_dag", "2026-05-10T09:00:00Z"),
        run("c_dag", "2026-05-10T11:30:00Z"),
        run("c_dag", "2026-05-10T08:00:00+02:00"),  # 06:00 UTC
        run("queued_dag", state="queued"),
        run("done_dag", state="success"),
        {"state": "running"},  # no dag_id
    ]
    summaries = core.summarise(runs)
    assert [s.dag_id for s in summaries] == ["c_dag", "a_dag", "b_dag"]
    assert summaries[0].running_runs == 2
    assert summaries[0].oldest_start == datetime(2026, 5, 10, 6, 0, tzinfo=UTC)
    assert core.total_runs(summaries) == 4


def test_summarise_handles_missing_and_bad_start_dates():
    summaries = core.summarise([run("x", None), run("y", "not a date"), run("z", "2026-05-10T11:00:00")])
    by_id = {s.dag_id: s for s in summaries}
    assert by_id["x"].oldest_start is None
    assert by_id["y"].oldest_start is None
    assert by_id["z"].oldest_start == datetime(2026, 5, 10, 11, 0, tzinfo=UTC)
    assert summaries[0].dag_id == "z"  # known start sorts before unknown


@pytest.mark.parametrize(
    "n_runs,threshold,over",
    [(10, 10, False), (11, 10, True), (0, 0, False), (1, 0, True)],
)
def test_threshold_is_strictly_greater(n_runs, threshold, over):
    summaries = core.summarise([run(f"d{i}") for i in range(n_runs)])
    assert core.is_over_threshold(summaries, threshold) is over


def test_count_by_dags_vs_runs():
    # One DAG with 12 concurrent runs (max_active_runs > 1).
    summaries = core.summarise([run("backfilling_dag") for _ in range(12)])
    assert core.is_over_threshold(summaries, 10, "runs") is True
    assert core.is_over_threshold(summaries, 10, "dags") is False
    with pytest.raises(core.ConfigError):
        core.is_over_threshold(summaries, 10, "tasks")


@pytest.mark.parametrize(
    "over,last,expected",
    [
        (False, None, False),
        (True, None, True),
        (True, NOW - timedelta(minutes=59), False),
        (True, NOW - timedelta(minutes=60), True),
        (False, NOW - timedelta(days=1), False),
    ],
)
def test_should_alert_cooldown(over, last, expected):
    assert core.should_alert(over, last, NOW, timedelta(minutes=60)) is expected


def test_zero_cooldown_alerts_every_time():
    assert core.should_alert(True, NOW, NOW, timedelta(0)) is True


# --- formatting -----------------------------------------------------------------


def test_format_lines():
    summaries = core.summarise(
        [
            run("long_runner", "2026-05-08T09:30:00Z"),
            run("hourly", "2026-05-10T10:15:00Z"),
            run("hourly", "2026-05-10T11:15:00Z"),
            run("fresh", "2026-05-10T11:58:00Z"),
            run("unknown", None),
        ]
    )
    lines = core.format_lines(summaries, NOW)
    assert lines == [
        "hourly  (2 runs, oldest running 1h 45m)",
        "long_runner  (1 run, oldest running 2d 2h)",
        "fresh  (1 run, oldest running 2m)",
        "unknown  (1 run, oldest start unknown)",
    ]


def test_format_lines_truncates():
    summaries = core.summarise([run(f"dag_{i:02d}") for i in range(30)])
    lines = core.format_lines(summaries, NOW, limit=25)
    assert len(lines) == 26
    assert lines[-1] == "...and 5 more DAGs"


def test_format_alert_contents():
    summaries = core.summarise([run(f"dag_{i}") for i in range(11)] + [run("dag_0")])
    payload = core.format_alert("preprod", summaries, 10, NOW, "runs", "https://example.com/airflow")
    assert payload["text"] == "[preprod] 12 DAG runs running (threshold 10)"
    listing = payload["blocks"][1]["text"]["text"]
    assert listing.startswith("```dag_0  (2 runs")
    context = payload["blocks"][3]["elements"][0]["text"]
    assert "12 running runs across 11 DAGs at 2026-05-10 12:00 UTC" in context
    assert "<https://example.com/airflow|Airflow UI>" in context


def test_format_alert_count_by_dags():
    summaries = core.summarise([run("a"), run("a"), run("b")])
    payload = core.format_alert("dev", summaries, 1, NOW, "dags")
    assert payload["text"] == "[dev] 2 DAGs running (threshold 1)"


def test_build_emf():
    record = core.build_emf("Airflow/Alerting", "dev", runs=12, dags=11, timestamp_ms=1)
    names = [m["Name"] for m in record["_aws"]["CloudWatchMetrics"][0]["Metrics"]]
    assert names == ["RunningDagRuns", "RunningDags"]
    assert record["RunningDagRuns"] == 12
    assert record["RunningDags"] == 11
    assert record["Environment"] == "dev"
