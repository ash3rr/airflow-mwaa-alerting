import json
from datetime import datetime, timedelta, timezone

import pytest

from silencer import core

UTC = timezone.utc
NOW = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)


def window_json(start="2026-05-10T00:00:00Z", end="2026-05-12T00:00:00Z", **extra):
    data = {"start": start, "end": end, "reason": "upstream migration", "set_by": "ops-user"}
    data.update(extra)
    return json.dumps(data)


# --- time parsing ---------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "2026-05-10T12:00:00Z",
        "2026-05-10T12:00:00z",
        "2026-05-10T12:00:00+00:00",
        "2026-05-10T15:00:00+03:00",
        "2026-05-10T07:00:00-05:00",
        "2026-05-10T12:00:00",  # no offset means UTC
        " 2026-05-10T12:00:00Z ",
    ],
)
def test_parse_utc_normalises_to_utc(text):
    assert core.parse_utc(text) == NOW
    assert core.parse_utc(text).tzinfo == UTC


@pytest.mark.parametrize("bad", ["", "   ", "tomorrow", "2026-13-01T00:00:00Z", None, 1715342400])
def test_parse_utc_rejects_garbage(bad):
    with pytest.raises(core.WindowError):
        core.parse_utc(bad)


def test_format_utc_round_trip_and_drops_microseconds():
    value = datetime(2026, 5, 10, 15, 0, 0, 123456, tzinfo=timezone(timedelta(hours=3)))
    assert core.format_utc(value) == "2026-05-10T12:00:00Z"
    assert core.parse_utc(core.format_utc(value)) == NOW


def test_format_utc_refuses_naive():
    with pytest.raises(ValueError):
        core.format_utc(datetime(2026, 5, 10, 12, 0))


@pytest.mark.parametrize(
    "text,expected",
    [
        ("90m", timedelta(minutes=90)),
        ("12h", timedelta(hours=12)),
        ("3d", timedelta(days=3)),
        ("1d12h", timedelta(days=1, hours=12)),
        ("1d2h30m", timedelta(days=1, hours=2, minutes=30)),
    ],
)
def test_parse_duration(text, expected):
    assert core.parse_duration(text) == expected


@pytest.mark.parametrize("bad", ["", "0m", "3 days", "h", "1w", "-2h"])
def test_parse_duration_rejects(bad):
    with pytest.raises(core.WindowError):
        core.parse_duration(bad)


@pytest.mark.parametrize(
    "delta,expected",
    [
        (timedelta(seconds=30), "<1m"),
        (timedelta(minutes=45), "45m"),
        (timedelta(hours=2, minutes=5), "2h 5m"),
        (timedelta(days=2, hours=3, minutes=10), "2d 3h"),
    ],
)
def test_humanise(delta, expected):
    assert core.humanise(delta) == expected


# --- window parsing and state ----------------------------------------------


def test_parse_window_valid():
    window = core.parse_window(window_json())
    assert window.start == datetime(2026, 5, 10, tzinfo=UTC)
    assert window.end == datetime(2026, 5, 12, tzinfo=UTC)
    assert window.reason == "upstream migration"
    assert window.set_by == "ops-user"


def test_parse_window_with_offsets_is_compared_in_utc():
    # 23:00 on the 9th in UTC-5 is 04:00 on the 10th in UTC.
    window = core.parse_window(window_json(start="2026-05-09T23:00:00-05:00", end="2026-05-10T14:00:00+02:00"))
    assert window.start == datetime(2026, 5, 10, 4, 0, tzinfo=UTC)
    assert window.end == datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    # The window ends exactly at NOW, so it is already expired.
    assert core.window_state(window, NOW) == core.STATE_EXPIRED


@pytest.mark.parametrize(
    "raw,message",
    [
        ("not json", "not valid JSON"),
        ("[1, 2]", "JSON object"),
        (json.dumps({"start": "2026-05-10T00:00:00Z", "end": "2026-05-11T00:00:00Z"}), "reason"),
        (window_json(reason=""), "reason"),
        (window_json(set_by="  "), "set_by"),
        (window_json(end="2026-05-09T00:00:00Z"), "end must be after start"),
        (window_json(start="soon"), "ISO 8601"),
    ],
)
def test_parse_window_rejects(raw, message):
    with pytest.raises(core.WindowError) as exc:
        core.parse_window(raw)
    assert message in str(exc.value)


@pytest.mark.parametrize(
    "now,state",
    [
        (datetime(2026, 5, 9, 23, 59, 59, tzinfo=UTC), core.STATE_PENDING),
        (datetime(2026, 5, 10, 0, 0, 0, tzinfo=UTC), core.STATE_ACTIVE),  # start is inclusive
        (datetime(2026, 5, 11, 23, 59, 59, tzinfo=UTC), core.STATE_ACTIVE),
        (datetime(2026, 5, 12, 0, 0, 0, tzinfo=UTC), core.STATE_EXPIRED),  # end is exclusive
        (datetime(2026, 5, 12, 2, 59, 59, tzinfo=timezone(timedelta(hours=3))), core.STATE_ACTIVE),
    ],
)
def test_window_state_boundaries(now, state):
    assert core.window_state(core.parse_window(window_json()), now) == state


def test_window_state_requires_aware_now():
    with pytest.raises(ValueError):
        core.window_state(core.parse_window(window_json()), datetime(2026, 5, 10, 12, 0))


def test_window_to_json_round_trip():
    window = core.parse_window(window_json(start="2026-05-10T03:00:00+03:00"))
    again = core.parse_window(window.to_json())
    assert again == window
    assert json.loads(window.to_json())["start"] == "2026-05-10T00:00:00Z"


# --- build_window (CLI) -----------------------------------------------------


def test_build_window_with_duration_defaults_start_to_now():
    window = core.build_window(NOW, "reload", "me", duration=timedelta(hours=6))
    assert window.start == NOW
    assert window.end == NOW + timedelta(hours=6)


def test_build_window_future_start_with_end():
    start = NOW + timedelta(days=1)
    window = core.build_window(NOW, "planned", "me", start=start, end=start + timedelta(hours=2))
    assert core.window_state(window, NOW) == core.STATE_PENDING


@pytest.mark.parametrize(
    "kwargs,message",
    [
        ({}, "exactly one"),
        ({"duration": timedelta(hours=1), "end": NOW + timedelta(hours=1)}, "exactly one"),
        ({"duration": timedelta(hours=1), "reason": " "}, "reason"),
        ({"end": NOW - timedelta(minutes=1), "start": NOW - timedelta(hours=1)}, "already be expired"),
        ({"duration": timedelta(days=30), "max_duration": timedelta(days=14)}, "exceeds the maximum"),
    ],
)
def test_build_window_rejects(kwargs, message):
    args = {"reason": "r", "set_by": "me"}
    args.update(kwargs)
    with pytest.raises(core.WindowError) as exc:
        core.build_window(NOW, **args)
    assert message in str(exc.value)


# --- DAG id extraction ------------------------------------------------------


@pytest.mark.parametrize(
    "message,subject,expected",
    [
        (json.dumps({"dag_id": "sales_daily", "task_id": "load"}), None, "sales_daily"),
        (json.dumps({"dagId": "sales.daily-v2"}), None, "sales.daily-v2"),
        (json.dumps({"detail": {"dag_id": "nested_dag"}}), None, "nested_dag"),
        (json.dumps({"context": {"dag": "ctx_dag"}}), None, "ctx_dag"),
        ("Task failed.\nDAG ID: orders_hourly\nTask ID: wait", None, "orders_hourly"),
        ("dag_id=orders_hourly, task_id=wait", None, "orders_hourly"),
        ("Task instance failed: <DagRun orders_hourly @ 2026-05-10 ...>", None, "orders_hourly"),
        ("free text with no id", "Airflow alert DAG: subject_dag failed", "subject_dag"),
        ("free text with no id", "something else", None),
        ("", None, None),
        (json.dumps({"dag_id": "has space"}), None, None),
        (json.dumps({"dag_id": 42}), None, None),
    ],
)
def test_extract_dag_id(message, subject, expected):
    assert core.extract_dag_id(message, subject) == expected


def test_extract_dag_id_prefers_json_over_text():
    message = json.dumps({"dag_id": "json_dag", "text": "DAG ID: text_dag"})
    assert core.extract_dag_id(message, "DAG: subject_dag") == "json_dag"


def test_parameter_name():
    assert core.parameter_name("/airflow/maintenance", "a_dag") == "/airflow/maintenance/a_dag"
    assert core.parameter_name("/airflow/maintenance/", "a_dag") == "/airflow/maintenance/a_dag"
    with pytest.raises(ValueError):
        core.parameter_name("/airflow/maintenance", "../other")


@pytest.mark.parametrize(
    "arn,expected",
    [
        ("arn:aws:sts::<ACCOUNT_ID>:assumed-role/PlatformAdmin/oncall-user", "PlatformAdmin/oncall-user"),
        ("arn:aws:iam::<ACCOUNT_ID>:user/ci-bot", "ci-bot"),
        ("arn:aws:iam::<ACCOUNT_ID>:root", "arn:aws:iam::<ACCOUNT_ID>:root"),
    ],
)
def test_principal_name(arn, expected):
    assert core.principal_name(arn) == expected


# --- decide ---------------------------------------------------------------


def test_decide_suppresses_inside_window():
    decision = core.decide("sales_daily", window_json(), NOW)
    assert decision.action == core.ACTION_SUPPRESS
    assert "2026-05-12T00:00:00Z" in decision.detail
    assert decision.window.reason == "upstream migration"


def test_decide_forwards_without_dag_id():
    assert core.decide(None, window_json(), NOW).action == core.ACTION_FORWARD


def test_decide_forwards_without_window():
    decision = core.decide("sales_daily", None, NOW)
    assert decision.action == core.ACTION_FORWARD
    assert decision.window is None


def test_decide_forwards_expired_with_note():
    decision = core.decide("sales_daily", window_json(end="2026-05-10T11:00:00Z"), NOW)
    assert decision.action == core.ACTION_FORWARD
    assert "expired" in decision.detail


def test_decide_forwards_pending():
    decision = core.decide("sales_daily", window_json(start="2026-05-11T00:00:00Z"), NOW)
    assert decision.action == core.ACTION_FORWARD
    assert "starts" in decision.detail


def test_decide_fails_open_on_bad_window():
    decision = core.decide("sales_daily", "{broken", NOW)
    assert decision.action == core.ACTION_FORWARD
    assert "invalid" in decision.detail


# --- formatting -------------------------------------------------------------


def test_slack_payload_keeps_original_text_and_adds_notes():
    decision = core.decide("sales_daily", window_json(end="2026-05-10T11:00:00Z"), NOW)
    payload = core.format_slack_payload(
        "Task failed", "DAG ID: sales_daily", decision, "dev", core.silence_hint("sales_daily")
    )
    assert payload["text"] == "[dev] Task failed"
    assert "DAG ID: sales_daily" in payload["blocks"][1]["text"]["text"]
    notes = [e["text"] for e in payload["blocks"][2]["elements"]]
    assert any("expired" in n for n in notes)
    assert any("maintenance.py set sales_daily" in n for n in notes)


def test_slack_payload_without_notes_has_no_context_block():
    decision = core.decide("sales_daily", None, NOW)
    payload = core.format_slack_payload(None, "something broke", decision, "")
    assert payload["text"] == "Airflow alert: sales_daily"
    assert [b["type"] for b in payload["blocks"]] == ["header", "section"]


def test_slack_payload_notes_missing_dag_id():
    decision = core.decide(None, None, NOW)
    payload = core.format_slack_payload(None, "something broke", decision, "")
    assert payload["text"] == "Airflow alert"
    assert "no DAG id" in payload["blocks"][2]["elements"][0]["text"]


def test_slack_payload_truncates_long_messages():
    decision = core.decide("d", None, NOW)
    payload = core.format_slack_payload("s", "x" * 10000, decision, "dev")
    assert len(payload["blocks"][1]["text"]["text"]) <= 3000


def test_build_emf_shape():
    record = core.build_emf("Airflow/Alerting", "SuppressedAlerts", "dev", "sales_daily", 1, {"reason": "r"})
    directive = record["_aws"]["CloudWatchMetrics"][0]
    assert directive["Namespace"] == "Airflow/Alerting"
    assert ["Environment", "DagId"] in directive["Dimensions"]
    assert record["SuppressedAlerts"] == 1
    assert record["DagId"] == "sales_daily"
    assert record["reason"] == "r"
