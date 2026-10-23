# maintenance-silencer

Stops a DAG that is known to be broken from posting the same failure alert to Slack every run, without hiding it for good.

You put a DAG into maintenance with a start, an end, a reason and your name:

```
$ python maintenance.py set sales_daily --for 3d --reason "Source API migration, ticket DATA-123"
sales_daily: silenced 2026-05-10T09:12:00Z to 2026-05-13T09:12:00Z (3d) by PlatformAdmin/oncall-user: Source API migration, ticket DATA-123

$ python maintenance.py list
DAG_ID          STATE    ENDS_UTC              LEFT    REASON (SET_BY)
orders_hourly   pending  2026-05-11T18:00:00Z  1d 8h   Planned reload (PlatformAdmin/data-eng)
sales_daily     active   2026-05-13T09:12:00Z  2d 23h  Source API migration, ticket DATA-123 (PlatformAdmin/oncall-user)
```

While the window is active, failure alerts for `sales_daily` are dropped before they reach Slack, and each dropped alert is counted in a CloudWatch metric. When the window ends, alerts come back on their own. Every other DAG is untouched.

This works like a downtime in CheckMK or Nagios: the silence is explicit, has an end, has a reason and an owner, and anyone can list what is silenced right now. The alternatives people reach for are worse. Pausing the DAG stops the data, not only the noise. Removing the failure callback or muting the channel hides the problem with no end date and no record of who did it. And leaving it alone trains everyone to ignore the alert channel, so the next real failure gets missed.

## How it works

```
Airflow on_failure_callback
        |
        v
   SNS alert topic  ------>  silencer Lambda  ------>  Slack incoming webhook
                                  |
                                  +-- GetParameter /airflow/maintenance/<dag_id>
                                  +-- suppressed: EMF log line, SuppressedAlerts metric
```

1. DAG failure callbacks publish to an SNS topic, as before. Instead of the topic going straight to Slack, the Lambda is subscribed to it.
2. The Lambda finds the DAG id in the message. It reads a JSON body first (`dag_id`, `dagId`, `dag`, also under `detail` or `context`), then common text forms (`DAG ID: x`, `dag_id=x`, `<DagRun x @ ...>`), then the subject.
3. It reads the SSM parameter `/airflow/maintenance/<dag_id>`:

   ```json
   {"start": "2026-05-10T09:12:00Z", "end": "2026-05-13T09:12:00Z", "reason": "Source API migration, ticket DATA-123", "set_by": "PlatformAdmin/oncall-user"}
   ```

4. If now is inside `[start, end)` the alert is dropped. The Lambda prints one CloudWatch Embedded Metric Format (EMF) log line. EMF is a JSON log format that CloudWatch turns into metrics, so the one line is both the log record of what was dropped (DAG, reason, owner, window end, original subject) and a data point for `Airflow/Alerting SuppressedAlerts`, with dimensions `Environment` and `Environment, DagId`.
5. Otherwise the alert is posted to Slack with its original text. If there was a window that has expired or not started yet, or the window JSON is invalid, a note saying so is added under the alert, along with the command to silence it.

All times are UTC. A timestamp without an offset is read as UTC. Offsets such as `+03:00` are accepted and converted.

The silencer fails open. No DAG id in the message, a malformed window, or an SSM error all result in the alert being forwarded. A bug in the silencer should produce extra noise, never silence.

Expired windows are ignored. With `DeleteExpiredWindows=true` the Lambda also deletes an expired window's parameter the next time an alert for that DAG arrives (only if the parameter has not been replaced in the meantime). Without it, `maintenance.py clear --expired` does the cleanup.

## Usage

### Deploy

Needs the AWS SAM CLI, an existing SNS topic that the failure callbacks publish to, and a Secrets Manager secret with the Slack incoming-webhook URL (either the bare URL or JSON like `{"webhook_url": "https://hooks.slack.com/..."}`).

```bash
sam build
sam deploy --guided \
  --parameter-overrides \
    AlertTopicArn=arn:aws:sns:<region>:<ACCOUNT_ID>:airflow-alerts-dev \
    SlackWebhookSecretArn=arn:aws:secretsmanager:<region>:<ACCOUNT_ID>:secret:slack/airflow-alerts-AbCdEf \
    EnvironmentLabel=dev
```

Then remove the old direct subscription from the topic to Slack (email-to-Slack address, AWS Chatbot, or whatever it was), otherwise every alert arrives twice.

Test with the sample event before cutting over:

```bash
sam local invoke SilencerFunction -e examples/sns-event.json
```

`examples/failure_callback.py` is a failure callback that publishes JSON with `dag_id` at the top level, which is the most reliable format for the silencer to parse.

### Manage windows

```bash
python maintenance.py set <dag_id> --for 3d --reason "..."              # starts now
python maintenance.py set <dag_id> --start 2026-06-01T06:00Z --until 2026-06-01T18:00Z --reason "planned reload"
python maintenance.py list                                              # active and pending
python maintenance.py list --all                                        # include expired and invalid
python maintenance.py clear <dag_id>                                    # alerts resume now
python maintenance.py clear --expired
```

`set_by` defaults to your AWS caller identity. Windows longer than 14 days are refused unless you pass `--allow-long` (or change `MAINTENANCE_MAX_DAYS`). A silence with no realistic end date is the same as deleting the alert, so a long one should be a deliberate choice.

The CLI needs `ssm:PutParameter`, `ssm:GetParametersByPath`, `ssm:DeleteParameter` on the prefix and `sts:GetCallerIdentity`. Who may set windows is controlled with normal IAM on that prefix. Parameter Store keeps the version history of each parameter, and CloudTrail records each change, so there is an audit trail of who silenced what.

### Watch what is being silenced

- `maintenance.py list` for what is silenced now.
- `SuppressedAlerts` by `DagId` on a dashboard shows which silenced DAGs are still failing. A DAG whose suppressed count drops to zero is fixed, and its window can be cleared early.
- CloudWatch Logs Insights on the Lambda's log group for the detail:

  ```
  fields @timestamp, DagId, reason, set_by, window_end, subject
  | filter event = "alert_suppressed"
  | sort @timestamp desc
  ```

## Tests

```bash
pip install -r ../requirements-dev.txt
pytest
```

The tests cover parsing of windows and messages, the window boundaries including time zone offsets, the CLI's window validation, Slack and EMF formatting, and the handler's forward, suppress, cleanup and fail-open paths with the AWS calls stubbed out.

## Limitations

- One window per DAG. Setting a new one replaces the old one. There is no per-task silencing.
- Only alerts that pass through this SNS topic are affected. Alerts sent some other way (email on failure, a CloudWatch alarm with its own action) are not.
- The DAG id has to be findable in the message. For a custom format that the built-in patterns do not match, add a pattern to `_TEXT_PATTERNS` in `src/silencer/core.py`.
- Parameter Store standard parameters are limited to 10,000 per account and Region and 4 KB per value. Neither is a practical concern here, but the prefix should not be shared with something that creates many parameters.
- Delivery to Slack is retried twice by Lambda's asynchronous invoke. After that the event goes to the dead-letter queue, which nothing reads automatically.
