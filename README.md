# airflow-mwaa-alerting

Alerting hygiene for Amazon MWAA (Managed Workflows for Apache Airflow). The goal is an alert channel that people still read: known-broken DAGs are silenced on purpose and for a limited time, non-production environments report when too much is running, and whoever picks up an alert has a runbook for the failures that keep coming back.

| Path | What it does |
|---|---|
| [`maintenance-silencer/`](maintenance-silencer/) | Lambda between the Airflow alert SNS topic and Slack. Drops failure alerts for a DAG while it has an active maintenance window (start, end, reason, owner in SSM Parameter Store) and counts what it dropped. A CLI sets, lists and clears windows. |
| [`running-dag-alert/`](running-dag-alert/) | Scheduled Lambda that counts running DAG runs per MWAA environment through the MWAA `InvokeRestApi` API and posts the list of DAGs to Slack above a threshold (default 10). Works on Airflow 2 and 3. |
| [`docs/failure-triage-runbook.md`](docs/failure-triage-runbook.md) | Symptom, cause, checks and fix for common failures: sensor timeouts on DynamoDB status records, sensors that find an error record, Glue `ConcurrentRunsExceededException`, AWS Batch "Essential container in task exited", and a missing Secrets Manager permission that turns into `NoSuchBucket`. |
| [`docs/airflow-3-migration-notes.md`](docs/airflow-3-migration-notes.md) | Moving DAGs from Airflow 2.10 to 3.x on MWAA: ruff AIR rules, `airflow config lint` and `update`, the DAG changes that come up most, and which of them can ship while still on 2.x. |

## The problem these address

A typical setup: DAG failure callbacks publish to an SNS topic, and the topic delivers to a Slack channel. It works until a few DAGs fail every run for weeks because of something outside the team's control (an upstream system being migrated, a source that stopped sending). The channel fills with the same alerts, people stop reading it, and someone ends up watching it by hand to spot the new failures among the old ones.

The usual fixes make things worse. Pausing a DAG stops the data along with the noise. Removing a callback or muting the channel hides the problem with no end date and no record. The silencer here keeps the alert path intact and adds an explicit, time-boxed exception per DAG that anyone can list.

The second gap is cost and capacity in non-production environments. DAGs get switched on to test something and stay on, or a DAG with catchup enabled starts many runs at once. The running-DAG alert replaces checking each environment's UI by hand.

## Layout

```
maintenance-silencer/
  src/silencer/        core.py (pure logic), handler.py (Lambda)
  maintenance.py       CLI: set | list | clear
  template.yaml        SAM template
  examples/            failure callback, sample SNS event
  tests/
running-dag-alert/
  src/running_dag_alert/  core.py (pure logic), handler.py (Lambda)
  template.yaml           SAM template
  tests/
docs/
```

Each component deploys on its own with the AWS SAM CLI. See the README in each folder.

## Tests

```bash
pip install -r requirements-dev.txt
(cd maintenance-silencer && pytest)
(cd running-dag-alert && pytest)
```

The logic both Lambdas depend on (parsing, time windows, paging, counting, formatting) lives in `core.py` modules with no AWS calls, and is tested directly. Handler tests replace the AWS clients with stubs.

## Requirements

- Python 3.12 (Lambda runtime). The code itself runs on 3.10 and later.
- AWS SAM CLI to deploy.
- `boto3` for the CLI and tests. Lambda provides it at runtime.
- An MWAA environment on Airflow 2.4.3 or later for `InvokeRestApi`.

## License

MIT. See [LICENSE](LICENSE).
