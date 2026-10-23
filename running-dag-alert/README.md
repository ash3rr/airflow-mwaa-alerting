# running-dag-alert

Tells you when too many DAG runs are running at once in a non-production MWAA environment, and which DAGs they are.

Every 15 minutes a Lambda counts the DAG runs in state `running` in each configured environment. When the count is above the threshold (10 by default) it posts to Slack:

```
[preprod] 14 DAG runs running (threshold 10)

orders_hourly  (3 runs, oldest running 2h 40m)
customer_snapshot  (1 run, oldest running 1d 3h)
sales_daily  (1 run, oldest running 47m)
...

Check whether these need to be running. Pause or mark failed the ones that do not,
and look at why so many started together (catchup, a backfill, a shared upstream trigger).
14 running runs across 9 DAGs at 2026-05-10 12:00 UTC | Airflow UI
```

In development and pre-production environments, DAGs are switched on to test something and then forgotten, or a DAG with catchup enabled starts dozens of runs after being unpaused. Each running DAG costs worker capacity and whatever it calls (Glue, Batch, a warehouse). Without an alert someone has to open each environment's UI now and then and look. With it, the person on duty gets the list when it matters and can decide what to switch off.

## How it works

1. An EventBridge Scheduler schedule invokes the Lambda (default every 15 minutes).
2. For each environment in `MwaaEnvironments`, the Lambda calls the Airflow REST API through the MWAA `InvokeRestApi` API: `GET /dags/~/dagRuns?state=running`, paged 100 at a time. `~` as the DAG id means all DAGs.
3. It groups the runs by DAG and publishes `RunningDagRuns` and `RunningDags` to CloudWatch (namespace `Airflow/Alerting`, dimension `Environment`) on every run, as an EMF log line.
4. If the count is above the threshold and no alert was sent for that environment within the cooldown (60 minutes by default), it posts the list to Slack and stores the alert time in SSM Parameter Store.

`CountBy` chooses what is compared with the threshold: `runs` (default) counts DAG runs, `dags` counts distinct DAGs with at least one running run. They differ for DAGs with `max_active_runs` above 1, for example during a backfill.

### Calling the Airflow REST API on MWAA

There is no Airflow username or password involved. MWAA offers two supported ways to call the REST API, both authorised by IAM:

- `InvokeRestApi` (used here). One AWS API call per Airflow REST call: `mwaa.invoke_rest_api(Name=..., Path="/dags/~/dagRuns", Method="GET", QueryParameters={...})`. The response has `RestApiStatusCode` and `RestApiResponse`. It is available for environments on Airflow 2.4.3 and later. The caller needs `airflow:InvokeRestApi` on `arn:aws:airflow:<region>:<account>:role/<environment>/<airflow-role>`, and the Airflow role in that ARN (`Viewer`, `User`, `Op`, `Admin`) is the role the call runs as. Listing DAG runs needs only `Viewer`, which is the template's default. Limits from the AWS documentation: 10 second timeout, 6 MB response, `Path` at most 64 characters. The Lambda calls the MWAA service API, not the web server, so it does not run in the VPC. AWS documents an `aws:SourceVpc` / `aws:SourceVpce` IAM condition for restricting where `InvokeRestApi` may be called from. Test it once against an environment with a private web server before relying on it there, since the access-mode documentation does not address it.
- A web login token. `create_web_login_token` returns a token valid for 60 seconds. You exchange it at the web server for a session (Airflow 2: POST to `/aws_mwaa/login`, cookie `session`, then call `/api/v1/...`. Airflow 3: POST to `/pluginsv2/aws_mwaa/login`, cookie `_token` used as a Bearer token, then call `/api/v2/...`). AWS suggests this when you need more than InvokeRestApi's throughput. It needs network access to the web server and `airflow:CreateWebLoginToken`.

Airflow 2 and 3. Airflow 3 replaced the `/api/v1` REST API with `/api/v2`. With `InvokeRestApi`, `Path` is given without the version prefix and MWAA routes it to the environment's API, so the same call works on both. The endpoint used here has the same shape in both versions: `/dags/~/dagRuns` accepts `state`, `limit`, `offset` and `order_by` and returns `dag_runs` and `total_entries`, and each run has `dag_id`, `state` and `start_date`. The Lambda reads the environment's `AirflowVersion` and logs it, and `core.rest_base_path()` gives the prefix (`/api/v1` or `/api/v2`) for anyone adapting this to the web-token method.

Paging. The Airflow REST API caps a page at `[api] maximum_page_limit`, 100 by default, without saying so in the response. The Lambda pages by `offset` until it has `total_entries` runs, so a lower server cap only means more pages.

## Usage

```bash
sam build
sam deploy --guided \
  --parameter-overrides \
    MwaaEnvironments="dev=analytics-dev,preprod=analytics-preprod" \
    MwaaEnvironmentNames="analytics-dev,analytics-preprod" \
    SlackWebhookSecretArn=arn:aws:secretsmanager:<region>:<ACCOUNT_ID>:secret:slack/airflow-ops-AbCdEf \
    Threshold=10
```

`MwaaEnvironments` gives each environment a short label for the alert and the metric. `MwaaEnvironmentNames` repeats the names as a list so the IAM policy can be scoped to exactly those environments.

To test the alert, set `Threshold=0` (any running DAG triggers it) and `CooldownMinutes=0`, trigger a DAG in a development environment, and invoke the function:

```bash
sam remote invoke RunningDagAlertFunction --stack-name <stack>
```

Then set the threshold back. To test with a realistic load, trigger more DAGs than the threshold and wait for the schedule.

The template also creates a CloudWatch alarm on the Lambda's `Errors` metric. A check that is failing (an expired permission, an environment that was renamed) is otherwise indistinguishable from a quiet environment. Pass `AlarmTopicArn` to send it somewhere.

## Tests

```bash
pip install -r ../requirements-dev.txt
pytest
```

Unit tests cover environment parsing, paging (including a server that returns fewer rows than asked for), grouping and sorting, the threshold and cooldown, and the Slack and EMF formatting. Handler tests run the full check against a fake MWAA client for both an Airflow 2 and an Airflow 3 environment.

## Limitations

- The count is a snapshot every 15 minutes. A burst that starts and ends between checks is not seen. The CloudWatch metrics are there if you want an alarm with its own evaluation periods instead of the Lambda's threshold.
- It counts DAG runs, not tasks or worker slots. One DAG run with 50 parallel tasks counts as 1. MWAA's own CloudWatch metrics (running and queued tasks per environment) cover that side.
- `InvokeRestApi` calls are throttled by AWS. One check makes one call per page per environment, far below the limit, but do not shorten the schedule to seconds.
- The cooldown is per environment, not per set of DAGs. If the list changes completely within the cooldown, the second alert waits.
- All environments must be in the same account and Region as the Lambda. Deploy one stack per account and Region otherwise.
