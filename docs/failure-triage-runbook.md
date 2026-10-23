# Airflow failure triage runbook

For whoever picks up a failed DAG alert. Each entry gives the symptom you see in the task log, the usual cause, what to check and in what order, and the fix. The examples are pipelines where Airflow orchestrates work that runs elsewhere (Lambda, Glue, AWS Batch) and waits on it, which is where most of the confusing failures come from: the Airflow task fails, but the real error is in another service.

General order for any failure:

1. Open the failed task's log and find the first exception, not the last line. Retries repeat the same error and the final attempt often shows only the timeout.
2. Decide whether the failure is in Airflow itself (import error, permissions of the MWAA execution role, sensor timeout) or in the service Airflow was waiting on (Lambda, Glue, Batch). The fix is almost always in the second.
3. Check whether the DAG has failed the same way before. A DAG that has failed every run for weeks needs a maintenance window and a ticket, not another retry. See [maintenance-silencer](../maintenance-silencer/).

Contents:

- [Sensor timeout waiting for a DynamoDB record](#sensor-timeout-waiting-for-a-dynamodb-record)
- [Sensor finds a record that reports an error](#sensor-finds-a-record-that-reports-an-error)
- [Glue ConcurrentRunsExceededException](#glue-concurrentrunsexceededexception)
- [AWS Batch job FAILED, "Essential container in task exited"](#aws-batch-job-failed-essential-container-in-task-exited)
- [Variable lookup denied, then NoSuchBucket](#variable-lookup-denied-then-nosuchbucket)

---

## Sensor timeout waiting for a DynamoDB record

### Symptom

A sensor task fails with `AirflowSensorTimeout`, for example:

```
airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of 908.3 seconds exceeds the specified timeout of 900.0.
```

Earlier in the same log the sensor repeats something like `Nothing found for {'source_system': 'pipeline_x', 'source_file': 'export_20260503.json'}` or `Collected: 0, expected: 1` on every poke.

### Likely cause

The pattern is: an upstream step (usually a Lambda triggered by a file landing in S3, or invoked by a previous task) processes the input and writes a status record to a DynamoDB table. The sensor polls the table for that record by key. A timeout means the record never appeared under the key the sensor was looking for. In order of how often it happens:

1. The upstream Lambda failed or was never invoked, so nothing was written.
2. The Lambda ran but wrote the record under a different key (different file name, date format, batch id, or source system name than the sensor expects).
3. No input file arrived, so there was nothing to process.
4. The Lambda is still running or is slow today, and the record would have appeared after the timeout.

### Checks

1. Copy the exact key from the sensor log (`source_system`, `source_file`, `batch_id`, whatever the sensor prints). This is the contract between the two sides.
2. Find the upstream Lambda's CloudWatch log group (`/aws/lambda/<function-name>`) and look at invocations around the DAG run's start time. You are looking for one of: no invocation at all, an invocation that ended in an error, or a successful invocation.
3. If there was no invocation, go one step further back: did the input file land in S3 (`aws s3 ls s3://<bucket>/<prefix>/`)? If the Lambda is triggered by S3 events, is the notification still configured on the bucket?
4. If the Lambda errored, the stack trace in its log is the real failure. Fix that, not the DAG.
5. If the Lambda succeeded, query DynamoDB for what it actually wrote (`aws dynamodb query` on the partition key, or scan the last few items in the console) and compare the key with what the sensor expects. A mismatch here is a code or config bug in one of the two sides.
6. Compare with the last successful run of the same DAG. If it used to succeed and stopped on a specific date, look for a deployment of the Lambda or the DAG on that date.

### Fix

- Lambda failed: fix the Lambda, then clear the sensor task (and anything downstream) in the Airflow UI so it re-runs. If the Lambda needs re-invoking for this run, do that first.
- Key mismatch: fix whichever side is wrong and deploy, then clear the task.
- No input: confirm with the source owner. If no file is expected for that day, mark the run success or skip it. Do not let it fail daily.
- Increase the sensor `timeout` only when the checks show the upstream work is legitimately slow (the record does appear, just after the timeout) and that latency is expected, for example a larger daily volume. Raising the timeout for a record that never arrives only makes the failure take longer.
- Make sure long sensors use `mode="reschedule"` (or a deferrable sensor) so a waiting sensor does not hold a worker slot.

---

## Sensor finds a record that reports an error

### Symptom

The sensor does not time out. It finds the record and then fails immediately with a message from the record itself, for example:

```
Received error in the underlying service: 1/1, DDB keys: batch_id: 9f3c..., layer: publish
```

The `1/1` style counter usually means "1 of 1 expected records reported an error".

### Likely cause

The sensor is working as designed. The upstream service finished and wrote a status record, but the status is an error. Common cases are data quality tests that ran after a load step and failed, or a processing Lambda that caught an exception and recorded it rather than crashing.

### Checks

1. Look up the record by the keys in the log message and read its error or status field. The record usually has a message, a failing test name, or a pointer to a log.
2. If the record comes from a test step (for example tests run after a load into a `publish` layer), look at the test results: which test failed, on which table, with how many rows. A test failure means the data is wrong, and stopping here is the point.
3. Look at the logs of the task that ran just before the tests (the load step). Warnings there often explain the test failure: zero rows loaded, duplicates, a schema change.
4. Check whether the same error appeared in previous runs. A recurring error points at a persistent data or schema problem upstream rather than a one-off.

### Fix

- Fix the underlying data or test problem, then re-run from the step that produced the bad data, not only the sensor. Clearing only the sensor re-reads the same error record.
- If the test is wrong (a threshold that no longer makes sense, a test on a dropped column), change the test through the normal review process.
- If the error record is stale (left over from an earlier attempt with the same key), remove it or make sure re-runs write a fresh key, otherwise every retry fails instantly.

---

## Glue ConcurrentRunsExceededException

### Symptom

A task that starts a Glue job fails at submission:

```
botocore.errorfactory.ConcurrentRunsExceededException: An error occurred (ConcurrentRunsExceededException) when calling the StartJobRun operation: Concurrent runs exceeded
```

It often appears on a frequently scheduled DAG (hourly or more) and in bursts.

### Likely cause

The Glue job has a maximum number of concurrent runs (the job's `MaxConcurrentRuns`, 1 by default), and that many runs are already active. Usually:

1. The previous run is overrunning. It normally takes 8 minutes, today it has been running for 35, and the next scheduled run tried to start on top of it.
2. A data volume spike for one period made a run slower than the schedule interval.
3. Multiple DAG runs were active at once (catchup, a manual backfill, or `max_active_runs` above 1) and each tried to start the same job.
4. The previous run is stuck (waiting on a lock, a slow source, or a hung connection) and will never finish on its own.

### Checks

1. In the Glue console, open the job's Runs tab, or `aws glue get-job-runs --job-name <job> --max-results 5`. Find the run in `RUNNING` and note how long it has been running compared with the usual duration.
2. Open that run's logs (the Glue output and error log groups, or the run's continuous logs) and check whether it is progressing or stuck in one stage.
3. Check the input volume for the period that run is processing, for example the number and size of objects under the hour's S3 prefix. A large jump explains a slow run.
4. In Airflow, check how many runs of the DAG are active. More than one active run of an hourly DAG that triggers a single-concurrency job is the problem on its own.

### Fix

Immediate:

- If the running job is progressing and only slow, let it finish. Then clear the failed task so it runs for its period.
- If it is stuck, stop it (`aws glue batch-stop-job-run --job-name <job> --job-run-ids <id>`), then re-run the periods that were missed in order.

Durable, pick what fits the pipeline:

- Set `max_active_runs=1` on the DAG so a new DAG run does not start while the previous one is still running. This makes overlap impossible from the Airflow side and is the right default for a DAG that triggers a single-concurrency job.
- Give the start task retries with backoff (`retries`, `retry_delay`, `retry_exponential_backoff=True`) so a short overlap resolves itself instead of paging someone.
- Alert on long runs: a sensor or task `execution_timeout`, or a CloudWatch alarm on the Glue job's elapsed time, set at a multiple of the normal duration. A run at 4x normal duration is worth a message before it blocks the next one.
- Raise `MaxConcurrentRuns` on the Glue job only if parallel runs over different periods are safe for the job's output (no shared target partition, no overwrite of the same files).

---

## AWS Batch job FAILED, "Essential container in task exited"

### Symptom

A sensor waiting on an AWS Batch job fails with a dump of the job description that includes:

```
'status': 'FAILED'
'exitCode': 1
'statusReason': 'Essential container in task exited'
'logStreamName': '<job-definition-name>/default/<task-id>'
```

### Likely cause

The Batch job ran and the program inside the container exited with a non-zero code. `Essential container in task exited` is the generic message for that. It does not say why. Exit code 1 means the program itself failed (an unhandled exception, a failed check). Other codes point elsewhere: 137 usually means the container was killed for exceeding its memory, and a failure with no exit code and a reason about the image or host means the container never started.

### Checks

1. Copy the `logStreamName` from the error. That is the container's log.
2. Open it in CloudWatch Logs. Batch writes to the `/aws/batch/job` log group by default unless the job definition sets its own log configuration. Search with `aws logs get-log-events --log-group-name /aws/batch/job --log-stream-name <logStreamName>`, or paste the stream name into the console search.
3. Read the end of the stream. The traceback or error printed by the program just before it exited is the actual failure: bad input data, an S3 permission error on the job role, a missing parameter, a dependency that changed in the image.
4. Check the job's parameters in the Batch console (Jobs, then the job id). A wrong prefix or date passed from the DAG shows up there.
5. If the exit code is 137 or the log stops mid-way, check the job's memory setting against the input size for that run.

### Fix

- Fix what the container log shows. Common ones: the input file format changed, the job role lacks access to a new bucket or prefix, the image was rebuilt with a different dependency version.
- Out of memory: raise memory in the job definition, or split the input.
- Re-run by clearing the task that submits the job (not only the sensor), so a new Batch job is submitted. Clearing only the sensor waits on the same failed job id.

---

## Variable lookup denied, then NoSuchBucket

### Symptom

Two errors in one task log, usually a few lines apart. First, Airflow fails to read a variable from Secrets Manager:

```
AccessDeniedException: ... is not authorized to perform: secretsmanager:GetSecretValue on resource: airflow/variables/BACKUP_BUCKET
```

Later, the task fails writing to S3:

```
botocore.errorfactory.NoSuchBucket: An error occurred (NoSuchBucket) when calling the PutObject operation: The specified bucket does not exist
```

### Likely cause

This is a chain of three things, all of which must be right:

1. The secret. The environment uses Secrets Manager as its secrets backend, so `Variable.get("BACKUP_BUCKET")` reads the secret `airflow/variables/BACKUP_BUCKET`. The MWAA execution role must be allowed `secretsmanager:GetSecretValue` on it.
2. The name. The secret's value is a bucket name. It has to be the right name for this environment.
3. The bucket. The bucket with that name has to exist, in the expected account and Region, and the execution role has to be allowed to write to it.

When the role cannot read the secret, Airflow logs the error and moves on to the next backend (environment variables, then the metadata database). If a variable with the same key exists there, possibly an old one copied from another environment, the task silently gets that value instead. That is how a permission error turns into NoSuchBucket: the task got a stale bucket name from the fallback backend. If no fallback value exists, the task fails earlier with a `KeyError` for the variable.

### Checks

Work down the chain in order:

1. Secret readable? Find the execution role on the MWAA environment page and check its policies for `secretsmanager:GetSecretValue` on `arn:aws:secretsmanager:<region>:<ACCOUNT_ID>:secret:airflow/variables/*`. Secrets Manager adds a random six-character suffix to secret ARNs, so the resource must end in a wildcard. A policy that only names `airflow/connections/*` is a common gap.
2. Secret exists and is right? `aws secretsmanager describe-secret --secret-id airflow/variables/BACKUP_BUCKET`, then check the value is the bucket name intended for this environment (development and production names often differ by one segment).
3. Fallback value? Check Admin, then Variables in the Airflow UI for a variable with the same key. If one exists, it is what the task has been using.
4. Bucket exists? `aws s3api head-bucket --bucket <name-from-secret>`. A 404 means it does not exist (or was never created in this account). A 403 means it exists but the role cannot access it.
5. Role can write? Check the execution role's S3 permissions for `s3:PutObject` on `arn:aws:s3:::<bucket>/*`.

### Fix

- Add the missing `secretsmanager:GetSecretValue` statement for `airflow/variables/*` to the execution role, through whatever manages that role (CloudFormation, CDK, Terraform), not by hand in the console.
- Remove any stale variable with the same key from the Airflow metadata database, so a future permission problem fails loudly instead of using an old value.
- If the bucket does not exist, find out whether it should: create it through infrastructure code if it is genuinely missing in this environment, or correct the secret's value if it points at the wrong name.
- Clear the failed task once all three links in the chain are fixed.
