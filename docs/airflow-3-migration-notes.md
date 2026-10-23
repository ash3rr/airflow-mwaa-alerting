# Airflow 2 to 3 migration notes for MWAA DAGs

Notes for moving a large set of DAGs from Airflow 2.10.x to 3.x on Amazon MWAA. The approach that worked was to make every change that is valid on both versions while still on 2.x, one easy DAG at a time, so the version switch itself carries as little DAG change as possible. Each item below is checked against the Airflow release notes, the official upgrade guide, the Airflow source for the relevant version, or a run of ruff against a sample DAG. Sources are linked at the end.

Versions referred to here: Airflow 2.10.x (starting point), 2.11 (the last 2.x minor, which adds migration tooling), 3.x (the target). MWAA added Airflow 2.11 in January 2026 and 3.2 in May 2026. The MWAA announcement for 3.2 says environments upgrade to it from 2.11 or later.

## Order of work

1. Upgrade the environment to 2.11 first. It is still Airflow 2, so DAGs keep working, and it adds `airflow config lint` and `airflow config update` and a `logical_date` field on every model that has `execution_date`.
2. Run ruff with the `AIR3` rules over the DAGs folder and the plugins folder to get the list of what must change.
3. Fix the changes that are valid on 2.x (table below) in small batches, starting with simple DAGs. Deploy and let them run on 2.11.
4. Run `airflow config update` to find configuration options that are removed or renamed in 3.
5. Upgrade a non-production environment to 3.x, make the remaining 3-only changes, and fix what breaks there. Only then do production.

The value of step 3 is that every change there is shipped and proven on the old version. When the upgrade happens, a failure is about the upgrade, not about a hundred edits made the same week.

## Finding the work: ruff AIR rules

[Ruff](https://docs.astral.sh/ruff/) has a set of Airflow rules. The ones for the upgrade:

| Code | Name | Meaning |
|---|---|---|
| AIR301 | airflow3-removal | Removed in Airflow 3.0. Must be fixed. |
| AIR302 | airflow3-moved-to-provider | Moved into a provider package in 3.0. Must be fixed (install the provider, change the import). |
| AIR303 | airflow3-incompatible-function-signature | A function signature changed in 3.0. |
| AIR311 | airflow3-suggested-update | Deprecated in 3.0, still works for now. Recommended. |
| AIR312 | airflow3-suggested-to-move-to-provider | Moved into a provider, the old path still works in 3.0. Recommended. |
| AIR321 | airflow31-moved | Moved in Airflow 3.1. Preview rule at the time of writing. |

The upgrade guide says AIR301 and AIR302 are breaking and AIR311 and AIR312 are strongly recommended. It asks for ruff 0.13.1 or later.

```bash
# What breaks on Airflow 3
ruff check dags/ plugins/ --select AIR301,AIR302,AIR303

# What is deprecated but still works
ruff check dags/ plugins/ --select AIR311,AIR312

# Show the fixes ruff would make, then apply the safe ones
ruff check dags/ --select AIR301 --show-fixes
ruff check dags/ --select AIR301 --fix
```

Running ruff 0.16 against a typical Airflow 2 DAG gives findings like these, which line up with the changes most DAGs need:

```
AIR301 `schedule_interval` is removed in Airflow 3.0
AIR301 `concurrency` is removed in Airflow 3.0
AIR301 `orientation` is removed in Airflow 3.0
AIR301 `fail_stop` is removed in Airflow 3.0
AIR301 `provide_context` is removed in Airflow 3.0
AIR301 `execution_date` is removed in Airflow 3.0
AIR302 `airflow.operators.dummy.DummyOperator` is moved into `standard` provider in Airflow 3.0
AIR311 `airflow.DAG` is removed in Airflow 3.0; It still works in Airflow 3.0 but is expected to be removed in a future version.
AIR312 `airflow.operators.python.PythonOperator` is deprecated and moved into `standard` provider in Airflow 3.0; It still works ...
```

Ruff marks some fixes as unsafe (for example changing only an import path, not the name). Review those by hand rather than passing `--unsafe-fixes` over the whole folder.

## Configuration: `airflow config lint` and `airflow config update`

Both commands were added in 3.0 and backported to 2.11 (release notes for 2.11.0, PRs #45736 and #50353). On 2.10 they do not exist, which is one more reason to go through 2.11.

```bash
airflow config lint                         # list removed or invalid options
airflow config update                       # dry run: show breaking changes
airflow config update --all-recommendations # include non-breaking recommendations
airflow config update --fix                 # apply the changes
```

On MWAA there is no shell on the scheduler. Run these in a local Airflow 2.11 container with the environment's Airflow configuration options copied into its `airflow.cfg` or environment variables, then apply the results as configuration options on the MWAA environment.

## DAG changes seen in practice

The "Valid on 2.x" column is the point of the exercise: anything marked yes can be changed and deployed before the upgrade.

| Airflow 2 | Airflow 3 | Valid on 2.x | Ruff |
|---|---|---|---|
| `DAG(schedule_interval=...)` | `DAG(schedule=...)` | Yes, `schedule` exists since 2.4 | AIR301 |
| `DAG(timetable=...)` | `DAG(schedule=<timetable>)` | Yes | AIR301 |
| `DAG(concurrency=12)` | `DAG(max_active_tasks=12)` | Yes | AIR301 |
| `DAG(orientation="LR")` | delete the argument | Yes, deleting it only changes the old UI's default graph direction | AIR301 |
| `PythonOperator(..., provide_context=True)` | delete the argument | Yes, it has done nothing since 2.0 | AIR301 |
| `context["execution_date"]`, `{{ execution_date }}`, `execution_date` callable argument | `logical_date`, or `data_interval_start` / `data_interval_end` | Yes, `logical_date` is in the 2.x context | AIR301 |
| `prev_ds`, `next_ds`, `yesterday_ds`, `tomorrow_ds` and the other removed date keys | compute from `data_interval_start` / `data_interval_end` or `logical_date` | Yes | AIR301 |
| `from airflow.operators.python import PythonOperator` (also Bash, Empty, sensors such as `ExternalTaskSensor`) | `from airflow.providers.standard.operators.python import PythonOperator` | Yes on 2.11, see below | AIR302 / AIR312 |
| `DummyOperator` | `EmptyOperator` from the standard provider | Yes, `EmptyOperator` exists in 2.x | AIR302 |
| `from airflow import DAG`, `from airflow.decorators import dag, task` | `from airflow.sdk import DAG, dag, task` | No, `airflow.sdk` is 3.x only | AIR311 |
| `DAG(fail_stop=True)` | `DAG(fail_fast=True)` | No, `fail_fast` does not exist in 2.x | AIR301 |
| `sla_miss_callback`, task `sla` | removed. Use Deadline Alerts (experimental from 3.1), or failure callbacks and external monitoring | Removing it is valid, the replacement is 3.x only | AIR311 |
| SubDAGs (`SubDagOperator`) | TaskGroups | Yes, TaskGroups exist in 2.x | AIR301 |
| Calls to the REST API at `/api/v1` | `/api/v2` | No | not checked by ruff |

Notes on the less obvious rows.

`concurrency` and `max_active_tasks`. In Airflow 2.10 `concurrency` is a deprecated alias that is copied into `max_active_tasks`, so renaming it on 2.x changes nothing. On Airflow 3 the meaning of `max_active_tasks` itself changed: the Task SDK documentation says it was a limit across the whole DAG in Airflow 2 and is per DAG run in Airflow 3. A DAG with `max_active_runs=3, max_active_tasks=12` can run up to 36 tasks at once on Airflow 3, against 12 on Airflow 2. Check DAGs that rely on this to protect a downstream system.

`orientation`. Not listed in the 3.0 release notes or the upgrade guide, but it is not a parameter of the Airflow 3 `DAG` class (`airflow.sdk`), and ruff reports it as AIR301. On 2.x it only set the default direction of the old Graph view. The new UI has its own layout control. Delete it.

`provide_context`. Removed from the Python operators in Airflow 2.0, which since then only warns and drops it ("provide_context is deprecated as of 2.0 and is no longer required"). In Airflow 3 any unknown keyword argument to an operator raises `TypeError: Invalid arguments were passed`, so a leftover `provide_context=True` breaks DAG parsing. Delete it. Context is passed to the callable whenever its signature asks for it (`**context` or named keys).

`execution_date`. The 3.0 release notes list `execution_date` among context variables removed from the task context, with `dag_run.logical_date` as the replacement. Template references such as `{{ execution_date }}` and callables that take an `execution_date` argument both need changing. `{{ ds }}` and `{{ ts }}` are still available. ruff finds dictionary lookups such as `context["execution_date"]`, but not every string inside a Jinja template, so also search the DAGs folder for `execution_date`, `next_ds`, `prev_ds` and friends. When changing the logic, note that for a manually triggered run on Airflow 3 `logical_date` can be `None`, and the data interval fields can be missing for asset-triggered and manual runs.

Standard provider. `PythonOperator`, `BashOperator`, `EmptyOperator`, `ExternalTaskSensor`, `FileSensor`, `ShortCircuitOperator`, `LatestOnlyOperator` and related hooks and triggers moved to `apache-airflow-providers-standard`. The release notes say the package is installable on Airflow 2.x so imports can be switched early. Current versions of the provider require Airflow 2.11 or later, so this is a change to make after the 2.11 step. On MWAA, add the provider to `requirements.txt` with the version from the MWAA constraints file for the environment's Airflow version.

`airflow.sdk`. The stable authoring interface (`DAG`, `dag`, `task`, `TaskGroup`, `Variable`, `Asset` and others) is new in Airflow 3 and the package is not in 2.x. The old import paths still work in 3.0 and are reported by AIR311, with deprecation warnings from 3.1. Leave these for after the upgrade, then do them in bulk.

Defaults that changed. On Airflow 3 `catchup` defaults to `False` (through `[scheduler] catchup_by_default`) and the default schedule is `None` instead of `@once`. A DAG that relied on either default needs it written out explicitly. Setting `catchup=` and `schedule=` explicitly on every DAG is valid on 2.x and removes the question.

## Things outside the DAG files

- Plugins and shared helper modules imported by DAGs need the same ruff pass. They often hold the oldest imports.
- Task code can no longer open a session on the metadata database. Anything that queried `DagRun` or `TaskInstance` directly inside a task has to use the REST API or the context instead. Ruff does not catch this.
- Scripts and Lambdas that call the Airflow REST API move from `/api/v1` to `/api/v2`. On MWAA, `InvokeRestApi` takes the path without the version prefix, so callers that use it, such as the [running-dag-alert](../running-dag-alert/) here, do not change. Callers that log in with a web token directly change their login path as well (see the MWAA REST API page).
- The `airflow` CLI is split: remote operations such as triggering DAGs or managing connections moved to a separate `airflowctl` client. Scripts that ran remote commands through the MWAA CLI token endpoint should be checked against the MWAA CLI command reference for 3.x before the upgrade.

## Sources

- Upgrading to Airflow 3 (3.2.1 docs): https://airflow.apache.org/docs/apache-airflow/3.2.1/installation/upgrading_to_airflow3.html
- Airflow release notes (3.0.0 "Unified Scheduling Field", "Context Cleanup and Parameter Removal", "REST API v2 replaces v1", "Provider Refactor & Standardization", "Summary of Removed Features", and 2.11.0 "Ease migration to Airflow 3"): https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- Airflow pull requests removing the deprecated DAG arguments: `concurrency` and `full_filepath` (https://github.com/apache/airflow/pull/41744), `schedule_interval` and `timetable` (https://github.com/apache/airflow/pull/41453)
- Task SDK API reference (`DAG` parameters, `max_active_tasks` per run): https://airflow.apache.org/docs/task-sdk/stable/api.html
- Airflow CLI reference, `config lint` and `config update`: https://airflow.apache.org/docs/apache-airflow/2.11.0/cli-and-env-variables-ref.html
- Ruff Airflow rules: https://docs.astral.sh/ruff/rules/#airflow-air
- MWAA support for Airflow 2.11: https://aws.amazon.com/about-aws/whats-new/2026/01/apache-airflow-2-11-support-amazon-managed-workflows
- MWAA support for Airflow 3.2: https://aws.amazon.com/about-aws/whats-new/2026/04/amazon-mwaa-now-supports-apache-airflow-3-2/
- MWAA, using the Airflow REST API: https://docs.aws.amazon.com/mwaa/latest/userguide/access-mwaa-apache-airflow-rest-api.html
