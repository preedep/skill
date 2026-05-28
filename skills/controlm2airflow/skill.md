Skill: Convert Control-M jobs to Apache Airflow DAGs

## Purpose
This skill converts Control-M jobs (XML) to Apache Airflow DAGs. (DAGs python code)

## Input
The input is a Control-M job XML file.

## Output
The output is a Python script that defines an Apache Airflow DAG. must follow standard patterns

| Pattern | Description |Example|
|---------|-------------|------|
| Naming convention DAG file | DAG file follow  `<company>-<app_id>-<app_code>-<folder_name>-<env>.py` for dag_name is existing folder name of job control  use small case (eg. acme-AP1234-pyment-abc-monthly-tab-dev.py) | `acme-AP1234-payment-abc-monthly-tab-dev.py` |
| Naming convention DAG ID | same as filename without `.py` | `acme-AP1234-payment-abc-monthly-tab` |
| Naming convention (task) | Task IDs follow `<app_id>-<app_code>-task_<task_name>-<execution_period>` for task_name is existing job name of job control  use small case (eg. AP1234-pyment-task_rt-rb2cm005-d) , for execution_period use small case (eg.  d=daily, w=weekly, m=monthly,y=yearly) | `AP1234-pyment-task_rt-rb2cm005-d` |
| Control-M Folder | Smart Folder / Control-M Folder replacement with DAG level |   |
| Control-M Job | Control-M Job replacement with DAG task |  |
| Control-M Job Type | Control-M Job Type replacement with Operator or Sensor |  |
| Control-M Job dependencies | >> operator is used to define task dependencies which focus in same folder| `task_1 >> task_2` |
| ODATE Replacement | ODATE values are replaced with Airflow 'logical date' | `{{ logical_date}}` or `{{ ds_nodash }}` or `{{ ds }}` |
| Error alert | uses email_on_failure attriute which default is false (add all DAGs) | `email_on_failure=False`|
| Retry alert | uses email_on_retry attriute which default is false (add all DAGs) | `email_on_retry=False` |
| SLA Alert | uses DeadlineAlert of Airflow 3.x  | `DeadlineAlert` |
| Timetables | Use a custom `Timetable` class when the schedule cannot be expressed as a cron string (e.g. banking holidays, special run dates). Emit `# TODO: implement custom Timetable` and `schedule=None` as placeholder. | `schedule=None  # TODO: implement custom Timetable` |

### execution_period Derivation
Derive `<execution_period>` suffix from the folder name:

| Folder name suffix | Period |
|--------------------|--------|
| `_DAILY` | `d` |
| `_WEEKLY` | `w` |
| `_MONTHLY` | `m` |
| `_YEARLY` or `_ANNUAL` | `y` |
| *(not found)* | `d` (default — emit `# TODO: verify period`) |


## Variable Substitution Reference

Translate Control-M date/variable expressions to Airflow Jinja templates:

| Control-M | Airflow Jinja |
|-----------|---------------|
| `%%$ODATE` / `%%ODATE` | `{{ ds_nodash }}` |
| `%%$YEAR.` | `{{ logical_date.strftime('%Y') }}` |
| `%%MONTH.` | `{{ logical_date.strftime('%m') }}` |
| `%%DAY.` | `{{ logical_date.strftime('%d') }}` |
| `%%PREV` (previous date) | `{{ (logical_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}` |
| `%%SUBSTR %%PREV 1 4` (year part) | `{{ (logical_date - macros.timedelta(days=1)).strftime('%Y') }}` |
| `%%SUBSTR %%PREV 5 2` (month part) | `{{ (logical_date - macros.timedelta(days=1)).strftime('%m') }}` |
| `%%SUBSTR %%PREV 7 2` (day part) | `{{ (logical_date - macros.timedelta(days=1)).strftime('%d') }}` |

> For any unrecognised `%%` expression, emit it as a `# TODO:` comment and use a placeholder string.


## Behavior
1. Parse the input XML and extract all Control-M folder and job metadata.
2. Group jobs by folder — each folder produces one DAG file.
3. For each folder:
   a. Derive DAG file name and DAG ID from `<company>-<app_id>-<app_code>-<folder_name>-<env>` (lowercased).
   b. Map the folder's schedule to an Airflow `schedule` parameter using the **Schedule Mapping** rules below; default timezone is "Asia/Bangkok".
   c. For each job in the folder, create one Airflow task:
      - Derive task ID from `<app_id>-<app_code>-task_<job_name>-<period>` (lowercased).
      - Derive `<period>` using the execution_period derivation rule (see Output section).
      - Select the operator based on job type (see Constraints).
      - Match job type (Appl_Type) with pattern `Control-M Appl_Type Mapping - Airflow Pattern Reference`
      - Apply SLA if defined on the job which is converted to `DeadlineAlert` (new standard in airflow 3.x).
      - Wire `on_failure_callback` to the standard alert hook.
      - Write comments 'control-m configuration' of the control-mjob to the task location.
      - Translate all Control-M `%%` variable expressions using the **Variable Substitution Reference**.
   d. Build task dependencies from Control-M job dependencies (`INCOND`/`OUTCOND`) using the **INCOND Resolution Algorithm** in the Dependency Mapping section.
   e. Add `ExternalTaskSensor` for any dependency referencing a job outside this folder.
4. Coding style refer to `Coding Style`
5. Write the generated DAG to a `.py` file.
6. Check syntax of any shell script or PowerShell embedded in `SSHOperator` or `PsrpOperator` — ensure backslashes are escaped and string delimiters are valid Python.
7. Verify the generated DAG by running `python <output_file>.py` inside the `.venv` (see Setup):
   - If it exits with code 0 → proceed to step 8.
   - If it fails → fix the error, re-run verification, repeat until clean.
   - **Do not deliver the file until `python <output_file>.py` exits with code 0. This step is a hard gate.**
8. For any unsupported job type, emit a `# TODO:` comment at the task location and log a warning.

### Schedule Mapping

| Control-M attribute | Value | Airflow schedule |
|---------------------|-------|-----------------|
| `FOLDER_ORDER_METHOD` | `SYSTEM` | scheduled — derive cron from `TIMEFROM` + folder name suffix |
| `FOLDER_ORDER_METHOD` | `""` (empty) | `None` (manually triggered) |
| `DAYS` | `ALL` + folder suffix `_DAILY` | `"<MM> <HH> * * *"` from `TIMEFROM` |
| `DAYS` | `ALL` + folder suffix `_WEEKLY` | `"<MM> <HH> * * 0"` (Sunday) |
| `DAYS` | `ALL` + folder suffix `_MONTHLY` | `"<MM> <HH> 1 * *"` (1st of month) |
| `CYCLIC=1` + `INTERVAL` | e.g. `00060M` | `"@hourly"` or derive cron from minutes |

> Default: if no schedule can be derived, use `schedule=None` and emit `# TODO: set schedule` comment.
> `TIMEFROM` format is `HHMM` — convert to cron as `MM HH * * *`.
> If the schedule involves non-standard dates (banking holidays, special calendars — indicated by `DAYSCAL` or `CONFCAL` attributes), use `schedule=None` and emit `# TODO: implement custom Timetable` instead of a cron string.


## Constraints & Assumptions
- One Control-M folder = one DAG file
- All Airflow operators/sensors must not be deprecated
- *Sensor* for priority selection => derferable mode -> reschedule -> poke
- Unsupported job types emit a `# TODO:` comment in the output and log a warning
- All identifiers lowercased
- Target: Airflow 3.x with classic operators (no Taskflow API)
- Generated DAG must pass `python <dag>.py` (inside `.venv`) with exit code 0 before it is considered complete — fix and re-verify in a loop until clean, never deliver a failing file


## Coding style

Follow the company DAG templates — see [`templates/`](templates/) for full working examples.

### File structure order
1. Imports (`pendulum`, `send_email`, `logging`)
2. Logging setup (`smtplib`, `airflow.utils.email` → DEBUG)
3. Variables zone — all config as module-level `_` prefixed variables
4. `success_callback` / `failure_callback` using `send_email` + `pendulum.now('Asia/Bangkok')`
5. `local_tz`, `default_args`, `dag = DAG(...)`
6. Tasks (grouped by section with `####` banners)
7. Dependencies

### Key rules
- Task variable names: lowercased, use `_` separator (Python), but `task_id` string uses `-` (refer to Naming convention table)
- `default_args` must include: `owner`, `depends_on_past`, `start_date`, `timezone`, `retries=3`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`
- DAG ID constructed as: `_company + '-' + _project + '-' + _dag_name + '-' + _env`
- Always set `is_paused_upon_creation=not _active`
- Callbacks wired via: `on_failure_callback=failure_callback if _enable_email_notification_fail else None`


## Setup

Create a virtual environment and install Airflow with the required providers before generating or verifying DAGs.

```bash
python -m venv .venv
source .venv/bin/activate
pip install apache-airflow \
    apache-airflow-providers-ssh \
    apache-airflow-providers-microsoft-psrp \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-amazon \
    pendulum
```

> This venv is used for DAG syntax verification (step 6 in Behavior). It does not need a running Airflow instance — import-level validation via `python <dag>.py` is sufficient.


## Reference
- [`controlm-schema.xsd`](controlm-schema.xsd) — official Control-M XML schema (DEFTABLE, FOLDER, SMART_FOLDER, JOB, INCOND, OUTCOND, VARIABLE, etc.)
- [`templates/`](templates/) — company Airflow DAG templates; read the relevant template when generating a task for a specific job type

> **When to consult the schema:** Only read this file when the input XML contains an unfamiliar element or attribute, when validating which child elements are valid inside a given container (e.g. `SMART_FOLDER` vs `FOLDER` vs `SUB_FOLDER`), or when resolving an edge case not covered by the Behavior or Constraints sections above. For standard `FOLDER`/`JOB` inputs, the schema is not needed.

> **When to consult the templates:** Read the matching template when generating a task operator — e.g. `templates/ssh-remote-unix/` for `OS`/Unix jobs, `templates/file-transfer-onprem-onprem-unix/` for `FILE_TRANS` Unix→Unix, `templates/psrp-operator/` for Windows jobs. Use the template's variable zone, callback, and `default_args` patterns as the baseline.


## Control-M Appl_Type / Job Type Mapping - Airflow Pattern Reference

### Connection ID Derivation
- `ssh_conn_id` = `"ssh_" + NODEID.lower()` (e.g. `NODEID="dunlop"` → `"ssh_dunlop"`)
- `psrp_conn_id` = `"psrp_" + NODEID.lower()` (e.g. `NODEID="winsrv01"` → `"psrp_winsrv01"`)
- `RUN_AS` maps to the remote username inside the connection config — document in `# comment`, not in code
- `aws_conn_id` = `"aws_" + AWS-ACCOUNT.lower()` (e.g. `AWS-ACCOUNT="nssctrlm"` → `"aws_nssctrlm"`)

### 1. APPL_TYPE = `FILE_TRANS`
- Check variables `FTP-*`
- L = Left (source), R = Right (destination)
- `FTP_<L|R>OSTYPE` is the OS type (e.g. `Windows`, `Unix`)
- Left OS = Unix/Linux → use `SSHOperator`; Left OS = Windows → use `PsrpOperator`
- Transfer command depends on Right Host type:

  | Right Host | Command |
  |------------|---------|
  | Windows/Unix/Linux | `lftp` |
  | Azure | `azcopy` |
  | AWS S3 (`FTP-CONNTYPE2=S3`) | `aws s3 cp` (see S3 specific rules below) |

- Flow: pre-command (if any) → transfer command → post-command (if any)

#### FILE_TRANS → S3 Specific Rules (`FTP-CONNTYPE2=S3`)
- Operator: `SSHOperator` on `FTP-LHOST` (local agent, e.g. `"dunlop"`)
- `ssh_conn_id` derived from `NODEID` (see Connection ID Derivation)
- Command: `aws s3 cp <FTP-LPATH1> s3://<FTP-S3_BUCKET_NAME><FTP-RPATH1><filename>`
- `FTP-S3_BUCKET_NAME` variable maps to the S3 bucket name
- `FTP-UPLOAD1=1` → upload (local → S3); `FTP-UPLOAD1=0` → download (S3 → local)
- `FTP-TYPE1=I` → binary (`--no-progress`); `FTP-TYPE1=A` → ASCII
- `FTP-TRANSFER_NUM` → number of transfer blocks (iterate `LPATH1/RPATH1`, `LPATH2/RPATH2`, …)

### 2. APPL_TYPE = `OS`
- Check variable `CMDLINE`
- Left OS = Unix/Linux → `SSHOperator` with bash command
- Left OS = Windows → `PsrpOperator` with PowerShell/batch command
- `ssh_conn_id` / `psrp_conn_id` derived from `NODEID` (see Connection ID Derivation)

### 3. APPL_TYPE = `FileWatch`
- Check variables `FileWatch-*`
- Use `FileSensor` from `airflow.providers.standard.sensors.filesystem`
- `mode='reschedule'`
- `timeout` = `TIME_LIMIT` (in seconds)
- `poke_interval` = `TIME_LIMIT / NUM_OF_ITERATIONS`
- `START_TIME` = aligns with DAG schedule

### 4. APPL_TYPE = `AWS`
- Check variables `AWS-*`
- If `SERVICE_TYPE=STEP` → use `StepFunctionStartExecutionOperator` + `StepFunctionExecutionSensor`
- `aws_conn_id` derived from `AWS-ACCOUNT` (see Connection ID Derivation)

#### AWS Step Function ARN Construction
```
arn:aws:states:<region>:<account_id>:stateMachine:<AWS-STEP_NAME>
```
- `region`: default `ap-southeast-1` (Bangkok)
- `account_id`: use `"ACCOUNT_ID_PLACEHOLDER"` — requires human to fill
- `execution name`: `AWS-STEP_EXECUTION_NAME + "-{{ ts_nodash }}"` (for uniqueness)
- `payload`: from `AWS-STEP_PAYLOAD_JSON-N001-VALUE` — unescape HTML entities (`&quot;` → `"`, `%4E` → `\n`)

```python
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor
import json

start_step = StepFunctionStartExecutionOperator(
    task_id="start_step_function",
    aws_conn_id="aws_nssctrlm",
    state_machine_arn=(
        "arn:aws:states:ap-southeast-1:"
        "ACCOUNT_ID_PLACEHOLDER:"
        "stateMachine:"
        "<AWS-STEP_NAME>"
    ),
    name="<AWS-STEP_EXECUTION_NAME>-{{ ts_nodash }}",
    input=json.dumps({ ... }),  # from AWS-STEP_PAYLOAD_JSON-N001-VALUE
)

wait_for_finish = StepFunctionExecutionSensor(
    task_id="wait_for_finish",
    aws_conn_id="aws_nssctrlm",
    execution_arn="{{ ti.xcom_pull(task_ids='start_step_function') }}",
    poke_interval=30,
    timeout=3600,
    mode="reschedule",
)

start_step >> wait_for_finish
```


## Dependency Mapping — INCOND/OUTCOND Pattern Reference

### 1. Condition String Format

#### Standard pattern

```
<JOB_NAME>-<STATUS>
```

The condition name equals the **source job's own name** plus a status suffix.  
This means: the predecessor emits its name as the outcond token; the successor waits on it as incond.

| Status suffix | Count | Notes |
|---------------|------:|-------|
| `ENDED-OK`    | 58,982 | Dominant — normal successful completion |
| `ENDED`       | 100   | Completion regardless of exit code |
| `END-OK`      | 37    | Legacy/typo variant of `ENDED-OK` |
| `ENED-OK`     | 2     | Typo variant |

#### Non-standard / custom tokens

| Variant | Count | Example | Meaning |
|---------|------:|---------|---------|
| `<NAME>X-ENDED-OK` | ~270 | `AFT_OFSAA_MANUAL_D_00010X-ENDED-OK` | Aliased name — condition references a *renamed* or *alternate* job token, not the actual FROM node name |
| `<NAME>-RERUN` | 5 | `RT_OBMS_FMS_D0010-RERUN` | Rerun-specific gate |
| `<NAME>-M2F` | ~10 | `RT_NEWMUREX_DTMREP_D0010-M2F` | Monday-to-Friday schedule variant |
| `<NAME>-SAT` | ~4 | `RT_NEWMUREX_DTMREP_D0025-SAT` | Saturday run variant |
| `<NAME>-SUN` | ~4 | `RT_NEWMUREX_DTMREP_D0015-SUN` | Sunday run variant |
| `<NAME>-SPECIFIC` | 1 | `RT_NEWMUREX_FRPT_EOD1_1850-SPECIFIC` | Special/manual run |
| `<NAME>-ENDED-OK-<NUM>` | ~3 | `RT_AFT_S1PTTRECCBSD005-ENDED-OK-969` | Numbered instance (cyclic job variant) |

**Key insight for migration:** Non-standard tokens cannot be auto-generated from the job name — they must be preserved verbatim.

---

### 2. AND / OR Gate Logic (`and_or` field)

| Value | Count | % | Semantics |
|-------|------:|---|-----------|
| `A`   | 58,823 | 99.4% | **AND** — all predecessor conditions must be satisfied |
| `O`   | 330   | 0.6% | **OR** — any one predecessor condition is sufficient |

AND is the default. When a job has multiple predecessors, assume AND unless `and_or = "O"` is explicit.

---

### 3. INCOND Resolution Algorithm

For each `INCOND` on a job, apply this decision tree:

1. Extract `JOB_NAME` from the condition string by stripping the known status suffix (`-ENDED-OK`, `-ENDED`, `-END-OK`, `-ENED-OK`, `-RERUN`, `-M2F`, `-SAT`, `-SUN`, `-SPECIFIC`, `-ENDED-OK-<N>`).
2. Is that `JOB_NAME` present in **this folder's** job list?
   - **YES** → wire as a task dependency: `predecessor_task >> this_task`
   - **NO** → add an `ExternalTaskSensor` pointing to the external DAG that owns that job (`external_dag_id` must be derived from the folder that contains that job)
3. If `AND_OR="O"` with multiple INCONDs → use `trigger_rule=TriggerRule.ONE_SUCCESS` instead of the default `ALL_SUCCESS`
