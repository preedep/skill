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
| Control-M Job dependencies | >> operator is used to define task dependencies | `task_1 >> task_2` |
| ODATE Replacement | ODATE values are replaced with Airflow 'logical date' | `{{ logical_date}}` or `{{ ds_nodash }}` or `{{ ds }}` |
| Error alert | uses email_on_failure attriute which default is false (add all DAGs) | `email_on_failure=False`|
| Retry alert | uses email_on_retry attriute which default is false (add all DAGs) | `email_on_retry=False` |
| SLA Alert | uses DeadlineAlert of Airflow 3.x  | `DeadlineAlert` |


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


## Behavior
1. Parse the input XML and extract all Control-M folder and job metadata.
2. Group jobs by folder — each folder produces one DAG file.
3. For each folder:
   a. Derive DAG file name and DAG ID from `<company>-<app_id>-<app_code>-<folder_name>-<env>` (lowercased).
   b. Map the folder's schedule to an Airflow `schedule` parameter and default timezone is "Asia/Bangkok".
   c. For each job in the folder, create one Airflow task:
      - Derive task ID from `<app_id>-<app_code>-task_<job_name>-<period>` (lowercased).
      - Select the operator based on job type (see Constraints).
      - Match job type (Appl_Type) with pattern `Control-M Appl_Type Mapping - Airflow Pattern Reference`  
      - Apply SLA if defined on the job which is converted to `DeadlineAlert` (new standard in airflow 3.x).
      - Wire `on_failure_callback` to the standard alert hook.
      - Write comments 'control-m configuration' of the control-mjob to the task location.
   d. Build task dependencies from Control-M job dependencies (`INCOND`/`OUTCOND`) which refer to reference `Dependency Mapping — INCOND/OUTCOND Pattern Reference` find job dependencies in the same folder.
   e. Add `ExternalTaskSensor` for any dependency referencing a job outside this folder.
4. Coding stype refer to `Coding Style`
5. Write the generated DAG to a `.py` file.
6. For any unsupported job type, emit a `# TODO:` comment at the task location and log a warning.


## Constraints & Assumptions
- One Control-M folder = one DAG file
- *Sensor* for priority selection => derferable mode -> reschedule -> poke
- Unsupported job types emit a `# TODO:` comment in the output and log a warning
- All identifiers lowercased
- Target: Airflow 3.x with classic operators (no Taskflow API)

## Reference
- [`controlm-schema.xsd`](controlm-schema.xsd) — official Control-M XML schema (DEFTABLE, FOLDER, SMART_FOLDER, JOB, INCOND, OUTCOND, VARIABLE, etc.)
- [`templates/`](templates/) — company Airflow DAG templates; read the relevant template when generating a task for a specific job type

> **When to consult the schema:** Only read this file when the input XML contains an unfamiliar element or attribute, when validating which child elements are valid inside a given container (e.g. `SMART_FOLDER` vs `FOLDER` vs `SUB_FOLDER`), or when resolving an edge case not covered by the Behavior or Constraints sections above. For standard `FOLDER`/`JOB` inputs, the schema is not needed.

> **When to consult the templates:** Read the matching template when generating a task operator — e.g. `templates/ssh-remote-unix/` for `OS`/Unix jobs, `templates/file-transfer-onprem-onprem-unix/` for `FILE_TRANS` Unix→Unix, `templates/psrp-operator/` for Windows jobs. Use the template's variable zone, callback, and `default_args` patterns as the baseline.

## Control-M Appl_Type / Job Type Mapping - Airflow Pattern Reference
L = Left , R = Right (Left is the source, Right is the target)
1. APPL_TYPE = `FILE_TRANS`
  - Check variables FTP_*
  - FTP_<L or R>OSTYPE is Operating System type (ex. Windows , Unix)
  - if Operating System is Unix/Linux then use SSHOperator  to remote to Left Host and that script 
      - run pre-command if need which run via `bash` operator
      - run the comand file transfer which depend on Right Host via `bash` operator
        | Right Host: | Command |
        | ----------- | ------ |
        | Windows/Unix/Linux | lftp|
        | Azure |azcopy |
        | AWS |aws s3 cp/sync |
      - run post-command if need which run via `bash` operator    
2. APPL_TYPE = `OS`
  - Check variable CMDLINE 



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
