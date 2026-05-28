Skill: Convert Control-M jobs to Apache Airflow DAGs

## Purpose
This skill converts Control-M jobs (XML) to Apache Airflow DAGs. (DAGs python code)

## Input
The input is a Control-M job XML file and an optional config JSON file.

## Output

**Mode A — No config JSON provided (default):**
Output is a single plain Python DAG file with all values hardcoded. Do NOT generate a config JSON file. Do NOT use `##KEY##` placeholders.

**Mode B — Config JSON file provided as input:**
Output is a DAG template Python file with `##KEY##` placeholders for every env-specific value, plus the corresponding config JSON file. **Never hardcode env-specific values in the DAG template in this mode.**

> The agent must not generate a config JSON or use `##KEY##` placeholders unless a config JSON file is explicitly provided as input. The Config JSON Schema section below is reference material for Mode B only.


| Pattern | Description |Example|
|---------|-------------|------|
| Naming convention DAG file | DAG file follow  `<company>-<app_id>-<app_code>-<folder_name>-<env>.py` for dag_name is existing folder name of job control  use small case (eg. acme-AP1234-pyment-abc-monthly-tab-dev.py) | `acme-AP1234-payment-abc-monthly-tab-dev.py` |
| Naming convention DAG ID | same as filename without `.py` | `acme-AP1234-payment-abc-monthly-tab-dev` |
| Naming convention (task) | Task IDs follow `<app_id>-<app_code>-task_<task_name>-<execution_period>` for task_name is existing job name of job control  use small case (eg. AP1234-pyment-task_rt-rb2cm005-d) , for execution_period use small case (eg.  d=daily, w=weekly, m=monthly,y=yearly) | `AP1234-pyment-task_rt-rb2cm005-d` |
| Naming convention (task Python variable) | Python variable name for each task follows `<app_id>_<app_code>_task_<task_name>_<execution_period>` (all lowercase, `-` replaced with `_`) | `ap1234_payment_task_rt_rb2cm005_d` |
| Control-M Folder | Smart Folder / Control-M Folder replacement with DAG level |   |
| Control-M Job | Control-M Job replacement with DAG task |  |
| Control-M Job Type | Control-M Job Type replacement with Operator or Sensor |  |
| Control-M Job dependencies | >> operator is used to define task dependencies which focus in same folder| `task_1 >> task_2` |
| ODATE Replacement | ODATE values are replaced with `{{ ds_nodash }}` (YYYYMMDD). Use `{{ ds }}` (YYYY-MM-DD) when a hyphenated date is needed. Never use bare `{{ logical_date }}` — it renders as an ISO datetime with timezone offset, not a date string. | `{{ ds_nodash }}` |
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


### Config JSON Schema

#### Common fields (all templates)

| Field | Type | Description | Derived from |
|-------|------|-------------|-------------|
| `COMPANY` | string | Company prefix | CLI `--company` argument |
| `PROJECT` | string | App ID | CLI `--app_id` argument |
| `APP_CODE` | string | App code | CLI `--app_code` argument |
| `DAG_NAME` | string | Folder name (lowercase) | Control-M `FOLDER_NAME` |
| `ENV` | string | Environment (`dev`/`sit`/`uat`/`prod`) | CLI `--env` argument |
| `ACTIVE` | bool | DAG active on deploy | `true` by default |
| `SCHEDULE` | string | Cron expression | Derived from Schedule Mapping |
| `TAGS` | array[string] | Airflow UI tags — all lowercase | `[company, app_id, app_code, dag_name, env]` |
| `EMAIL_LIST` | array[string] | Alert recipients | `[]` by default |
| `ENABLE_EMAIL_NOTIFICATION_SUCCESS` | bool | Email on DAG success | `false` by default |
| `ENABLE_EMAIL_NOTIFICATION_FAIL` | bool | Email on task failure | `false` by default |

#### Operator-specific fields

| Template | Extra config fields |
|----------|-------------------|
| **ssh-remote-unix** | `SSH_CONN_ID`, `REMOTE_HOST` |
| **psrp-operator** | `PSRP_CONN_ID`, `REMOTE_HOST` |
| **file-transfer-onprem-onprem-unix** | `SSH_CONN_ID`, `REMOTE_HOST`, `SOURCE_PATH`, `DESTINATION_HOST`, `DESTINATION_PATH`, `DESTINATION_USER`, `DESTINATION_PORT`, `DESTINATION_PROTOCOL`, `DESTINATION_PASSWORD_SECRET` |
| **file-transfer-onprem-onprem-windows** | `PSRP_CONN_ID`, `REMOTE_HOST`, `SOURCE_PATH`, `DESTINATION_HOST`, `DESTINATION_PATH`, `DESTINATION_USER`, `DESTINATION_PORT`, `DESTINATION_PROTOCOL`, `DESTINATION_PASSWORD_SECRET` |
| **file-transfer-onprem-blob** | `SSH_CONN_ID`, `REMOTE_HOST`, `SOURCE_HOST`, `SOURCE_PATH`, `SOURCE_USER`, `SOURCE_PORT`, `SOURCE_PROTOCOL`, `SOURCE_PASSWORD_SECRET`, `AZURE_STORAGE_ACCOUNT`, `AZURE_CONTAINER`, `AZURE_BLOB_PREFIX`, `AZURE_IDENTITY_CLIENT_ID` |
| **file-transfer-blob-blob** | `SSH_CONN_ID`, `REMOTE_HOST`, `SOURCE_STORAGE_ACCOUNT`, `SOURCE_CONTAINER`, `SOURCE_BLOB_PREFIX`, `DEST_STORAGE_ACCOUNT`, `DEST_CONTAINER`, `DEST_BLOB_PREFIX`, `IDENTITY_CLIENT_ID` |
| **file-transfer-onprem-s3** | `SSH_CONN_ID`, `REMOTE_HOST`, `SOURCE_HOST`, `SOURCE_PATH`, `SOURCE_USER`, `SOURCE_PORT`, `SOURCE_PROTOCOL`, `SOURCE_PASSWORD_SECRET`, `S3_BUCKET`, `S3_PREFIX`, `S3_REGION`, `S3_ENDPOINT_URL`, `AWS_PROFILE` |
| **file-transfer-s3-s3** | `SSH_CONN_ID`, `REMOTE_HOST`, `SOURCE_S3_BUCKET`, `SOURCE_S3_PREFIX`, `SOURCE_S3_REGION`, `SOURCE_S3_ENDPOINT_URL`, `DEST_S3_BUCKET`, `DEST_S3_PREFIX`, `DEST_S3_REGION`, `DEST_S3_ENDPOINT_URL`, `AWS_PROFILE` |
| **kube-pod-operator** | `CLUSTER_CONFIG_ENV`, `NAMESPACE`, `IMAGE`, `SERVICE_ACCOUNT`, `NODE_SELECTOR`, `LABELS`, `RESOURCE` |
| **kube-job-operator** | same as kube-pod-operator + `JOB_TTL_SECONDS` |

#### Key conventions

- **`##KEY##` placeholders:** every env-specific value in the DAG template must use `##KEY##` syntax (e.g. `_ssh_conn_id = "##SSH_CONN_ID##"`). The pipeline does a literal string replace — no Jinja, no Python eval.
- **`*_PASSWORD_SECRET`:** stores the **secret name/key** (e.g. Airflow Variable name or K8s secret key), never the actual password value.
- **`{DATE}` in paths:** a template placeholder in config values replaced at runtime with `{{ ds_nodash }}` in the DAG — document this in comments.
- **Connection ID naming:** `SSH_CONN_ID` = `ssh_<host>_<env>`, `PSRP_CONN_ID` = `psrp_<host>_<env>` — derive from Control-M `NODEID` + env.
- **Config file naming:** `<dag_filename>.json` — same base name as the DAG template file, no `.py`.

#### Template selection guide

| Control-M job type | Left OS | Right host | Template |
|--------------------|---------|------------|----------|
| `OS` | Unix/Linux | — | `ssh-remote-unix` |
| `OS` | Windows | — | `psrp-operator` |
| `FILE_TRANS` | Unix | Unix/Linux | `file-transfer-onprem-onprem-unix` |
| `FILE_TRANS` | Windows | Unix/Linux | `file-transfer-onprem-onprem-windows` |
| `FILE_TRANS` | Unix | Azure Blob | `file-transfer-onprem-blob` |
| `FILE_TRANS` | Unix | AWS S3 | `file-transfer-onprem-s3` |
| `FILE_TRANS` | Azure | Azure | `file-transfer-blob-blob` |
| `FILE_TRANS` | S3 | S3 | `file-transfer-s3-s3` |


## Behavior
1. Parse the input XML and extract all Control-M folder and job metadata.
2. Group jobs by folder — each folder produces one DAG file + one config JSON.
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
      - Write comments 'control-m configuration and conditions INCOND/OUTCOND' of the control-mjob to the task location and task dependencies.
      - Translate all Control-M `%%` variable expressions using the **Variable Substitution Reference**.
   d. Build task dependencies from Control-M job dependencies (`INCOND`/`OUTCOND`) using the **INCOND Resolution Algorithm** in the Dependency Mapping section.
   e. Add `ExternalTaskSensor` for any dependency referencing a job outside this folder.
4. Coding style refer to `Coding Style`
5. Write output files:
   - **Mode A (no config JSON input):** write plain DAG to `output/<dag_filename>.py` only.
   - **Mode B (config JSON input provided):** write DAG template with `##KEY##` placeholders to `output/dags/<dag_filename>.py` AND config JSON to `output/config/<env>/<dag_filename>.json`.
7. Check syntax of any shell script or PowerShell embedded in `SSHOperator` or `PsrpOperator` — ensure backslashes are escaped and string delimiters are valid Python.
8. Verify the generated DAG by running `python <output_file>.py` inside the `.venv` (see Setup):
   - If it exits with code 0 → proceed to step 9.
   - If it fails → fix the error, re-run verification, repeat until clean.
   - **Do not deliver the file until `python <output_file>.py` exits with code 0. This step is a hard gate.**
9. For any unsupported job type, emit a `# TODO:` comment at the task location and log a warning.

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
- **Imports:** only import operators/sensors that are actually used in the DAG. `EmptyOperator` must be imported from `airflow.providers.standard.operators.empty` (Airflow 3.x) — never from `airflow.operators.empty` (deprecated). Do not import `EmptyOperator` unless a task uses it.
- **All inputs lowercased:** `company`, `app_id`, `app_code`, `folder_name`, `env`, all tag values, task IDs, Python variable names, and the DAG ID components must always be `.lower()` — regardless of how they are provided as input. Even if the user passes `APP_ID=APP1234`, store and emit it as `app1234`.
- **Task Python variable name:** `<app_id>_<app_code>_task_<job_name>_<period>` — all lowercase, `-` replaced with `_` (e.g. `app1234_testapp_task_rt_rb2cm005_d`). The `task_id` string uses `-` per the Naming convention table.
- `default_args` must include: `owner`, `depends_on_past`, `start_date`, `timezone`, `retries=3`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`, `email_on_failure=False`, `email_on_retry=False`
- DAG ID constructed as: `_company + '-' + _project + '-' + _app_code + '-' + _dag_name + '-' + _env`
- Always set `is_paused_upon_creation=not _active`
- DAG-level callbacks: `on_success_callback=success_callback if _enable_email_notification_success else None` and `on_failure_callback=failure_callback if _enable_email_notification_fail else None`
- **`dag=dag` is removed in Airflow 3.x** — do not pass `dag=dag` as a keyword argument to any operator or sensor. Declare all tasks inside a `with DAG(...) as dag:` context manager instead.
- **`# RUN_AS` comment:** always write the actual RUN_AS username from the Control-M job (e.g. `# RUN_AS: ctrlm`) — never use a placeholder like `# RUN_AS comment`.

### Shell Script Guidelines

Use for shell scripts embedded in SSHOperator tasks.

#### General Rules

* Generate production-ready shell scripts.
* Use Bash-compatible syntax unless otherwise specified.
* Scripts must be deterministic and rerun-safe.
* Avoid interactive commands.
* Avoid commands requiring TTY input.

#### Script Format

Embed shell scripts as Python raw triple-quoted strings. **Do not rely on a shebang line** — SSHOperator passes the script content to the remote shell's stdin/exec; a `#!/usr/bin/env bash` line is treated as a comment and does not select the interpreter. To guarantee bash execution, wrap the entire script body with `bash -s` or use a heredoc invocation:

```python
command=r"""bash -s << 'BASH'
set -euo pipefail
# ... script body ...
BASH"""
```

> Backslashes inside `r"""..."""` are literal — do not escape them further.

#### Airflow Scheduling Semantics

* Use Airflow logical date semantics instead of current system time.
* Prefer `{{ ds }}` (YYYY-MM-DD) and `{{ ds_nodash }}` (YYYYMMDD) for date strings in file paths, filenames, and SQL parameters.
* Use `{{ logical_date.strftime('%Y') }}`, `{{ logical_date.strftime('%m') }}`, `{{ logical_date.strftime('%d') }}` when individual date parts are needed.
* **Never use bare `{{ logical_date }}` in file paths or date strings** — it renders as an ISO datetime with timezone offset (e.g. `2026-05-28T00:00:00+07:00`) which contains colons and is invalid in filenames on most systems.
* Do not use `date`, `$(date)`, or runtime timestamps for business date calculations unless explicitly required.

#### Error Handling

Use `set -euo pipefail` at the top of every script. Scripts must invoke bash explicitly to guarantee pipefail support — wrap the entire script body with `bash -s` or use a heredoc invocation (see Script Format above). Do not rely on the remote user's default login shell.

**Important:** with `set -euo pipefail` active, a failed command causes immediate script exit — any `if [ $? -ne 0 ]` check placed *after* the command is **never reached**. Use a `trap` to emit failure logs instead:

```bash
set -euo pipefail

trap 'echo "[ERROR] Script failed at line $LINENO — exit code $?"' ERR

echo "[INFO] Starting ..."
# ... commands ...
echo "[INFO] Completed successfully"
```

The `trap ... ERR` fires on any command failure and logs the line number and exit code before the script exits. Do not use `if [ $? -ne 0 ]` after commands when `set -e` is active.

* Validate critical commands explicitly.
* Exit non-zero on failures.
* Avoid silent failures.

#### Logging

Use clear logging:

```bash
echo "[INFO] ..."
echo "[ERROR] ..."
```

Log: start, logical date, source/destination, completion, failure reason.

#### File Operations

* Validate file existence before transfer or processing.
* Quote paths safely: `"$FILE_PATH"`

#### FTP / SFTP / FTPS

* Prefer non-interactive commands.
* For `lftp`, use `set ssl:verify-certificate no` only when explicitly required by legacy systems.
* Validate transfer results.

#### SSHOperator Compatibility

* Scripts must run correctly inside SSHOperator.
* Avoid environment assumptions.
* Use absolute paths whenever possible.

#### Security

* Never hardcode passwords.
* Use Airflow Variables or Connections.
* Avoid printing secrets to logs.
* **Do not use `set cmd:verbose true` in `lftp` commands** when a password is passed in the connection string — verbose mode logs the full command including the password. Omit or replace with `set cmd:verbose false`.
* Passwords retrieved via `{{ var.value['...'] }}` Jinja will appear in the Airflow "Rendered Template" task log — emit a `# WARNING: password visible in Airflow rendered template log` comment so operators are aware.

#### Output Requirements

* Produce complete runnable scripts.
* Do not generate pseudocode — every `lftp`, `aws`, or command block must be fully written out with all options; never use `...` as a placeholder.
* Do not omit required variables.
* Keep scripts enterprise-readable and maintainable.

### PowerShell Guidelines

Use for PowerShell scripts embedded in PsrpOperator tasks.

#### General Rules

* Generate production-ready PowerShell.
* Use PowerShell-compatible syntax.
* Scripts must be deterministic and rerun-safe.
* Avoid interactive prompts.
* Avoid GUI-dependent commands.

#### Script Format

Use raw multiline string to avoid backslash interpretation in Windows paths:

```python
powershell = r"""
...
"""
```

> Backslashes inside `r"""..."""` are literal — do not escape them further. Step 6's "ensure backslashes are escaped" rule does **not** apply inside an r-string; the r-prefix is the correct and sufficient form.

#### Airflow Scheduling Semantics

* Use Airflow logical date semantics instead of runtime system time.
* Prefer `{{ ds }}` (YYYY-MM-DD) and `{{ ds_nodash }}` (YYYYMMDD) for date strings in file paths, filenames, and SQL parameters.
* Use `{{ logical_date.strftime('%Y') }}`, `{{ logical_date.strftime('%m') }}` etc. when individual date parts are needed.
* **Never use bare `{{ logical_date }}` in file paths or date strings** — it renders as an ISO datetime with timezone offset (e.g. `2026-05-28T00:00:00+07:00`) which is invalid in Windows filenames.
* Do not use `Get-Date` for business date calculations unless explicitly required.

#### Control-M Migration Rules

* Treat Control-M `%%ODATE` / `%%$ODATE` as `{{ ds_nodash }}` (YYYYMMDD) — consistent with the Variable Substitution Reference table. Do **not** substitute ODATE with bare `{{ logical_date }}`.
* Ensure rerun/backfill behavior remains deterministic.
* Preserve original scheduling semantics where possible.

#### Error Handling

Set `$ErrorActionPreference = 'Stop'` at the top of every script so that PowerShell cmdlet failures (non-terminating errors from `Copy-Item`, `Invoke-WebRequest`, etc.) are promoted to terminating exceptions. Then validate external executable exit codes:

```powershell
$ErrorActionPreference = 'Stop'

# ... script body ...

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: ..."
    exit 1
}
```

Do not allow silent failures.

#### Logging

Use `Write-Host "..."`. Log: start, logical date, source/destination, completion, failure reason.

#### File Operations

* Validate file existence before transfer or processing.
* Use properly quoted Windows paths: `"D:\path\file.txt"`

#### FTP / SFTP / FTPS

* Prefer `lftp` for FTP, SFTP, and FTPS transfers — it supports all three protocols (`ftp://`, `sftp://`, `ftps://` schemes).
* For SFTP: `lftp -e "mirror/get/put ...; quit" sftp://host`
* Use `set ssl:verify-certificate no` only for legacy environments when required.
* Validate every transfer result.

#### PsrpOperator Compatibility

* Scripts must run correctly inside PsrpOperator.
* Avoid assumptions about user profiles or session persistence.
* Use absolute paths whenever possible.

#### Security

* Never hardcode credentials.
* Use Airflow Variables or Connections.
* Do not print secrets in logs.
* **Do not use `set cmd:verbose true` in `lftp` commands** when a password is interpolated in the connection string — verbose mode logs the full command including credentials. Omit or use `set cmd:verbose false`.
* Passwords retrieved via `{{ var.value['...'] }}` Jinja will appear in the Airflow "Rendered Template" task log — emit a `# WARNING: password visible in Airflow rendered template log` comment so operators are aware.

#### Output Requirements

* Produce complete runnable PowerShell scripts.
* Do not generate pseudocode — every `lftp`, command block, or transfer sequence must be fully written out with all options; never use `...` as a placeholder.
* Do not omit required variables.
* Keep scripts enterprise-readable and maintainable.


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
- Check `NODEID` to determine remote host OS (refer to Node ID Information table)
- Select sensor/operator based on NODEID OS and protocol:

| NODEID OS | Protocol | Approach | Provider package |
|-----------|----------|----------|-----------------|
| Airflow worker local | — | `FileSensor` | `apache-airflow-providers-standard` |
| Unix/Linux (e.g. Dunlop, Donut) | SFTP | `SFTPSensor` | `apache-airflow-providers-sftp` |
| Unix/Linux | FTPS | No native sensor — use `SSHOperator` with `lftp ls` polling loop; emit `# TODO: implement FTPS file watch` | — |
| Windows (e.g. Glory) | SMB/mapped drive | No native sensor — `FileSensor` cannot reach a remote Windows path (e.g. `S:\`). Emit `# TODO: implement Windows remote file watch` and generate a `PsrpOperator` polling script as placeholder | — |
| AWS S3 | S3 | `S3KeySensor` | `apache-airflow-providers-amazon` |

> **`FileSensor` only works for files on the Airflow worker's own local filesystem.** Never use `FileSensor` for a path on a remote Windows server (e.g. `S:\`, `D:\`) or a remote Unix host — the worker cannot see those paths.

- All sensors: use `mode='reschedule'` (deferrable preferred if provider supports it, then reschedule, then poke)
- `timeout` = `TIME_LIMIT` converted to seconds
- `poke_interval` = `TIME_LIMIT / NUM_OF_ITERATIONS`
- `START_TIME` = aligns with DAG schedule

#### Windows FileWatch — PsrpOperator polling placeholder

When NODEID is Windows, generate a `PsrpOperator` task that polls for the file and exits 0 when found:

```python
# TODO: implement Windows remote file watch
# Replace this PsrpOperator polling placeholder with a proper custom sensor when available.
# This task polls every <poke_interval>s up to <timeout>s for the file to appear.
<appid>_<appcode>_task_<jobname>_<period> = PsrpOperator(
    task_id='<appid>-<appcode>-task_<jobname>-<period>',
    psrp_conn_id='psrp_<nodeid>',
    # RUN_AS: <run_as>
    powershell=r"""
$ErrorActionPreference = 'Stop'
$FilePath = "<FileWatch-FILE_PATH with %%ODATE replaced by {{ ds_nodash }}>"
$TimeoutSec = <TIME_LIMIT in seconds>
$PollSec = <TIME_LIMIT / NUM_OF_ITERATIONS>
$Elapsed = 0
Write-Host "[INFO] Waiting for file: $FilePath"
while (-not (Test-Path $FilePath)) {
    if ($Elapsed -ge $TimeoutSec) {
        Write-Host "[ERROR] File not found after ${TimeoutSec}s: $FilePath"
        exit 1
    }
    Write-Host "[INFO] File not yet present. Elapsed: ${Elapsed}s / ${TimeoutSec}s"
    Start-Sleep -Seconds $PollSec
    $Elapsed += $PollSec
}
Write-Host "[INFO] File found: $FilePath"
""",
    wsman_options={"ssl": False},
    on_failure_callback=failure_callback,
)
```

### 4. APPL_TYPE = `AWS`
- Check variables `AWS-*`
- If `SERVICE_TYPE=STEP` → use `StepFunctionStartExecutionOperator` + `StepFunctionExecutionSensor`
- `aws_conn_id` derived from `AWS-ACCOUNT` (see Connection ID Derivation)

#### AWS Step Function ARN Construction
```
arn:aws:states:<region>:<account_id>:stateMachine:<AWS-STEP_NAME>
```
- `region`: default `ap-southeast-1` (Singapore)
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

## Node ID Information
| Node ID | OS |
|---------|----------------|
| `Glory` | `Windows` |
| `Dunlop` | `Linux` |
| `Donut` | `Linux` |
