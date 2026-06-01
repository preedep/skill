Skill: Convert Control-M jobs to Apache Airflow DAGs

## Purpose
This skill converts Control-M jobs (XML) to Apache Airflow (version 3.x) DAGs. (DAGs python code)

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

> **Wildcard preservation:** After substituting `%%` variables in file paths, preserve any surrounding `*`/`?` wildcards verbatim — do not strip them. Example: `%%$ODATE..*` → `{{ ds_nodash }}*`.

### Airflow Date/Time Best Practices

Apply these principles consistently across all generated scripts (bash, PowerShell, Python):

* Use Airflow logical date semantics instead of current system time.
* Prefer `{{ ds }}` (YYYY-MM-DD) and `{{ ds_nodash }}` (YYYYMMDD) for date strings in file paths, filenames, and SQL parameters.
* Use `{{ logical_date.strftime('%Y') }}`, `{{ logical_date.strftime('%m') }}`, `{{ logical_date.strftime('%d') }}` when individual date parts are needed.
* **Never use bare `{{ logical_date }}`** in file paths or date strings — it renders as ISO datetime with timezone offset (e.g. `2026-05-28T00:00:00+07:00`), invalid in filenames.
* Treat Control-M `%%ODATE` / `%%$ODATE` as `{{ ds_nodash }}` (YYYYMMDD) — consistent with Variable Substitution Reference above.


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

1. **Parse and group:** Extract all Control-M folder and job metadata from input XML. Group jobs by folder — each folder produces one DAG file + one config JSON (Mode B only).

2. **Configure DAG:** For each folder:
   - Derive DAG file name and DAG ID from `<company>-<app_id>-<app_code>-<folder_name>-<env>` (lowercased)
   - Map folder schedule to Airflow `schedule` parameter using **Schedule Mapping** rules (default timezone: `Asia/Bangkok`)
   - Add DAG-level header comment documenting the Control-M source:
     ```python
     # Apache Airflow DAG: <dag_id>
     # Converted from Control-M folder: <folder_name>
     # Converted from Control-M job: <job_name> (use command if more than one job in folder)
     # Converted by Control-M 2 Airflow Skill
     ```
   - Add Task-level comment which use job configuration from Control-M job definition
   - Add Dependency comment which use job configuration from in/out condition Control-M job definition
   - Add Schedule comment which use job configuration from Control-M job definition 

3. **Build tasks:** For each job in the folder:
   - Derive task ID: `<app_id>-<app_code>-task_<job_name>-<period>` (lowercased)
   - Derive `<period>` from folder suffix (`_DAILY`→`d`, `_WEEKLY`→`w`, etc.; see execution_period Derivation)
   - Select operator based on job type using **Control-M Appl_Type Mapping** table
   - Apply SLA if defined → convert to `DeadlineAlert` (Airflow 3.x)
   - Wire `on_failure_callback` to standard alert hook
   - **Extract all Control-M variables** (`%%ODATE`, `%%PREV`, etc.) and translate to Airflow Jinja using **Variable Substitution Reference**; declare as **module-level variables** at the top of the DAG file (not hardcoded in task code)
   - Add task-level comment documenting Control-M source and conditions:
     ```python
     # Control-M job: <job_name>
     # INCOND: <condition> | OUTCOND: <condition>
     # RUN_AS: <username>
     ```

4. **Wire dependencies:** Build task dependencies from Control-M `INCOND`/`OUTCOND` using **INCOND Resolution Algorithm**:
   - If predecessor is in this folder → direct `>>` dependency
   - If predecessor is external → add `ExternalTaskSensor`
   - If `AND_OR="O"` with multiple conditions → use `trigger_rule=TriggerRule.ONE_SUCCESS`

5. **Syntax check:** Validate any embedded shell scripts (SSHOperator) or PowerShell (PsrpOperator):
   - Ensure backslashes are escaped correctly
   - Ensure string delimiters are valid Python
   - Use raw strings (`r"""..."""`) for Windows paths

6. **Write output:**
   - **Mode A:** Write plain DAG to `output/<dag_filename>.py`
   - **Mode B:** Write DAG template to `output/dags/<dag_filename>.py` with `##KEY##` placeholders AND config JSON to `output/config/<env>/<dag_filename>.json`

7. **Verify:** Run `python <output_file>.py` inside `.venv` (see Setup) to validate:
   - Python syntax (indentation, brackets, quotes)
   - All imports resolve correctly (airflow, providers, pendulum)
   - DAG instantiation succeeds (`dag = DAG(...)`)
   - No deprecated operators or syntax
   - Callback functions are valid
   - Raw strings (`r"""..."""`) for bash/PowerShell scripts are properly closed
   
   **Validation steps:**
   ```bash
   python <output_file>.py       # Check syntax and DAG instantiation
   echo $?                       # Exit code 0 = pass, ≠ 0 = fail
   ```
   - Exit code 0 → success; proceed to step 8
   - Exit code ≠ 0 → fix error, re-verify, repeat until clean
   - **Hard gate:** Do not deliver until verification passes

8. **Flag unsupported types:** For any job type not in **Control-M Appl_Type Mapping**, emit `# TODO: <job_type> not yet supported` comment and log a warning.

### Schedule Mapping

| Control-M attribute | Value | Airflow schedule |
|---------------------|-------|-----------------|
| `FOLDER_ORDER_METHOD` | `SYSTEM` | scheduled — derive cron from `TIMEFROM` + folder name suffix |
| `FOLDER_ORDER_METHOD` | `""` (empty) | `None` (manually triggered) |
| `DAYS` | `ALL` + folder suffix `_DAILY` | `"<MM> <HH> * * *"` from `TIMEFROM` |
| `DAYS` | `ALL` + folder suffix `_WEEKLY` | `"<MM> <HH> * * 0"` (Sunday) |
| `DAYS` | `ALL` + folder suffix `_MONTHLY` | `"<MM> <HH> 1 * *"` (1st of month) |
| `CYCLIC=1` + `INTERVAL` (any job in folder) | e.g. `00015M` | `timedelta(minutes=15)` — use `timedelta`, not cron |
| `CYCLIC=1` + `INTERVAL=00060M` | 60 minutes | `timedelta(hours=1)` |
| `CYCLIC=1` + `INTERVAL=00000M` | immediate (downstream job) | inherit schedule from the cyclic entry-point job; do not set a separate schedule |

**CYCLIC INTERVAL parsing:** format is `NNNNNu` where `u` = `M` (minutes) or `H` (hours).
- Extract numeric value: `00015M` → 15 minutes → `schedule=timedelta(minutes=15)`
- `00060M` → 60 min → `schedule=timedelta(hours=1)` (simplify when evenly divisible)
- `00000M` → 0 (downstream job, no independent schedule) → inherit from folder entry point
- `TIMEFROM` on a cyclic job sets the **first run time** — document as `start_date` with that time, not as a cron schedule
- `CYCLIC_TYPE=C` (completion-to-start) vs `CYCLIC_TYPE=S` (fixed interval) — document in DAG header comment; Airflow `timedelta` schedule approximates `CYCLIC_TYPE=S` (fixed); emit `# NOTE: original CYCLIC_TYPE=C (completion-to-start) — Airflow timedelta is fixed-interval` if type is C

> Default: if no schedule can be derived, use `schedule=None` and emit `# TODO: set schedule` comment.
> `TIMEFROM` format is `HHMM` — convert to cron as `MM HH * * *` for non-cyclic jobs.
> If the schedule involves non-standard dates (banking holidays, special calendars — indicated by `DAYSCAL` or `CONFCAL` attributes), use `schedule=None` and emit `# TODO: implement custom Timetable` instead of a cron string.


## Constraints & Assumptions

### Airflow Cluster Architecture
- **Airflow version:** 3.x
- **Executor:** KubernetesExecutor — each task runs in an isolated worker pod on Kubernetes
- **Worker pods have no access to on-premise file systems** — never transfer files through the Airflow worker pod
- All file transfers must execute on the **remote agent host** via `SSHOperator` or `PsrpOperator` — the operator runs a script on the remote host, which then performs the transfer locally on that host
- `FileSensor` is **never valid** for remote files — it only works for files on the Airflow worker pod's own local filesystem (which has no on-premise mounts)
- This reinforces the operator selection rules: always use `SSHOperator`/`PsrpOperator`/`SFTPSensor`/`S3KeySensor` to reach remote files, never assume the worker pod can access them directly

### General
- One Control-M folder = one DAG file
- All Airflow operators/sensors must not be deprecated
- *Sensor* for priority selection => deferrable mode → reschedule → poke
- Unsupported job types emit a `# TODO:` comment in the output and log a warning
- All identifiers lowercased
- Target: Airflow 3.x with classic operators (no Taskflow API)
- Generated DAG must pass `python <dag>.py` (inside `.venv`) with exit code 0 before it is considered complete — fix and re-verify in a loop until clean, never deliver a failing file


## Coding style

Follow the company DAG (focus on Airflow 3.x) templates — see [`templates/`](templates/) for full working examples.

### File structure order
1. **Header comment** — Control-M source documentation (DAG ID, folder, jobs, skill version)
2. Imports (`pendulum`, `send_email`, `logging`)
3. Logging setup (`smtplib`, `airflow.utils.email` → DEBUG)
4. Variables zone — all config as module-level `_` prefixed variables

```Example
_company = "##COMPANY##"
_project = "##PROJECT##"
_env = "##ENV##"
_dag_name = "##DAG_NAME##"
```

5. `success_callback` / `failure_callback` using `send_email` + `pendulum.now('Asia/Bangkok')`
6. `local_tz`, `default_args`, `dag = DAG(...)`
7. Tasks (grouped by section with `####` banners)
8. Dependencies

### Key rules
- **Imports:** only import operators/sensors that are actually used in the DAG. 
  - `EmptyOperator` → `from airflow.providers.standard.operators.empty import EmptyOperator` (Airflow 3.x) — never from `airflow.operators.empty` (deprecated)
  - `TriggerRule` → `from airflow.models.trigger_rule import TriggerRule` (Airflow 3.x) — **ONLY if** `AND_OR="O"` appears in any INCOND definition. Check all INCOND tags first; if none have `AND_OR="O"`, do NOT import. Never import from `airflow.utils.trigger_rule` (deprecated).
  - `send_email` → `from airflow.utils.email import send_email` — **ONLY if** callbacks are enabled. Place the import **inside** the `if` guard, not at the top of the callback or at module level:
    ```python
    def success_callback(context):
        if _enable_email_notification_success:
            from airflow.utils.email import send_email
            send_email(...)
    ```
    An import placed before the `if` guard fires on every callback invocation regardless of the flag — this is wrong.
  - Do not import operators/sensors unless a task uses them. Avoid importing unused symbols.
- **All inputs lowercased:** `company`, `app_id`, `app_code`, `folder_name`, `env`, all tag values, task IDs, Python variable names, and the DAG ID components must always be `.lower()` — regardless of how they are provided as input. Even if the user passes `APP_ID=APP1234`, store and emit it as `app1234`.
- **Task Python variable name:** `<app_id>_<app_code>_task_<job_name>_<period>` — all lowercase, `-` replaced with `_` (e.g. `app1234_testapp_task_rt_rb2cm005_d`). The `task_id` string uses `-` per the Naming convention table.
- `default_args` must include: `owner`, `depends_on_past`, `start_date`, `timezone`, `retries=3`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`, `email_on_failure=False`, `email_on_retry=False`
- DAG ID constructed as: `_company + '-' + _project + '-' + _app_code + '-' + _dag_name + '-' + _env`
- Always set `is_paused_upon_creation=not _active`
- DAG-level callbacks: `on_success_callback=success_callback if _enable_email_notification_success else None` and `on_failure_callback=failure_callback if _enable_email_notification_fail else None`
- **`dag=dag` is removed in Airflow 3.x** — do not pass `dag=dag` as a keyword argument to any operator or sensor. Declare all tasks inside a `with DAG(...) as dag:` context manager instead.
- **`# RUN_AS` comment:** always write the actual RUN_AS username from the Control-M job (e.g. `# RUN_AS: ctrlm`) — never use a placeholder like `# RUN_AS comment`.
- **Module-level variables — only declare what is actually referenced:** Only declare a `_`-prefixed module-level variable when it is referenced by the operator/sensor parameter directly (e.g. `ssh_conn_id=_ssh_conn_id`) or via Airflow Jinja in a templated field. Do NOT declare module-level path variables (`_lpath`, `_rpath`, etc.) when the value is only used inside a `command=r"""..."""` or `powershell=r"""..."""` raw string body — raw strings cannot reference Python variables. In those cases, embed the substituted value directly in the script string. Declaring a variable that is never referenced is dead code.

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

Follow the **Airflow Date/Time Best Practices** section (see above) for all date variable handling. For bash scripts specifically, avoid `date` / `$(date)` for business date calculations unless explicitly required.

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
* **FTP-SSL (`CONNTYPE2=FTP-SSL`):** use `ftps://` scheme and force SSL — never fall back to plain FTP:
  ```bash
  lftp -u "$RUSER","$RPASS" \
      -e "set ftp:ssl-allow yes; set ftp:ssl-force yes; \
          set ftp:passive-mode 1; \
          mput -O \"$RPATH\" $LPATH; quit" \
      "ftps://$RHOST"
  ```
  Using `ftp://` with only `set ftp:ssl-allow yes` allows a plain-FTP fallback — always add `set ftp:ssl-force yes` and use the `ftps://` scheme.

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
* All DAG files must be complete **profesional comments  (incl. header (before imports area) , task , dependencies)**
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
* **Windows OS `TASKTYPE=Command` (`.bat` / `.cmd`):** wrap via `cmd /c` inside `powershell=r"""..."""` — do NOT use a bare `command=` string. Always include `$ErrorActionPreference = 'Stop'` and check `$LASTEXITCODE`:
  ```python
  PsrpOperator(
      task_id='...',
      psrp_conn_id='psrp_<nodeid>',
      powershell=r"""
  $ErrorActionPreference = 'Stop'
  & cmd /c "F:\path\script.bat {{ ds_nodash }}"
  if ($LASTEXITCODE -ne 0) {
      Write-Host "[ERROR] Script failed: exit $LASTEXITCODE"
      exit $LASTEXITCODE
  }
  """,
      on_failure_callback=failure_callback,
  )
  ```

#### Script Format

Use raw multiline string to avoid backslash interpretation in Windows paths:

```python
powershell = r"""
...
"""
```

> Backslashes inside `r"""..."""` are literal — do not escape them further. Step 6's "ensure backslashes are escaped" rule does **not** apply inside an r-string; the r-prefix is the correct and sufficient form.

#### Airflow Scheduling Semantics

Follow the **Airflow Date/Time Best Practices** section (see above) for all date variable handling. For PowerShell scripts specifically, avoid `Get-Date` for business date calculations unless explicitly required.

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
* **Wildcard paths:** If `$LPath` or `$RPath` contains `*` or `?`, use `-Path` without quotes so PowerShell expands the glob — double-quoted strings suppress wildcard expansion:
  ```powershell
  # Wildcard upload — unquoted -Path
  Copy-Item -Path $LPath -Destination "\\$RHost\share\dest\" -Force
  # Wildcard download — unquoted -Path
  Copy-Item -Path "\\$RHost\share\$RPath" -Destination $LPath -Force
  ```

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
- Check variables `FTP-*` (see [[ctrlm_aft_variables]] for complete variable reference)
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

#### FILE_TRANS Command Generation

Extract `FTP-*` variables from the Control-M job XML. For each active transfer slot (N = 1 to `FTP-TRANSFER_NUM`):

1. **Check `FTP-UPLOAD{N}` value:**
   - `1` → Upload: local `FTP-LPATH{N}` → remote `FTP-RPATH{N}`
   - `0` → Download: remote `FTP-RPATH{N}` → local `FTP-LPATH{N}`
   - `3` → **File Watcher** — do NOT generate a transfer command. Instead generate a sensor/operator to wait for the file at `FTP-LPATH{N}` to appear, then proceed. Use this decision tree based on `NODEID` (look up OS from **Node ID Information** table):

     | NODEID OS | File path prefix | Sensor / Operator | Notes |
     |-----------|-----------------|-------------------|-------|
     | Unix/Linux | `/path/` (no wildcard) | `SFTPSensor` | `sftp_conn_id="ssh_<nodeid>"`, `path=FTP-LPATH{N}`, `mode='reschedule'`, `timeout=FTP-TIMELIMIT{N}×60`, `poke_interval=FTP-WATCH_INTERVAL{N}` |
     | Unix/Linux | `/path/*` or `?` (wildcard) | `SSHOperator` polling | SFTPSensor does not support globs — use `SSHOperator` with `ls` polling loop (see SFTPSensor Wildcard Restriction rule) |
     | AWS S3 (`FTP-CONNTYPE2=S3`) | `s3://` or S3 bucket path | `S3KeySensor` | `bucket_key=FTP-LPATH{N}`, `wildcard_match=True` if path contains `*`/`?` |
     | Windows (from table) | `D:\`, `S:\`, etc. | `PsrpOperator` polling | PowerShell `Test-Path` loop — same pattern as FileWatch Windows jobs (see Remote Windows FileWatch section) |

     After the sensor/operator completes (file found), the actual lftp/aws upload defined by `FTP-RPATH{N}` and `FTP-CONNTYPE2` follows as a **separate downstream task** wired with `>>`. Do not merge watch and transfer into a single task.

2. **Determine protocol from `FTP-CONNTYPE1` / `FTP-CONNTYPE2`:**
   - `LOCAL` + `SFTP/FTP/FTPS` → use `lftp` (Unix agent)
   - `LOCAL` + `S3` → use `aws s3 cp` (Unix agent)
   - `LOCAL` + `Azure` → use `azcopy` (Unix agent)
   - `Windows` source → use `PsrpOperator` with PowerShell `lftp` or native cmdlets

3. **Build the transfer command** (see templates below)

4. **Wrap with pre/post commands** (if `FTP-PRECOMM{N}` / `FTP-POSTCOMM{N}` exist):
   
   **Extraction Algorithm:**
   1. For each transfer N (1, 2, ...):
      - Check if `%%FTP-PRECOMM{HOST}{N}` exists (HOST=1 for source, 2 for destination)
      - For Windows source (LOSTYPE=Windows), extract `%%FTP-PRECOMM2{N}` (runs on destination before lftp)
      - For Unix source (LOSTYPE=Unix), extract `%%FTP-PRECOMM1{N}` (runs on source before lftp)
   2. Look up command parameters in `%%FTP-PREPARAM{HOST}{N}{Y}` (Y = parameter index: 1=first param, 2=second, etc.)
   3. Substitute Control-M variables in parameters: `%%$ODATE` → `{{ ds_nodash }}`, `%%PREV` → previous date logic
   4. **Prepend** pre-command script **before** the lftp/aws/azcopy command
   5. Same logic for post-commands: extract `%%FTP-POSTCOMM{N}` / `%%FTP-POSTPARAM{N}{Y}` and **append after** transfer
   
   **Concrete Example (mockup):**
   ```
   XML has:  %%FTP-PRECOMM21="mkdir"
             %%FTP-PREPARAM211="/mnt/data/output/%%$ODATE./folder"
   
   Extract:  Transfer 1, destination host (2), pre-command=mkdir, param1=/path/with/%%$ODATE
   Substitute: %%$ODATE → {{ ds_nodash }}
   Generate PowerShell:
   
   # Pre-command: mkdir (create destination directory)
   $Output = & lftp -u "$RUser","$RPass" \
       -e "mkdir /mnt/data/output/{{ ds_nodash }}/folder; quit" \
       "ftps://$RHost" 2>&1
   if ($LASTEXITCODE -ne 0) {
       Write-Host "[ERROR] mkdir failed: $Output"; exit 1
   }
   
   # Transfer 1: Upload
   $Output = & lftp -u "$RUser","$RPass" \
       -e "set ftp:ssl-allow yes; cd /mnt/data/output/{{ ds_nodash }}/folder; \
           put \"$LPath1\"; quit" \
       "ftps://$RHost" 2>&1
   ```
   
   **Critical:** Always include pre-commands and post-commands in the script flow; do NOT skip them even if they seem simple (mkdir, rm, etc.)

5. **Handle multiple transfers:** Build a bash/PowerShell loop if `FTP-TRANSFER_NUM > 1`

##### FILE_TRANS → Unix→Unix / Unix→Windows (lftp template)

For `FTP-CONNTYPE2 ∈ {FTP, FTPS, SFTP}`:

```bash
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Transfer failed at line $LINENO — exit code $?"' ERR

LPATH="{{ FTP-LPATH{N} }}"
RPATH="{{ FTP-RPATH{N} }}"
RHOST="{{ FTP-RHOST }}"
RUSER="{{ FTP-RUSER }}"
RPASS="{{ var.value['FTP_RPASS_SECRET'] }}"
PROTOCOL="{{ FTP-CONNTYPE2 }}"

echo "[INFO] Starting {{ FTP-UPLOAD{N}=1 ? 'upload' : 'download' }}"
echo "[INFO] Local: $LPATH"
echo "[INFO] Remote: $RHOST:$RPATH"

if [ "{{ FTP-UPLOAD{N} }}" = "1" ]; then
    # Upload: local → remote
    lftp -u "$RUSER","$RPASS" \
        -e "set ftp:ssl-allow {{ FTP-CONNTYPE2=FTPS ? 'yes' : 'no' }}; \
            set ftp:passive-mode {{ FTP-LPASSIVE }}; \
            cd $(dirname "$RPATH"); \
            put \"$LPATH\" -o \"$(basename \"$RPATH\")\"; \
            quit" \
        "$PROTOCOL://$RHOST"
else
    # Download: remote → local
    lftp -u "$RUSER","$RPASS" \
        -e "set ftp:ssl-allow {{ FTP-CONNTYPE2=FTPS ? 'yes' : 'no' }}; \
            set ftp:passive-mode {{ FTP-RPASSIVE }}; \
            get \"$RPATH\" -o \"$LPATH\"; \
            quit" \
        "$PROTOCOL://$RHOST"
fi

echo "[INFO] Transfer complete"
BASH
```

> **Mode:** `FTP-TYPE{N}=I` (binary) → no flags; `FTP-TYPE{N}=A` (ASCII) → add `-a` flag to `put`/`get`
> **Passive mode:** `FTP-LPASSIVE=1` → `set ftp:passive-mode 1`; `FTP-RPASSIVE=1` → same on remote side
> **Post-action:** If `FTP-SRCOPT{N}=1` (delete), append `rm "$LPATH"` after upload; if `FTP-DSTOPT{N}=1`, append `rm` on destination
> **Wildcard paths:** If `FTP-LPATH{N}` or `FTP-RPATH{N}` contains `*` or `?`, switch:
> - `put "$LPATH"` → `mput -O "$(dirname "$RPATH")" "$LPATH"`
> - `get "$RPATH"` → `mget -O "$LPATH" "$RPATH"`
> Do NOT use `get`/`put` with wildcard paths — lftp will not expand them.

##### FILE_TRANS → Unix→S3 (aws s3 cp template)

For `FTP-CONNTYPE2=S3`:

```bash
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] S3 transfer failed at line $LINENO — exit code $?"' ERR

LPATH="{{ FTP-LPATH{N} }}"
RPATH="{{ FTP-RPATH{N} }}"
S3_BUCKET="{{ FTP-S3_BUCKET_NAME }}"
S3_REGION="{{ FTP-S3_REGION | 'ap-southeast-1' }}"
AWS_PROFILE="{{ FTP-AWS_PROFILE | 'default' }}"

echo "[INFO] Starting {{ FTP-UPLOAD{N}=1 ? 'upload to S3' : 'download from S3' }}"
echo "[INFO] Bucket: s3://$S3_BUCKET/$RPATH"

if [ "{{ FTP-UPLOAD{N} }}" = "1" ]; then
    # Upload: local → S3
    aws s3 cp "$LPATH" "s3://$S3_BUCKET$RPATH" \
        --region "$S3_REGION" \
        --profile "$AWS_PROFILE" \
        {{ FTP-TYPE{N}=I ? '--no-progress' : '' }}
else
    # Download: S3 → local
    aws s3 cp "s3://$S3_BUCKET$RPATH" "$LPATH" \
        --region "$S3_REGION" \
        --profile "$AWS_PROFILE" \
        {{ FTP-TYPE{N}=I ? '--no-progress' : '' }}
fi

echo "[INFO] S3 transfer complete"
BASH
```

> **Credentials:** Use Airflow Connections or `~/.aws/credentials` (default profile)
> **Binary mode:** `FTP-TYPE{N}=I` → add `--no-progress`; `FTP-TYPE{N}=A` → omit
> **Wildcard paths:** If `FTP-LPATH{N}` or `FTP-RPATH{N}` contains `*` or `?`, use `aws s3 sync` instead of `aws s3 cp`:
> ```bash
> aws s3 sync "$(dirname "$LPATH")" "s3://$S3_BUCKET/$(dirname "$RPATH")/" \
>     --include "$(basename "$LPATH")" --exclude "*" \
>     --region "$S3_REGION" --profile "$AWS_PROFILE"
> ```
> `aws s3 cp` does not support wildcard expansion.

##### FILE_TRANS → Unix→Azure (azcopy template)

For `FTP-CONNTYPE2=Azure`:

```bash
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Azure transfer failed at line $LINENO — exit code $?"' ERR

LPATH="{{ FTP-LPATH{N} }}"
RPATH="{{ FTP-RPATH{N} }}"
STORAGE_ACCOUNT="{{ FTP-AZURE_STORAGE_ACCOUNT }}"
CONTAINER="{{ FTP-AZURE_CONTAINER }}"
SAS_TOKEN="{{ var.value['AZURE_SAS_TOKEN_SECRET'] }}"

DEST_URI="https://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER}${RPATH}?${SAS_TOKEN}"

echo "[INFO] Starting {{ FTP-UPLOAD{N}=1 ? 'upload to Azure' : 'download from Azure' }}"
echo "[INFO] Container: $STORAGE_ACCOUNT/$CONTAINER"

if [ "{{ FTP-UPLOAD{N} }}" = "1" ]; then
    # Upload: local → Azure
    azcopy copy "$LPATH" "$DEST_URI"
else
    # Download: Azure → local
    azcopy copy "$DEST_URI" "$LPATH"
fi

echo "[INFO] Azure transfer complete"
BASH
```

> **Auth:** SAS token stored in Airflow Variable, never hardcoded
> **Wildcard paths:** If `FTP-LPATH{N}` contains `*` or `?`, use `--include-pattern` instead of passing the glob directly:
> ```bash
> azcopy copy "$(dirname "$LPATH")/*" "$DEST_URI" --include-pattern "$(basename "$LPATH")"
> ```
> Quoted glob strings are not expanded by the shell — pass the pattern via `--include-pattern`.

##### FILE_TRANS → Windows source (PowerShell template)

For Windows source (`FTP-LOSTYPE=Windows`), use `PsrpOperator` with PowerShell:

```powershell
$ErrorActionPreference = 'Stop'

$LPath = "{{ FTP-LPATH{N} }}"
$RPath = "{{ FTP-RPATH{N} }}"
$RHost = "{{ FTP-RHOST }}"
$RUser = "{{ FTP-RUSER }}"
$RPass = "{{ var.value['FTP_RPASS_SECRET'] }}"

Write-Host "[INFO] Starting {{ FTP-UPLOAD{N}=1 ? 'upload' : 'download' }}"
Write-Host "[INFO] Local: $LPath"
Write-Host "[INFO] Remote: $RHost : $RPath"

# For SFTP/FTP via lftp (if installed on Windows)
# For native SMB, use Copy-Item with -Credential

if ("{{ FTP-CONNTYPE2 }}" -eq "SFTP" -or "{{ FTP-CONNTYPE2 }}" -eq "FTP") {
    $Credential = New-Object System.Management.Automation.PSCredential(
        $RUser,
        (ConvertTo-SecureString $RPass -AsPlainText -Force)
    )
    
    if ("{{ FTP-UPLOAD{N} }}" -eq "1") {
        # Upload: local → remote (e.g., via mapped SMB drive)
        Copy-Item "$LPath" "\\$RHost\$RPath" -Force
    } else {
        # Download: remote → local
        Copy-Item "\\$RHost\$RPath" "$LPath" -Force
    }
} else {
    Write-Host "[ERROR] Unsupported protocol on Windows: {{ FTP-CONNTYPE2 }}"
    exit 1
}

Write-Host "[INFO] Transfer complete"
```

#### FILE_TRANS → S3 Specific Rules (`FTP-CONNTYPE2=S3`)
- Operator: `SSHOperator` on `FTP-LHOST` (local agent, e.g. `"dunlop"`)
- `ssh_conn_id` derived from `NODEID` (see Connection ID Derivation)
- Command: use **aws s3 cp template** above
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

FileWatch monitors filesystem for file events (creation, deletion, modification) before allowing dependent tasks to proceed.

- Check variables `FileWatch-*` (see [[ctrlm_filewatch_variables]] for complete variable reference)
- Key parameters:
  - `FileWatch-FILE_PATH` — file path/pattern to monitor (supports wildcards: `*`, `?`)
  - `FileWatch-MODE` — `CREATE` or `DELETE`
  - `FileWatch-TIME_LIMIT` — max monitoring duration (minutes); convert to seconds for Airflow `timeout`
  - `FileWatch-INT_FILE_SEARCHES` — polling interval (seconds); use as `poke_interval`
  - `FileWatch-NUM_OF_ITERATIONS` + `FileWatch-INT_FILESIZE_COMPARISON` — file size stability checks
  - `FileWatch-MIN_DET_SIZE` — minimum file size (bytes); implement in custom sensor
  - `FileWatch-START_TIME` / `FileWatch-STOP_TIME` — time window constraints
  - `FileWatch-FILESIZE_WILDCARD` — enable size checks with wildcards (Y/N)

- Check `NODEID` against `Node ID Information` table to determine agent OS
- **Determine file location from `FileWatch-FILE_PATH`:**
  - Path starts with drive letter (`S:\`, `D:\`, `C:\`) → remote Windows file
  - Path starts with `/` → remote Unix/Linux file or local Unix
  - Relative path or local → Airflow worker local filesystem

- Select sensor/operator based on **FILE_PATH location + NODEID OS combination:**

| FILE_PATH Pattern | NODEID OS (from table) | File Location | Operator/Sensor | Notes |
|-------------------|----------------------|---------------|-----------------|-------|
| `S:\`, `D:\`, `C:\` (Windows drive) | Windows (from table) | Remote Windows SMB | `PsrpOperator` | PowerShell `Test-Path` polling |
| `/path/` (Unix path, no wildcard) | Unix/Linux (from table) | Remote Unix SFTP | `SFTPSensor` | `mode='reschedule'`, `sftp_conn_id="ssh_<nodeid>"` |
| `/path/*` or `?` (Unix wildcard) | Unix/Linux (from table) | Remote Unix | `SSHOperator` polling | SFTPSensor has no glob support — use `ls` loop |
| AWS S3 path | Any | S3 bucket | `S3KeySensor` | `wildcard_match=True` if path has `*`/`?` |

> **Critical Rule — FileSensor is NEVER valid for Control-M FileWatch migrations:**
> Under KubernetesExecutor, each task runs in an isolated worker pod with no on-premise filesystem mounts. A "local" or "relative" path in a Control-M FileWatch job refers to a path on the **on-premise agent host (NODEID)** — which is always remote from the worker pod.
> `FileSensor` is therefore **never a valid output** for any Control-M FileWatch job migration. Always use:
> - Windows NODEID → `PsrpOperator` (PowerShell `Test-Path` polling)
> - Unix/Linux NODEID, exact path → `SFTPSensor`
> - Unix/Linux NODEID, wildcard path → `SSHOperator` polling
> - S3 path → `S3KeySensor`

> **Critical Rule — SFTPSensor Wildcard Restriction:**
> `SFTPSensor.path` uses `SFTP.stat()` internally — exact path lookup only, no glob expansion. A wildcard path (`*`, `?`) will never match.
> When `FileWatch-FILE_PATH` contains `*` or `?` on a Unix/Linux NODEID, replace `SFTPSensor` with an `SSHOperator` polling script:
> ```python
> # TODO: SFTPSensor does not support wildcards — using SSHOperator polling instead
> task = SSHOperator(
>     task_id='<task_id>',
>     ssh_conn_id='ssh_<nodeid>',
>     command=r"""bash -s << 'BASH'
> set -euo pipefail
> FILE_PATTERN="<FileWatch-FILE_PATH with %% variables substituted>"
> TIMEOUT=<TIME_LIMIT × 60>
> POLL=<INT_FILE_SEARCHES>
> ELAPSED=0
> echo "[INFO] Waiting for: $FILE_PATTERN"
> while [ $ELAPSED -lt $TIMEOUT ]; do
>     if ls $FILE_PATTERN 2>/dev/null | grep -q .; then
>         echo "[INFO] File found: $(ls $FILE_PATTERN)"
>         exit 0
>     fi
>     echo "[INFO] Not yet found. Elapsed: ${ELAPSED}s / ${TIMEOUT}s"
>     sleep $POLL
>     ELAPSED=$((ELAPSED + POLL))
> done
> echo "[ERROR] File not found after ${TIMEOUT}s: $FILE_PATTERN"
> exit 1
> BASH""",
>     on_failure_callback=failure_callback,
> )
> ```
> Note: `ls $FILE_PATTERN` is intentionally unquoted so the shell expands the glob.

> **Critical Rule — S3KeySensor Wildcard:**
> When `FileWatch-FILE_PATH` (after `%%` substitution) contains `*` or `?`, always set `wildcard_match=True`. Without it, S3KeySensor performs an exact key lookup and wildcards silently never match:
> ```python
> S3KeySensor(
>     task_id='<task_id>',
>     bucket_name='<bucket>',
>     bucket_key='<prefix>/{{ ds_nodash }}*.txt',
>     wildcard_match=True,   # REQUIRED when path contains * or ?
>     aws_conn_id='aws_<account>',
>     poke_interval=<INT_FILE_SEARCHES>,
>     timeout=<TIME_LIMIT × 60>,
>     mode='reschedule',
>     on_failure_callback=failure_callback,
> )
> ```

- All sensors: use `mode='reschedule'` (deferrable preferred if provider supports it, then reschedule, then poke)
- Parameter mapping:
  - `timeout` = `FileWatch-TIME_LIMIT` (minutes) × 60 (seconds)
  - `poke_interval` = `FileWatch-INT_FILE_SEARCHES` (already in seconds)
  - Stability window = `FileWatch-NUM_OF_ITERATIONS` × `FileWatch-INT_FILESIZE_COMPARISON`
  - `START_TIME` = align DAG `schedule` with this time window (if specified)

#### Remote Windows FileWatch — PsrpOperator polling placeholder

When NODEID maps to Windows OS (refer to `Node ID Information` table) and FILE_PATH is a Windows drive path (`S:\`, `D:\`, etc.), generate a `PsrpOperator` task that polls for the file and exits 0 when found:

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
$TimeoutSec = <TIME_LIMIT × 60>
$PollSec = <INT_FILE_SEARCHES>
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

##### Concrete Example: Windows FileWatch with PsrpOperator (mockup)

Given:
- NODEID = `serverwin1` (Windows, from Node ID Information table)
- FILE_PATH = `S:\data\processed\report_%%$ODATE..txt`
- TIME_LIMIT = 5 minutes (300 seconds)
- INT_FILE_SEARCHES = 60 seconds

**Generated PsrpOperator task:**

```python
# Control-M job: MONITOR_DATA_001 | NODEID: serverwin1 | RUN_AS: datauser
# FILE_PATH: S:\data\processed\report_{{ ds_nodash }}..txt
# TIME_LIMIT: 5 minutes (300 seconds) | INT_FILE_SEARCHES: 60 seconds
app1234_testapp_task_monitor_data_001_d = PsrpOperator(
    task_id='app1234-testapp-task_monitor_data_001-d',
    psrp_conn_id='psrp_serverwin1',  # NODEID=serverwin1 → psrp_serverwin1
    # RUN_AS: datauser
    powershell=r"""
$ErrorActionPreference = 'Stop'
$FilePath = "S:\data\processed\report_{{ ds_nodash }}..txt"
$TimeoutSec = 300  # 5 minutes
$PollSec = 60  # INT_FILE_SEARCHES
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

**Key implementation points:**
- ✅ NODEID=serverwin1 (Windows) → **Use `PsrpOperator` NOT `FileSensor`**
- ✅ FILE_PATH starts with `S:\` (Windows drive) → Remote Windows file requires PsrpOperator
- ✅ `psrp_conn_id='psrp_serverwin1'` derived from NODEID.lower()
- ✅ `%%$ODATE` replaced with `{{ ds_nodash }}`
- ✅ `TIME_LIMIT=5` (minutes) → 300 seconds
- ✅ `INT_FILE_SEARCHES=60` (seconds) → $PollSec
- ✅ PowerShell with `$ErrorActionPreference = 'Stop'` for error handling

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

<appid>_<appcode>_task_<jobname>_<period> = StepFunctionStartExecutionOperator(
    task_id="<appid>-<appcode>-task_<jobname>-<period>",
    aws_conn_id="aws_<aws_account>",
    state_machine_arn=(
        "arn:aws:states:ap-southeast-1:"
        "ACCOUNT_ID_PLACEHOLDER:"
        "stateMachine:"
        "<AWS-STEP_NAME>"
    ),
    name="<AWS-STEP_EXECUTION_NAME>-{{ ts_nodash }}",
    state_machine_input='{ ... }',  # from AWS-STEP_PAYLOAD_JSON-N001-VALUE — unescape &quot;→" and %4E→\n
    on_failure_callback=failure_callback,
)

<appid>_<appcode>_task_<jobname>_wait_<period> = StepFunctionExecutionSensor(
    task_id="<appid>-<appcode>-task_<jobname>_wait-<period>",
    aws_conn_id="aws_<aws_account>",
    execution_arn="{{ ti.xcom_pull(task_ids='<appid>-<appcode>-task_<jobname>-<period>') }}",
    poke_interval=30,
    timeout=3600,
    mode="reschedule",
    on_failure_callback=failure_callback,
)

<appid>_<appcode>_task_<jobname>_<period> >> <appid>_<appcode>_task_<jobname>_wait_<period>
```

> **`state_machine_input`** (not `input`) is the correct parameter for `StepFunctionStartExecutionOperator`. Pass the payload as a plain string — Airflow renders Jinja inside it at task execution time. Do NOT use `json.dumps()` with Jinja expressions — `json.dumps()` runs at DAG parse time and produces a literal string containing the Jinja template tags, which is correct here, but it adds unnecessary complexity and escaping risk. Build the payload string directly.
> **`import json`** is not needed — omit it unless other code in the DAG uses it.


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
| `obms-ir-prod` | `Windows` |
| `ultrasone` | `Linux` |
