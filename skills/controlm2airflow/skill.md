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

   #### 2a. Multi-schedule split

   Before configuring a single DAG, check whether jobs in the folder have different `TIMEFROM` values:

   **Step 1 — Resolve each job's effective schedule:**
   - Job-level `TIMEFROM` takes precedence over folder-level `TIMEFROM`
   - If a job has no `TIMEFROM`, it inherits the folder-level value
   - Group jobs by their effective `TIMEFROM`
   - If all jobs share the same `TIMEFROM` → single DAG (standard flow, continue below)

   **Step 2 — Classify dependencies:**
   - **Same-group:** both jobs share the same `TIMEFROM` → stays as direct `>>` in the same DAG
   - **Cross-group:** jobs belong to different `TIMEFROM` groups → becomes `ExternalTaskSensor`

   **Step 3 — Generate one DAG per schedule group:**
   - Naming: append `_<HHMM>` from the group's `TIMEFROM` to the folder name:
     `<company>-<app_id>-<app_code>-<folder_name>_<HHMM>-<env>.py`
   - Add to each DAG header:
     ```python
     # NOTE: split from Control-M folder <FOLDER_NAME> — schedule group <HHMM>
     # Other schedule groups: <HHMM1>, <HHMM2>, ...
     ```

   **Step 4 — Wire cross-group dependencies:**
   - In the downstream DAG, add an `ExternalTaskSensor` before the dependent task:
     ```python
     wait_for_job_b = ExternalTaskSensor(
         task_id="wait_<job_b_task_id>",
         external_dag_id="<company>-<app_id>-<app_code>-<folder_name>_<HHMM>-<env>",
         external_task_id="<job_b_task_id>",
         mode="reschedule",
         timeout=86400,
     )
     wait_for_job_b >> task_c
     ```
   - The upstream DAG needs no change — task success satisfies the OUTCOND.

   **Examples:**

   Case A — Independent groups (no cross-group deps):
   ```
   Folder: AFT_ERP_DAILY (folder TIMEFROM=0800)
     Job A  TIMEFROM=0800  no INCOND
     Job B  TIMEFROM=0800  INCOND: A-ENDED-OK
     Job C  TIMEFROM=2200  no INCOND  (job-level override)

   → DAG 1 scb-ap1001-erp-aft_erp_daily_0800-dev:  task_a >> task_b
   → DAG 2 scb-ap1001-erp-aft_erp_daily_2200-dev:  task_c  (standalone)
   ```

   Case B — Cross-group dependency:
   ```
   Folder: AFT_ERP_DAILY (folder TIMEFROM=0800)
     Job A  TIMEFROM=0800  no INCOND
     Job B  TIMEFROM=0800  INCOND: A-ENDED-OK
     Job C  TIMEFROM=2200  INCOND: B-ENDED-OK  ← cross-group

   → DAG 1 scb-ap1001-erp-aft_erp_daily_0800-dev:
       task_a >> task_b
   → DAG 2 scb-ap1001-erp-aft_erp_daily_2200-dev:
       wait_for_task_b (ExternalTaskSensor → DAG 1 / task_b) >> task_c
   ```

   > **Edge cases:** Multiple cross-group deps on one task → add one `ExternalTaskSensor` per predecessor, then `[wait_a, wait_b] >> task_c`. Circular cross-group dependency → split as above, emit `# TODO: circular cross-schedule dependency detected — verify execution order` and `schedule=None` on the ambiguous DAG.

   > **Rule:** same-group deps → `>>`. Cross-group deps → `ExternalTaskSensor`. Never drop a dependency because jobs run at different times.

   #### 2b. Single DAG configuration

   - Derive DAG file name and DAG ID from `<company>-<app_id>-<app_code>-<folder_name>-<env>` (lowercased)
   - Map folder schedule to Airflow `schedule` parameter using **Schedule Mapping** rules (default timezone: `Asia/Bangkok`)
   - Add DAG-level header comment documenting the Control-M source:
     ```python
     # Apache Airflow DAG: <dag_id>
     # Converted from Control-M folder: <folder_name>
     # Converted from Control-M job: <job_name> (use command if more than one job in folder)
     # Converted by Control-M 2 Airflow Skill (Developed by NiX)
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
   - **Extract all Control-M variables** (`%%ODATE`, `%%PREV`, etc.) and translate to Airflow Jinja using **Variable Substitution Reference**; declare every extracted value as a `_`-prefixed **module-level (global) variable** in the variables zone. Shell scripts and PowerShell scripts must reference these globals by injecting them via an f-string (`f"""..."""`) — **not** a raw string (`r"""..."""`) — so Python interpolates the values at DAG load time. Example:
     ```python
     # variables zone
     _lpath = "/data/input/file_{{ ds_nodash }}.txt"
     _rhost = "dunlop"

     # task
     command=f"""bash -s << 'BASH'
     set -euo pipefail
     LPATH="{_lpath}"
     RHOST="{_rhost}"
     BASH"""
     ```
     > Windows paths in f-strings: use forward slashes or double-backslashes (`\\`) since f-strings interpret backslashes — e.g. `_lpath = "D:\\app\\file.txt"` or `_lpath = "D:/app/file.txt"`.
   - Add task-level comment documenting Control-M source and conditions:
     ```python
     # Control-M job: <job_name>
     # INCOND: <condition> | OUTCOND: <condition>
     # RUN_AS: <username>
     ```

4. **Wire dependencies:** Build task dependencies from Control-M `INCOND`/`OUTCOND` using **INCOND Resolution Algorithm**:
   - If predecessor is in this folder → direct `>>` dependency
   - If predecessor is in a **different folder, same application** (`app_id` + `app_code` match) → `ExternalTaskSensor`
   - If predecessor is in a **different application** (cross-app) → **do NOT use `ExternalTaskSensor`**. Cross-app dependencies must be resolved via file-based signalling at deployment time. Emit the following instead:
     ```python
     # TODO: cross-app dependency — RT_XXX_ENDED-OK originates from a different application.
     # ExternalTaskSensor is not allowed across applications.
     # Implement file-based trigger: upstream app writes a sentinel file; this task polls for it
     # using SFTPSensor or a custom FileSensor before proceeding.
     # Sentinel path (suggested): /airflow/signals/<upstream_app>/<condition_name>.done
     ```
   - If `AND_OR="O"` with multiple conditions → use `trigger_rule=TriggerRule.ONE_SUCCESS`

   > **How to identify cross-app:** If the INCOND name contains a different `app_id` or belongs to a Control-M folder from a different application team, treat it as cross-app. When in doubt, flag as cross-app and emit the TODO — it is safer to under-wire than to create hidden cross-app coupling.

5. **Syntax check:** Validate any embedded shell scripts (SSHOperator) or PowerShell (PsrpOperator):
   - Ensure backslashes are escaped correctly in f-strings (double `\\` for Windows paths)
   - Ensure string delimiters are valid Python
   - Use f-strings (`f"""..."""`) so module-level globals are interpolated into scripts

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
   pyflakes <output_file>.py     # Lint: unused imports, undefined names
   echo $?                       # Exit code 0 = pass, ≠ 0 = fix and re-run
   ```
   - Both commands must exit 0 before the file is considered complete
   - Exit code ≠ 0 on either → fix error, re-verify, repeat until clean
   - **Hard gate:** Do not deliver until both checks pass

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
2. Imports (`logging`, `pendulum`, Airflow operators/sensors) — **all** non-smtplib imports go here, in this order:
   ```python
   import logging
   import pendulum
   from airflow import DAG
   from airflow.providers.ssh.operators.ssh import SSHOperator
   # ... other operators/sensors as needed
   ```
   > **Hard gate:** Do NOT place any `from airflow import ...` or `from airflow.providers...` import after the logging banner. All Airflow imports must appear in step 2, before the banner. If you find yourself writing an Airflow import after the `###################### logging ######################` line, stop and move it up to step 2.
3. Logging setup — **always include both lines**, separated by a `###################### logging ######################` banner:
   ```python
   ###################### logging ######################
   import smtplib
   logging.getLogger("smtplib").setLevel(logging.DEBUG)
   logging.getLogger("airflow.utils.email").setLevel(logging.DEBUG)
   ```
   **Critical:** `import smtplib` must appear **inside** this logging section (immediately after the `####` banner), never in the top-level imports block (step 2). Do NOT move it up with the other imports — its placement here is intentional and required.
   > **Violation to avoid:** Do NOT write `import smtplib` in step 2 (imports block). If you find yourself writing `import smtplib` before the logging banner, stop and move it here.
   > **Hard gate before writing any DAG file:** Scan your generated output for `import smtplib`. It must appear exactly once, on the line immediately after the `###################### logging ######################` banner. If it is absent, or appears anywhere else (top-level imports, inside a function, after the variables zone), do NOT write the file — fix the placement first and re-check.
   > **pyflakes suppression:** `smtplib` is imported for its side-effect (enabling debug logging) and is never called directly. pyflakes will flag it as unused. Always add `_ = smtplib` on the line immediately after `logging.getLogger("airflow.utils.email").setLevel(logging.DEBUG)` to suppress this warning without using `# noqa`.

   **Canonical logging block — always use this exact block, adjusting only the operator lines that apply:**
   ```python
   ###################### logging ######################
   import smtplib
   logging.getLogger("smtplib").setLevel(logging.DEBUG)
   logging.getLogger("airflow.utils.email").setLevel(logging.DEBUG)
   _ = smtplib
   # SSHOperator — include when DAG contains SSHOperator tasks
   logging.getLogger("airflow.providers.ssh.operators.ssh").setLevel(logging.DEBUG)
   logging.getLogger("airflow.providers.ssh.hooks.ssh").setLevel(logging.DEBUG)
   # PsrpOperator — include when DAG contains PsrpOperator tasks
   logging.getLogger("airflow.providers.microsoft.psrp.operators.psrp").setLevel(logging.DEBUG)
   logging.getLogger("airflow.providers.microsoft.psrp.hooks.psrp").setLevel(logging.DEBUG)
   ```

   **Which lines to include:**
   - Always include the `smtplib` + `airflow.utils.email` lines (every DAG).
   - Add the `ssh` lines only if the DAG contains at least one `SSHOperator` task.
   - Add the `psrp` lines only if the DAG contains at least one `PsrpOperator` task.
   - Never add both sets if only one operator type is used — unused logger lines are noise.

   **What each logger exposes in the Airflow task log:**

   | Logger | What it logs |
   |---|---|
   | `airflow.providers.ssh.operators.ssh` | Command dispatch, exit code, timeout events |
   | `airflow.providers.ssh.hooks.ssh` | SSH connection negotiation, channel open/close, key exchange |
   | `airflow.providers.microsoft.psrp.operators.psrp` | PowerShell command dispatch, runspace state, exit code |
   | `airflow.providers.microsoft.psrp.hooks.psrp` | WinRM connection setup, PSRP protocol messages, auth |
4. Variables zone — all config as module-level `_` prefixed variables, preceded by a `###################### variables zone ######################` banner:

```Example
###################### variables zone ######################
_company  = "##COMPANY##"
_project  = "##PROJECT##"
_app_code = "##APP_CODE##"
_dag_name = "##DAG_NAME##"
_env      = "##ENV##"
_active   = False
_schedule = "30 22 * * *"  # TIMEFROM=2230; DAYS=ALL — always declare as _schedule, never hardcode in DAG()
```
   > **`_schedule` must always be a module-level variable.** Never pass a literal string directly to `schedule=` inside `DAG(...)`. Derive the value from `TIMEFROM` + folder suffix and assign it to `_schedule` first, then reference it: `schedule=_schedule`. This applies to `timedelta` schedules too: `_schedule = timedelta(minutes=15)`.

5. `success_callback` / `failure_callback` — **always use this canonical template**:
   ```python
   def success_callback(context):
       dag_id = context['dag'].dag_id
       task_id = context['task_instance'].task_id
       execution_date = pendulum.now('Asia/Bangkok')

       subject = f"DAG {dag_id} - Task {task_id} succeeded"
       body = f"""
       <h3>DAG Task Succeeded</h3>
       <p><strong>DAG:</strong> {dag_id}</p>
       <p><strong>Task:</strong> {task_id}</p>
       <p><strong>Execution Time:</strong> {execution_date}</p>
       """

       if _enable_email_notification_success:
           from airflow.utils.email import send_email
           send_email(to=_email_list, subject=subject, html_content=body)


   def failure_callback(context):
       dag_id = context['dag'].dag_id
       task_id = context['task_instance'].task_id
       execution_date = pendulum.now('Asia/Bangkok')
       exception = context.get('exception', 'Unknown error')

       subject = f"DAG {dag_id} - Task {task_id} failed"
       body = f"""
       <h3>DAG Task Failed</h3>
       <p><strong>DAG:</strong> {dag_id}</p>
       <p><strong>Task:</strong> {task_id}</p>
       <p><strong>Execution Time:</strong> {execution_date}</p>
       <p><strong>Error:</strong> {exception}</p>
       """

       if _enable_email_notification_fail:
           from airflow.utils.email import send_email
           send_email(to=_email_list, subject=subject, html_content=body)
   ```
   **Rules:**
   - Always extract `dag_id`, `task_id`, `execution_date` from `context` — never hardcode the DAG name.
   - Always capture `exception = context.get('exception', 'Unknown error')` in `failure_callback` and include it in the body.
   - `send_email` import must be **inside** the `if` guard, not at the top of the function.
   - The HTML body uses `<h3>` + `<p><strong>` structure.
   - For FILE_TRANS DAGs, append job-relevant context variables to the body (e.g. `_rhost`, `_lpath`, `_rpath`) so the recipient can identify the transfer without opening Airflow.
   - Never include secret variables (`_rpass_secret`, passwords) in the email body.

6. `local_tz`, `default_args`, `dag = DAG(...)`
7. Tasks (grouped by section with `####` banners)
8. Dependencies

### Key rules
- **Imports:** only import operators/sensors that are actually used in the DAG. **Before writing any import, verify that at least one task in the generated DAG actually instantiates that operator/sensor class.** If no task uses it, do not import it — unused imports are a code smell and must not appear in generated output.
  - `EmptyOperator` → `from airflow.providers.standard.operators.empty import EmptyOperator` (Airflow 3.x) — never from `airflow.operators.empty` (deprecated). **Only import if the DAG contains at least one `EmptyOperator(...)` task instance.**
  > **Self-check before writing imports:** List every operator/sensor class you will instantiate. Only import those classes. If you find no `EmptyOperator(...)` call in your task list, do NOT add the `EmptyOperator` import.
  - `ExternalTaskSensor` → `from airflow.providers.standard.sensors.external_task import ExternalTaskSensor` (Airflow 3.x) — never from `airflow.sensors.external_task` (deprecated).
  - `TriggerRule` → `from airflow.task.trigger_rule import TriggerRule` (Airflow 3.x) — **ONLY if** `AND_OR="O"` appears in any INCOND definition. Check all INCOND tags first; if none have `AND_OR="O"`, do NOT import. Never import from `airflow.utils.trigger_rule` (deprecated — redirects to `airflow.task.trigger_rule` with a warning) or `airflow.models.trigger_rule` (does not exist in Airflow 3.x).
  - `send_email` → `from airflow.utils.email import send_email` — **ONLY if** callbacks are enabled. Place the import **inside** the `if` guard at the bottom of each callback (see canonical template in step 5). An import placed before the `if` guard fires on every callback invocation regardless of the flag — this is wrong.
  - Do not import operators/sensors unless a task uses them. Avoid importing unused symbols.
- **All inputs lowercased:** `company`, `app_id`, `app_code`, `folder_name`, `env`, all tag values, task IDs, Python variable names, and the DAG ID components must always be `.lower()` — regardless of how they are provided as input. Even if the user passes `APP_ID=APP1234`, store and emit it as `app1234`. This includes `_project` in the variables zone — it must always be the lowercased `app_id` value (e.g. `_project = "ap1002"`, never `"AP1002"`).
- **Task Python variable name:** `<app_id>_<app_code>_task_<job_name>_<period>` — all lowercase, `-` replaced with `_` (e.g. `app1234_testapp_task_rt_rb2cm005_d`). The `task_id` string uses `-` per the Naming convention table.
- `default_args` must include: `owner`, `depends_on_past`, `start_date`, `timezone`, `retries=3`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`, `email_on_failure=False`, `email_on_retry=False`
  - **`"owner"` must be `_company`** — never `_project`. The owner field identifies the team, not the application.
  - **`"timezone"` must be a plain string** — never pass `local_tz` (a `pendulum.Timezone` object). Airflow 3.x Pydantic serialization cannot handle the object type and raises `PydanticSerializationError`. Always write: `"timezone": "Asia/Bangkok"`. Use `local_tz` only for `pendulum.datetime(...)` calls in `start_date`.
  - **`"start_date"` must use `pendulum.datetime(..., tz=local_tz)`** — never `datetime(...)` from the standard library. Do not import `from datetime import datetime, timedelta`; use `pendulum.datetime()` and `pendulum.duration()` throughout.
  - **`"retry_delay"` and `"max_retry_delay"` must use `pendulum.duration(...)`** — never `timedelta(...)`.
- **`_tags` must reference variables** — always `_tags = [_company, _project, _app_code, _dag_name, _env]`. Never hardcode strings like `["nix", "apxxxx", ...]` — they silently diverge if a variable is changed.
- DAG ID constructed as: `_company + '-' + _project + '-' + _app_code + '-' + _dag_name + '-' + _env`
- **Default paused:** `_active = False` — all generated DAGs must be paused on creation by default. Always set `is_paused_upon_creation=not _active` (evaluates to `True` when `_active=False`). Never set `_active = True` in generated output — activation is a manual deployment step.
- DAG-level callbacks: `on_success_callback=success_callback if _enable_email_notification_success else None` and `on_failure_callback=failure_callback if _enable_email_notification_fail else None`
- **`dag=dag` is removed in Airflow 3.x** — do not pass `dag=dag` as a keyword argument to any operator or sensor. Declare all tasks inside a `with DAG(...) as dag:` context manager instead.
- **`SSHOperator` and `PsrpOperator` must always include `cmd_timeout` and `conn_timeout`:**
  ```python
  SSHOperator(
      ...,
      cmd_timeout=1800,   # seconds — kill remote command if it runs longer than 30 min
      conn_timeout=60,    # seconds — fail fast if SSH connection cannot be established
  )
  PsrpOperator(
      ...,
      cmd_timeout=1800,
      conn_timeout=60,
  )
  ```
  Never omit these — without them the operator inherits provider defaults which may be indefinite, causing silent hangs that block the worker slot.
- **`# RUN_AS` comment:** always write the actual RUN_AS username from the Control-M job (e.g. `# RUN_AS: ctrlm`) — never use a placeholder like `# RUN_AS comment`.
- **Module-level variables — declare all extracted Control-M values as globals:** Every path, host, user, and translated `%%` variable extracted from Control-M must be declared as a `_`-prefixed module-level variable in the variables zone. Scripts reference these globals by using an f-string (`f"""..."""`) for the `command=` or `powershell=` argument — never embed values directly in the script body. For Windows paths inside f-strings use double-backslashes (`\\`) or forward slashes to avoid backslash interpretation.
- **f-string vs plain string in the variables zone:** Use a **plain string** (not f-string) for any module-level variable whose value contains only Airflow Jinja templates (`{{ ds_nodash }}`, `{{ logical_date.strftime(...) }}`, etc.) with no Python globals being interpolated. An f-string with no `{python_expr}` placeholders is flagged by pyflakes as `f-string is missing placeholders`. The rule is:
  - Variable declaration with Jinja only → plain string: `_rpath = "/data/{{ ds_nodash }}/*.txt"`
  - `command=` / `powershell=` argument that injects Python globals into the script body → f-string: `command=f"""... LPATH="{_lpath}" ..."""`
  > **Self-check:** Before writing any `f"..."` in the variables zone, ask: "Am I interpolating a Python `_variable` here?" If the answer is no — only Airflow `{{ }}` tokens — use a plain string.

### Script Guidelines (Shell + PowerShell)

Rules apply to both `SSHOperator` (bash) and `PsrpOperator` (PowerShell) tasks. Language-specific differences are noted inline.

#### General Rules

* Generate production-ready, deterministic, rerun-safe scripts.
* No interactive commands, TTY input, or GUI-dependent commands.
* Use absolute paths wherever possible.
* Avoid environment assumptions.
* Produce complete runnable scripts with professional comments (header, task, dependencies). Never use `...` as a placeholder.

#### Script Format

| | Bash (SSHOperator) | PowerShell (PsrpOperator) |
|---|---|---|
| String type | `f"""..."""` (f-string) | `f"""..."""` (f-string, unless no globals) |
| Wrapping | `bash -s << 'BASH' ... BASH` — do NOT rely on shebang; SSHOperator passes to stdin | `powershell=f"""..."""` directly |
| Globals in script | `LPATH="{_lpath}"` — Python interpolates at DAG load | `$LPath = "{_lpath}"` |
| Windows paths in f-string | N/A | Use double-backslashes `"D:\\\\app\\\\file.txt"` or forward slashes |
| `.bat`/`.cmd` on Windows | N/A | Wrap with `cmd /c` inside `powershell=r"""..."""`; check `$LASTEXITCODE` |

Bash example:
```python
command=f"""bash -s << 'BASH'
set -euo pipefail
LPATH="{_lpath}"
RHOST="{_rhost}"
# ... script body ...
BASH"""
```

PowerShell example:
```python
powershell=f"""
$ErrorActionPreference = 'Stop'
$LPath = "{_lpath}"
$RHost = "{_rhost}"
# ... script body ...
"""
```

#### Error Handling

| | Bash | PowerShell |
|---|---|---|
| Stop on error | `set -euo pipefail` | `$ErrorActionPreference = 'Stop'` |
| Failure log | `trap 'echo "[ERROR] failed at line $LINENO — exit $?"' ERR` | `if ($LASTEXITCODE -ne 0) { Write-Host "[ERROR]..."; exit 1 }` |
| Note | Do NOT use `if [ $? -ne 0 ]` after commands when `set -e` is active — it is never reached | PowerShell only promotes terminating exceptions with `'Stop'`; external exe codes must be checked manually |

#### Logging

Every script must emit structured log lines covering the full execution lifecycle. **Never print secret values** — mask passwords and tokens before logging.

##### Required log points (both Bash and PowerShell)

| Stage | What to log |
|---|---|
| **Start** | Job name, logical date (`{{ ds_nodash }}`), source path, destination host+path, operator (upload/download/watch) |
| **Pre-command** | Command name and parameters (no secrets) |
| **Transfer** | File count / file names being transferred |
| **Post-command** | Command name and parameters (no secrets) |
| **Completion** | "DONE" + job name + logical date |
| **Error** | "ERROR" + failed step + line number (Bash) or error message (PowerShell) |

##### Secret masking rules

- **Never** log `$RPASS`, `$Password`, or any variable that holds a secret value.
- Assign the secret to a variable with a name ending in `_SECRET` or `_PASS` — do not echo that variable.
- Log the **remote user** (`$RUSER` / `$RUser`) but never the credential alongside it.

##### Bash logging template

```bash
# --- header ---
echo "[INFO] ============================================================"
echo "[INFO] Job      : <job_name>"
echo "[INFO] Date     : {{ ds_nodash }}"
echo "[INFO] Source   : $LPATH"
echo "[INFO] Dest     : ftps://$RHOST$RPATH"
echo "[INFO] User     : $RUSER  (password suppressed)"
echo "[INFO] ============================================================"

# --- per-step ---
echo "[INFO] STEP <N>: <description>"

# --- completion ---
echo "[INFO] DONE <job_name> — {{ ds_nodash }}"

# --- error (via trap) ---
trap 'echo "[ERROR] <job_name> failed at line $LINENO — exit $?"' ERR
```

##### PowerShell logging template

```powershell
# --- header ---
Write-Host "[INFO] ============================================================"
Write-Host "[INFO] Job      : <job_name>"
Write-Host "[INFO] Date     : {{ ds_nodash }}"
Write-Host "[INFO] Source   : $LPath"
Write-Host "[INFO] Dest     : ftps://$RHost$RPath"
Write-Host "[INFO] User     : $RUser  (password suppressed)"
Write-Host "[INFO] ============================================================"

# --- per-step ---
Write-Host "[INFO] STEP <N>: <description>"

# --- completion ---
Write-Host "[INFO] DONE <job_name> — {{ ds_nodash }}"

# --- error ---
# Check $LASTEXITCODE after every external command and log before exit:
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] <job_name> failed at step <N> — exit $LASTEXITCODE"
    exit 1
}
```

#### Date Variables

Follow **Airflow Date/Time Best Practices** (see above). Do NOT use `date`/`$(date)` (bash) or `Get-Date` (PowerShell) for business date calculations unless explicitly required. `%%ODATE`/`%%$ODATE` → `{{ ds_nodash }}`.

#### File Operations

* Validate file existence before transfer or processing.
* **Always log file size** for any file operation (upload, download, rename, move, delete). This is essential for diagnosing empty files, partial transfers, and size mismatches.
* Bash: quote paths `"$FILE_PATH"`.
* PowerShell: use `"D:\path\file.txt"`. For wildcard paths use unquoted `-Path $LPath` — double-quoted strings suppress glob expansion:

##### File size logging — Bash

```bash
# Single file — before transfer
FILE_SIZE=$(stat -c%s "$LPATH" 2>/dev/null || echo "unknown")
echo "[INFO] File      : $LPATH"
echo "[INFO] Size      : ${FILE_SIZE} bytes"

# Wildcard — list all matched files with sizes before transfer
echo "[INFO] Source files:"
ls -lh $LPATH 2>/dev/null || echo "[WARN] No files matched: $LPATH"

# After transfer — confirm destination exists and log its size
DEST_SIZE=$(stat -c%s "$NEWNAME" 2>/dev/null || echo "unknown")
echo "[INFO] Dest file : $NEWNAME"
echo "[INFO] Dest size : ${DEST_SIZE} bytes"
```

##### File size logging — PowerShell

```powershell
# Single file — before transfer
$FileInfo = Get-Item -Path $LPath -ErrorAction SilentlyContinue
if ($FileInfo) {
    Write-Host "[INFO] File      : $($FileInfo.FullName)"
    Write-Host "[INFO] Size      : $($FileInfo.Length) bytes"
} else {
    Write-Host "[WARN] File not found: $LPath"
}

# Wildcard — list all matched files with sizes before transfer
Write-Host "[INFO] Source files:"
Get-Item -Path $LPath -ErrorAction SilentlyContinue | ForEach-Object {
    Write-Host "[INFO]   $($_.Name)  $($_.Length) bytes"
}

# After transfer — confirm destination exists and log its size
$DestInfo = Get-Item -Path $DestPath -ErrorAction SilentlyContinue
if ($DestInfo) {
    Write-Host "[INFO] Dest file : $($DestInfo.FullName)"
    Write-Host "[INFO] Dest size : $($DestInfo.Length) bytes"
}
```
  ```powershell
  Copy-Item -Path $LPath -Destination "\\$RHost\share\dest\" -Force
  ```

#### FTP / SFTP / FTPS

* **Bash:** use `lftp` with `-e` form. Pass credentials and connection inside the `-e` string using `open -u`. This ensures all `set` commands are evaluated before the connection is established.

  **Standard FTPS template (`CONNTYPE2=FTP-SSL`):**
  ```bash
  lftp \
      -e "set ssl:verify-certificate false; \
          set ftp:ssl-protect-data true; \
          set ftp:ssl-protect-list true; \
          set ftp:passive-mode yes; \
          set cmd:verbose false; \
          set xfer:log on; \
          open -u '$RUSER','$RPASS' ftps://$RHOST:$RPORT; \
          mput -O \"$RPATH\" $LPATH; \
          bye"
  ```

  > **`open -u USER,PASS URL` inside `-e`** — credentials are passed inside the `-e` command string, not as the outer `-u` flag. This ensures `set` options take effect before `open` connects.
  > **`ftp:ssl-protect-data true` + `ftp:ssl-protect-list true`** — enables TLS on both the data and control channels. Use these instead of `ftp:ssl-force yes` which can fail on port 991.
  > **`ssl:verify-certificate false`** — disables cert verification for internal/legacy hosts. Prevents lftp from trying to load `<hostname>.crt` from disk which causes a hang if the file is absent.
  > **`bye` not `quit`** — use `bye` to close the lftp session cleanly.
  > **Never use `ftp:ssl-implicit`** — this variable does not exist in lftp and will produce `no such variable` error.

* **Port:** Always declare `_rport` as a module-level variable and pass it in the `open` URL:
  | Protocol | Standard port | **This environment** |
  |---|---|---|
  | FTP (plain) | 21 | 21 |
  | FTPS (`FTP-CONNTYPE2=FTP-SSL`) | 990/991 | **991** (all servers — Windows and MVS) |
  | SFTP | 22 | 22 |
  > **`CONNTYPE2=FTP-SSL` → always use `_rport = 991`** regardless of target OS. Never use port 21 for FTPS.

* **PowerShell:** use `lftp` for FTP/SFTP/FTPS — apply the same `-e` form and parameter set above.

#### Security

* Never hardcode passwords. Use Airflow Variables or Connections.
* Do NOT use `set cmd:verbose true` in `lftp` — it prints the full FTP protocol trace including `PASS <password>` in plain text. Always use `set cmd:verbose false; set xfer:log on`. `xfer:log on` logs each transferred file with its size and duration without exposing credentials.
* Passwords from `{{ var.value['...'] }}` appear in the Airflow "Rendered Template" log — emit `# WARNING: password visible in Airflow rendered template log` on the line where the variable is assigned.
* **Never echo a secret variable** — do not log `$RPASS`, `$Password`, or any variable holding a token or credential at any log point. Log the username only with the note `(password suppressed)`.
* In PowerShell, do not use `-Verbose` on `lftp` or any command that receives a password as an argument.


## Reusable File Transfer DAG (Optional Mode)

By default the skill generates a self-contained DAG per folder where every `FILE_TRANS` task is inlined directly inside `SSHOperator` / `PsrpOperator`. This is **Mode A (default)**.

Optionally, the project team may choose **Mode B — Reusable Transfer DAG**. In this mode the project owns a single parameterized transfer DAG (cloned from template) and all `FILE_TRANS` jobs in the caller DAG become `TriggerDagRunOperator` + `ExternalTaskSensor` pairs.

> **Always ask the user which mode they want before generating.** If not specified, use Mode A.

### Mode Selection Prompt

```
Which output mode do you want for FILE_TRANS jobs?

  [A] Inline (default)
      Full transfer script inlined inside SSHOperator / PsrpOperator per task.
      Self-contained, no inter-DAG dependencies.

  [B] Reusable transfer DAG
      Project owns one parameterized transfer DAG (schedule=None).
      Each FILE_TRANS job becomes TriggerDagRunOperator + ExternalTaskSensor.
      All parameters passed via conf={} — one DAG handles all jobs in the project.
```

---

### Mode A — Inline (default)

Standard output — no change from existing behavior. Every `FILE_TRANS` job generates a fully inlined task.

---

### Mode B — Reusable Transfer DAG

#### Output files

Generate **three files** per project:

```
scb_projecta_file_transfer_win_dev.py    ← reusable DAG (Windows, if any Windows FILE_TRANS)
scb_projecta_file_transfer_unix_dev.py   ← reusable DAG (Unix, if any Unix FILE_TRANS)
scb_projecta_jobs_dev.py                 ← business DAG (caller)
```

One reusable DAG per OS type per project — regardless of how many source servers or destination hosts exist. All transfer parameters are passed at runtime via `conf={}`.

#### Template selection

| Source OS (from NODEID lookup) | Clone from template |
|---|---|
| Windows (`PsrpOperator`) | `file-transfer-reusable-win-dynamic` |
| Unix (`SSHOperator`) | `file-transfer-reusable-unix-dynamic` |

Clone the template, replace only `##COMPANY##`, `##PROJECT##`, `##DAG_NAME##`, `##ENV##`, `##ACTIVE##`, `##TAGS##`, `##EMAIL_LIST##`, `##ENABLE_EMAIL_NOTIFICATION_SUCCESS##`, `##ENABLE_EMAIL_NOTIFICATION_FAIL##`. No infrastructure placeholders — all infra comes from `conf={}` at runtime.

#### Reusable DAG naming

`<company>-<project>-file-transfer-<win|unix>-<env>`

Example: `scb-projecta-file-transfer-win-dev`

#### Per-job params passed via `conf={}`

Every `FILE_TRANS` job maps to these `conf` keys:

| `conf` key | Derived from Control-M | Required |
|---|---|---|
| `psrp_conn_id` / `ssh_conn_id` | `NODEID` → Connection ID Derivation | yes |
| `source_path` | `FTP-LPATH{N}` (with `%%` substitution) | yes |
| `dest_host` | `FTP-RHOST` | yes |
| `dest_port` | `FTP-RPORT` — **no default, always explicit**: `21` (ftp/ftps) or `22` (sftp) | yes |
| `dest_protocol` | `FTP-CONNTYPE2` lowercased — **no default, always explicit**: `ftp` / `ftps` / `sftp` | yes |
| `dest_user` | `FTP-RUSER` | yes |
| `dest_path` | `FTP-RPATH{N}` (with `%%` substitution) | yes |
| `password_var_name` | Airflow Variable name for `FTP-RPASS` | yes |
| `pre_command` | `FTP-PRECOMM{N}` — empty string `""` if not present | yes |
| `post_command` | `FTP-POSTCOMM{N}` — empty string `""` if not present | yes |
| `delete_source` | `True` if `FTP-SRCOPT{N}=1`, else `False` | yes |

> **`dest_port` and `dest_protocol` have no defaults** — always derive from Control-M XML and pass explicitly. Never assume `21`/`ftp`.

#### Generated caller DAG — one FILE_TRANS job becomes two tasks

Each `FILE_TRANS` job generates a **`TriggerDagRunOperator` + `ExternalTaskSensor` pair**:

```python
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# Control-M job: TRANSFER_REPORT | NODEID: winsrv01 | FTP-UPLOAD1=1
# LPATH: D:\data\report\*.csv | RPATH: /incoming/report/
# INCOND: PREV_JOB-ENDED-OK | OUTCOND: TRANSFER_REPORT-ENDED-OK
ap1234_projecta_task_trigger_transfer_report_d = TriggerDagRunOperator(
    task_id='ap1234-projecta-task_trigger_transfer_report-d',
    trigger_dag_id='scb-projecta-file-transfer-win-dev',
    wait_for_completion=False,      # fire and free worker slot immediately
    conf={
        "psrp_conn_id":      "psrp_winsrv01",
        "source_path":       r"D:\data\report\*.csv",
        "dest_host":         "10.0.1.50",
        "dest_port":         "21",
        "dest_protocol":     "ftp",
        "dest_user":         r"domain\svc_transfer",
        "dest_path":         "/incoming/report/",
        "password_var_name": "projecta_transfer_password",
        "pre_command":       "",
        "post_command":      "",
        "delete_source":     False,
    },
)

ap1234_projecta_task_wait_transfer_report_d = ExternalTaskSensor(
    task_id='ap1234-projecta-task_wait_transfer_report-d',
    external_dag_id='scb-projecta-file-transfer-win-dev',
    external_task_id=None,          # None = watch entire DAG run (not a specific task)
    mode='reschedule',              # releases worker slot between polls
    poke_interval=60,
    timeout=3600,
    on_failure_callback=failure_callback,
)
```

#### Task naming convention for Mode B

| Task | `task_id` pattern |
|---|---|
| Trigger | `<app_id>-<app_code>-task_trigger_<job_name>-<period>` |
| Sensor | `<app_id>-<app_code>-task_wait_<job_name>-<period>` |

Python variable names follow the same pattern with `_` replacing `-`.

#### Dependency wiring — wire to the sensor, not the trigger

`INCOND`/`OUTCOND` chains connect to `wait_<job_name>` — that is the task that signals completion to downstream jobs:

```python
# Upstream task → trigger → sensor → downstream task
ap1234_projecta_task_prev_job_d >> ap1234_projecta_task_trigger_transfer_report_d
ap1234_projecta_task_trigger_transfer_report_d >> ap1234_projecta_task_wait_transfer_report_d
ap1234_projecta_task_wait_transfer_report_d >> ap1234_projecta_task_process_data_d
```

Or chained concisely:

```python
ap1234_projecta_task_prev_job_d >> ap1234_projecta_task_trigger_transfer_report_d >> ap1234_projecta_task_wait_transfer_report_d >> ap1234_projecta_task_process_data_d
```

#### Worker slot behaviour

```
Mode A — inline:
  worker pod ──────────────── transfer running ──────────────── done ──► free
  [slot held for full transfer duration, e.g. 30 min]

Mode B — trigger + sensor(reschedule):
  trigger: worker pod ──► free  (seconds)
  sensor:  wake ──► poll ──► sleep ──► wake ──► poll ──► done ──► free
  [slot held only during poll check, e.g. ~2 sec every 60 sec]
```

#### Required imports in caller DAG (Mode B)

```python
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
```

Use the `providers.standard` paths (Airflow 3.x). Do not use `airflow.sensors.external_task` or `airflow.operators.trigger_dagrun` — those are the Airflow 2.x paths. Only import these if Mode B is selected — do not add unused imports.

#### Reusable DAG — validate_source task

The `validate_source_files` task checks that source files exist **on the source server**. The `SSHOperator` is already connected to the source server via `ssh_conn_id` — use a local `ls` only:

```bash
SRC_PATH="{{ params.source_path }}"
ls ${SRC_PATH} || { echo "[ERROR] No source files: ${SRC_PATH}"; exit 1; }
```

Do **not** connect to `dest_host` in `validate_source` — that is the transfer destination, not the source. Connecting to `dest_host` with the source path will always return empty or fail.

#### Verification (Mode B)

Run `python` + `pyflakes` on **both** files — reusable DAG and caller DAG must both pass before delivery:

```bash
python scb_projecta_file_transfer_win_dev.py && pyflakes scb_projecta_file_transfer_win_dev.py
python scb_projecta_jobs_dev.py              && pyflakes scb_projecta_jobs_dev.py
```

---

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
    apache-airflow-providers-standard \
    pendulum \
    pyflakes
```

> This venv is used for DAG syntax verification (step 7 in Behavior). It does not need a running Airflow instance — `python <dag>.py` validates imports and DAG instantiation; `pyflakes <dag>.py` catches unused imports and undefined names.


## Reference
- [`controlm-schema.xsd`](raws/controlm-schema.xsd) — official Control-M XML schema (DEFTABLE, FOLDER, SMART_FOLDER, JOB, INCOND, OUTCOND, VARIABLE, etc.)
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
   $Output = & lftp `
       -e "set ssl:verify-certificate false; set ftp:ssl-protect-data true; set ftp:ssl-protect-list true; `
           set cmd:verbose false; set xfer:log on; `
           open -u '$RUser','$RPass' ftps://${RHost}:$RPort; `
           mkdir /mnt/data/output/{{ ds_nodash }}/folder; bye" 2>&1
   if ($LASTEXITCODE -ne 0) {
       Write-Host "[ERROR] mkdir failed: $Output"; exit 1
   }
   
   # Transfer 1: Upload
   $Output = & lftp `
       -e "set ssl:verify-certificate false; set ftp:ssl-protect-data true; set ftp:ssl-protect-list true; `
           set ftp:passive-mode yes; set cmd:verbose false; set xfer:log on; `
           open -u '$RUser','$RPass' ftps://${RHost}:$RPort; `
           cd /mnt/data/output/{{ ds_nodash }}/folder; put \"$LPath\"; bye" 2>&1
   ```
   
   **Critical:** Always include pre-commands and post-commands in the script flow; do NOT skip them even if they seem simple (mkdir, rm, etc.)

5. **Handle multiple transfers:** Build a bash/PowerShell loop if `FTP-TRANSFER_NUM > 1`

##### FILE_TRANS → Unix→Unix / Unix→Windows (lftp)

For `FTP-CONNTYPE2 ∈ {FTP, FTPS, SFTP}`. Script structure: `set -euo pipefail` + `trap` → assign vars from `FTP-*` → `lftp` command → log complete.

Key rules:
- `PROTOCOL=$(echo "$CONNTYPE2" | tr '[:upper:]' '[:lower:]')` — lowercase for URI scheme
- FTPS: use `ftp:ssl-protect-data true` + `ftp:ssl-protect-list true` + `ssl:verify-certificate false` inside `-e`; never `ftp://` (allows plain-FTP fallback)
- Passive mode: `FTP-LPASSIVE=1` → `set ftp:passive-mode yes`
- `FTP-TYPE{N}=I` (binary) → no extra flags; `FTP-TYPE{N}=A` (ASCII) → `-a` flag on `put`/`get`
- `FTP-SRCOPT{N}=1` → append `rm "$LPATH"` after upload
- Pre-compute `RDIR=$(dirname "$RPATH")` **outside** the `lftp -e` string — `$(...)` inside `-e` runs on the Airflow worker pod, not the remote host
- **Wildcard paths** (`*` or `?` in LPATH/RPATH): use `mput`/`mget`, NOT `put`/`get` (lftp will not expand globs with single-file commands):
  ```bash
  RDIR=$(dirname "$RPATH")
  # Upload wildcard (FTPS)
  lftp \
      -e "set ssl:verify-certificate false; \
          set ftp:ssl-protect-data true; \
          set ftp:ssl-protect-list true; \
          set ftp:passive-mode yes; \
          set cmd:verbose false; \
          set xfer:log on; \
          open -u '$RUSER','$RPASS' ftps://$RHOST:$RPORT; \
          cd \"$RDIR\"; mput $LPATH; \
          bye"
  # Download wildcard (FTPS)
  lftp \
      -e "set ssl:verify-certificate false; \
          set ftp:ssl-protect-data true; \
          set ftp:ssl-protect-list true; \
          set ftp:passive-mode yes; \
          set cmd:verbose false; \
          set xfer:log on; \
          open -u '$RUSER','$RPASS' ftps://$RHOST:$RPORT; \
          mget -O \"$LDIR\" $RPATH; \
          bye"
  ```
- `RPASS` from `{{ var.value['FTP_RPASS_SECRET'] }}` — emit `# WARNING: password visible in Airflow rendered template log`

##### FILE_TRANS → Unix→S3 (aws s3)

For `FTP-CONNTYPE2=S3`. Script structure: `set -euo pipefail` + `trap` → assign vars → `aws s3` command → log complete.

Key rules:
- Upload (`FTP-UPLOAD{N}=1`): `aws s3 cp "$LPATH" "s3://$S3_BUCKET$RPATH" --region "$S3_REGION" --profile "$AWS_PROFILE"`
- Download: swap src/dest
- `FTP-TYPE{N}=I` → add `--no-progress`
- **Wildcard paths**: use `aws s3 sync` — `aws s3 cp` does not expand globs:
  ```bash
  aws s3 sync "$(dirname "$LPATH")" "s3://$S3_BUCKET/$(dirname "$RPATH")/" \
      --include "$(basename "$LPATH")" --exclude "*" \
      --region "$S3_REGION" --profile "$AWS_PROFILE"
  ```
- Credentials: Airflow Connections or `~/.aws/credentials`

##### FILE_TRANS → Unix→Azure (azcopy)

For `FTP-CONNTYPE2=Azure`. Script structure: `set -euo pipefail` + `trap` → assign vars → build `DEST_URI` → `azcopy copy` → log complete.

Key rules:
- `DEST_URI="https://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER}${RPATH}?${SAS_TOKEN}"`
- Upload: `azcopy copy "$LPATH" "$DEST_URI"`; Download: swap args
- `SAS_TOKEN` from Airflow Variable — never hardcoded
- **Wildcard paths**: use `--include-pattern` — shell does not expand globs in quoted strings:
  ```bash
  azcopy copy "$(dirname "$LPATH")/*" "$DEST_URI" --include-pattern "$(basename "$LPATH")"
  ```

##### FILE_TRANS → Windows source (PowerShell template)

For Windows source (`FTP-LOSTYPE=Windows`), use `PsrpOperator` with PowerShell:

```python
powershell=r"""
$ErrorActionPreference = 'Stop'

$LPath = "{{ FTP-LPATH{N} }}"
$RPath = "{{ FTP-RPATH{N} }}"
$RHost = "{{ FTP-RHOST }}"
$RUser = "{{ FTP-RUSER }}"
$RPass = "{{ var.value['FTP_RPASS_SECRET'] }}"

Write-Host "[INFO] Starting {{ FTP-UPLOAD{N}=1 ? 'upload' : 'download' }}"
Write-Host "[INFO] Local: $LPath"
Write-Host "[INFO] Remote: ${RHost}:${RPath}"

# Use lftp for FTP/FTPS/SFTP transfers from Windows agent (FTPS shown — adjust open URL scheme for FTP/SFTP)
$Output = & lftp `
    -e "set ssl:verify-certificate false; `
        set ftp:ssl-protect-data true; `
        set ftp:ssl-protect-list true; `
        set ftp:passive-mode yes; `
        set cmd:verbose false; `
        set xfer:log on; `
        open -u '$RUser','$RPass' ftps://${RHost}:$RPort; `
        {{ FTP-UPLOAD{N}=1 ? 'put \"'+$LPath+'\" -o \"'+$RPath+'\"' : 'get \"'+$RPath+'\" -o \"'+$LPath+'\"' }}; `
        bye" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Transfer failed: $Output"
    exit 1
}

Write-Host "[INFO] Transfer complete"
"""

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
> trap 'echo "[ERROR] Polling script failed at line $LINENO"' ERR
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


### 5. APPL_TYPE = `AIRFLOWV2` — **not supported**

`AIRFLOWV2` jobs are not migrated. When encountered, emit a warning comment and skip the job:

```python
# TODO: APPL_TYPE=AIRFLOWV2 is not supported — job <JOBNAME> skipped.
#       These jobs trigger an existing Airflow DAG from Control-M (%%UCM-DAGID=<dag_id>).
#       Migration requires a manual decision on whether to retain, remove, or re-wire this dependency.
```

Do not generate any operator or task for this job type.

---

### 6. APPL_TYPE = `BIM` — **not supported**

`BIM` (Business Impact Management) jobs are Control-M SLA monitoring placeholders (`TASKTYPE=Dummy`) with no executable logic. They are not migrated. When encountered, emit a warning comment and skip the job:

```python
# TODO: APPL_TYPE=BIM is not supported — job <JOBNAME> skipped.
#       BIM jobs are SLA monitoring placeholders (TASKTYPE=Dummy); they carry no executable logic.
#       Re-implement SLA requirements (%%BIM-DUE_TIME, %%BIM-SENSITIVITY) using Airflow SLA miss
#       callbacks or external monitoring at deployment time.
```

Do not generate any operator or task for this job type.

---

## Dependency Mapping — INCOND/OUTCOND Pattern Reference

### 1. Condition String Format

#### Standard pattern

```
<JOB_NAME>-<STATUS>
```

The condition name equals the **source job's own name** plus a status suffix.  
This means: the predecessor emits its name as the outcond token; the successor waits on it as incond.

| Status suffix | Notes |
|---------------|-------|
| `ENDED-OK`    | Dominant — normal successful completion |
| `ENDED`       | Completion regardless of exit code |
| `END-OK`      | Legacy/typo variant of `ENDED-OK` |
| `ENED-OK`     | Typo variant |

#### Non-standard / custom tokens

| Variant | Example | Meaning |
|---------|---------|---------|
| `<NAME>X-ENDED-OK` | `AFT_OFSAA_MANUAL_D_00010X-ENDED-OK` | Aliased name — references a renamed/alternate job token, not the actual FROM node |
| `<NAME>-RERUN` | `RT_OBMS_FMS_D0010-RERUN` | Rerun-specific gate |
| `<NAME>-M2F` | `RT_NEWMUREX_DTMREP_D0010-M2F` | Monday-to-Friday schedule variant |
| `<NAME>-SAT` / `-SUN` | `RT_NEWMUREX_DTMREP_D0025-SAT` | Weekend run variant |
| `<NAME>-SPECIFIC` | `RT_NEWMUREX_FRPT_EOD1_1850-SPECIFIC` | Special/manual run |
| `<NAME>-ENDED-OK-<NUM>` | `RT_AFT_S1PTTRECCBSD005-ENDED-OK-969` | Numbered instance (cyclic job variant) |

**Key insight:** Non-standard tokens cannot be auto-generated from the job name — preserve verbatim.

---

### 2. AND / OR Gate Logic (`and_or` field)

- `A` (AND, default) — all predecessor conditions must be satisfied
- `O` (OR, rare) — any one predecessor condition is sufficient → use `trigger_rule=TriggerRule.ONE_SUCCESS`

AND is the default. Assume AND unless `and_or = "O"` is explicit.

---

### 3. INCOND Resolution Algorithm

For each `INCOND` on a job, apply this decision tree:

1. Extract `JOB_NAME` from the condition string by stripping the known status suffix (`-ENDED-OK`, `-ENDED`, `-END-OK`, `-ENED-OK`, `-RERUN`, `-M2F`, `-SAT`, `-SUN`, `-SPECIFIC`, `-ENDED-OK-<N>`).
2. Is that `JOB_NAME` present in **this folder's** job list?
   - **YES** → wire as a task dependency: `predecessor_task >> this_task`
   - **NO** → add an `ExternalTaskSensor` pointing to the external DAG that owns that job (`external_dag_id` must be derived from the folder that contains that job)
3. If `AND_OR="O"` with multiple INCONDs → use `trigger_rule=TriggerRule.ONE_SUCCESS` instead of the default `ALL_SUCCESS`

## Node ID Information

To determine the OS for a given `NODEID`:

1. Look up the `NODEID` value (case-insensitive) in [`raws/node_id_win.md`](raws/node_id_win.md).
2. If found → OS is **Windows** → use `PsrpOperator`.
3. If not found → OS is **Linux** (default) → use `SSHOperator`.
