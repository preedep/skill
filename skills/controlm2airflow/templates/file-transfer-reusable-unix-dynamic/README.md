# File Transfer Reusable DAG — Unix Dynamic (lftp/azcopy via SSH)

Reusable per-project DAG for Unix file transfers. Acts like a function — all parameters
are passed at runtime by the caller via `TriggerDagRunOperator conf={}`.

One DAG handles all Unix `FILE_TRANS` jobs in the project. No infrastructure is hardcoded.
Project team owns and maintains this DAG. Isolated blast radius — changes only affect this project.

`schedule=None` — always triggered by caller via `TriggerDagRunOperator`, never runs on its own.

---

## How to choose: Direct or Relay?

```mermaid
flowchart TD
    A([Start: set up conf]) --> B{Can Airflow SSH directly\nto the source server?}

    B -- Yes --> C[Direct mode\nsrc_protocol = '' or 'local']
    B -- No --> D{Why not?}

    D -- Source server is behind\na firewall / DMZ --> E[Use Relay / Wilson mode]
    D -- Source files are on\na NAS mount on SSH target --> C

    E --> F[ssh_conn_id = relay server\nsrc_protocol = sftp / ftps / ftp\nsrc_host = actual source server\nwilson_staging_dir = /tmp/airflow/staging/job]

    C --> G[ssh_conn_id = source server\nsrc_protocol = '' leave empty]
```

---

## What happens at runtime

### Direct mode (`src_protocol` is empty or `"local"`)

```mermaid
flowchart LR
    Airflow -->|SSH| SourceServer
    SourceServer -->|ftp/ftps/sftp: lftp| Destination
    SourceServer -->|s3: aws s3 cp| S3[(S3 bucket)]
    SourceServer -->|blob: azcopy| Blob[(Azure Blob)]

    style SourceServer fill:#d4edda,stroke:#28a745
```

```mermaid
flowchart TD
    S([start]) --> VP[validate_params]
    VP --> PRE[pre_command]
    PRE --> PULL[pull_from_source\n⏭ skipped]
    PULL --> VAL[validate_source\nchecks source_path]
    VAL --> TF[transfer_files\nftp/ftps/sftp → lftp source_path → dest\ns3 → aws s3 cp source_path → s3://bucket\nblob → azcopy source_path → container]
    TF --> VER[verify]
    VER --> POST[post_transfer_action]
    POST --> PC[post_command]
    PC --> E([end])

    style PULL fill:#f8f9fa,stroke:#adb5bd,color:#adb5bd
```

### Relay mode (`src_protocol` = `sftp` / `ftps` / `ftp`)

> Relay only supports pulling from the source via lftp (sftp/ftps/ftp).
> The destination push can still be ftp/ftps/sftp, s3, or blob — relay stages files locally first, then pushes using the appropriate tool.

```mermaid
flowchart LR
    Airflow -->|SSH| Relay
    Relay -->|lftp pull\nsrc_protocol| SourceServer
    SourceServer -->|files| Relay
    Relay -->|ftp/ftps/sftp: lftp staging→dest| Destination
    Relay -->|s3: aws s3 cp staging→bucket| S3[(S3 bucket)]
    Relay -->|blob: azcopy staging→container| Blob[(Azure Blob)]

    style Relay fill:#fff3cd,stroke:#ffc107
    style SourceServer fill:#d4edda,stroke:#28a745
```

```mermaid
flowchart TD
    S([start]) --> VP[validate_params]
    VP --> PRE[pre_command]
    PRE --> PULL[pull_from_source\nrelay pulls source_path from src_host\nvia lftp sftp/ftps/ftp\n→ wilson_staging_dir/run_id]
    PULL --> VAL[validate_source\nchecks wilson_staging_dir/run_id]
    VAL --> TF[transfer_files\nftp/ftps/sftp → lftp staging_dir/* → dest\ns3 → aws s3 cp staging_dir → s3://bucket\nblob → azcopy staging_dir → container]
    TF --> VER[verify]
    VER --> POST[post_transfer_action\n+ Wilson cleanup\nrm -rf staging_dir/run_id]
    POST --> PC[post_command]
    PC --> E([end])

    style PULL fill:#fff3cd,stroke:#ffc107
    style POST fill:#fff3cd,stroke:#ffc107
```

### Relay detection rule (same check in every task)

```bash
if [ -n "${SRC_PROTOCOL}" ] && [ "${SRC_PROTOCOL}" != "local" ]; then
    # relay mode — use wilson_staging_dir as source
else
    # direct mode — use source_path directly
fi
```

| `src_protocol` value | Mode | pull_from_source |
|---|---|---|
| `""` (empty) | Direct | skipped |
| `"local"` | Direct | skipped |
| `"sftp"` | Relay | runs — lftp sftp pull |
| `"ftps"` | Relay | runs — lftp ftps pull (explicit TLS) |
| `"ftp"` | Relay | runs — lftp ftp pull |

---

## DAG task summary

| Task | Direct | Relay | Skipped when |
|---|---|---|---|
| `validate_params` | ✅ runs | ✅ runs | — |
| `pre_command` | ✅ runs | ✅ runs | `pre_command` param is empty |
| `pull_from_source` | ⏭ skipped | ✅ pulls `source_path` from `src_host` → staging | `src_protocol` empty / `local` |
| `validate_source_files` | checks `source_path` | checks `wilson_staging_dir/run_id` | `archive_only`, `cleanup_only`, `s3_download`, `blob_download` |
| `transfer_files` | lftp `source_path` → dest (ftp/ftps/sftp)<br>aws s3 cp `source_path` → bucket (s3)<br>azcopy `source_path` → container (blob) | lftp `staging_dir/*` → dest (ftp/ftps/sftp)<br>aws s3 cp `staging_dir` → bucket (s3)<br>azcopy `staging_dir` → container (blob) | `archive_only`, `cleanup_only` |
| `verify` | checks dest | checks dest | `archive_only`, `cleanup_only` |
| `post_transfer_action` | delete/rename/move/archive | same + Wilson cleanup (`rm -rf staging_dir/run_id`) | depends on `direction` |
| `post_command` | ✅ runs | ✅ runs | `post_command` param is empty |

---

## Parameters

### Identity (set once in the .py file)

| Variable | Description | Example |
|---|---|---|
| `_company` | Company prefix | `"nix"` |
| `_project` | Project name | `"apxxxx"` |
| `_env` | Environment | `"nonprod"` |
| `_dag_name` | DAG name suffix | `"file-transfer-unix"` |

### Runtime conf (passed per job from caller DAG)

#### Connection

| Parameter | Required | Description |
|---|---|---|
| `ssh_conn_id` | ✅ | SSH conn ID to **source server** (direct) or **relay/Wilson server** (relay) |

#### Two-hop relay — leave all empty for direct transfer

| Parameter | Required | Description |
|---|---|---|
| `src_protocol` | ➖ | `""` or `"local"` = direct; `"sftp"` / `"ftps"` / `"ftp"` = relay pull |
| `src_host` | ✅† | Source server hostname — †required when relay |
| `src_port` | ✅† | Source port: `22` (sftp) or `21` (ftp/ftps) |
| `src_user` | ✅† | Source username |
| `src_password_var` | ✅† | Airflow Variable key holding source password |
| `wilson_staging_dir` | ✅† | Staging dir on relay (e.g. `/tmp/airflow/staging/job_xyz`) |
| `wilson_cleanup` | ➖ | `"true"` (default) = delete staging after push; `"false"` = keep |

#### Transfer

| Parameter | Required | Description |
|---|---|---|
| `source_path` | ✅ | File path / glob on source server (relay) or on SSH target (direct) |
| `dest_protocol` | ✅ | `ftp` / `ftps` / `sftp` / `s3` / `blob` |
| `dest_host` | ✅* | Destination hostname or IP (*ftp/ftps/sftp only) |
| `dest_port` | ✅* | Destination port: `21` (ftp/ftps) or `22` (sftp) |
| `dest_user` | ✅* | Destination FTP/SFTP username |
| `dest_path` | ✅ | Destination path (remote for upload; local for download) |
| `password_var_name` | ✅* | Airflow Variable key holding destination password |
| `direction` | ✅ | Transfer mode — see table below |

#### S3 (required when `dest_protocol=s3`)

| Parameter | Required | Description |
|---|---|---|
| `s3_bucket` | ✅ | S3 bucket name |
| `s3_prefix` | ✅ | S3 key prefix, e.g. `data/incoming/` |
| `s3_region` | ✅ | AWS region, e.g. `ap-southeast-1` |
| `s3_endpoint_url` | ➖ | VPC endpoint URL (empty = public S3) |
| `aws_profile` | ➖ | AWS CLI named profile (empty = instance role) |
| `aws_access_key_id_var` | ➖ | Airflow Variable key for `AWS_ACCESS_KEY_ID` |
| `aws_secret_key_var` | ➖ | Airflow Variable key for `AWS_SECRET_ACCESS_KEY` |

#### Azure Blob (required when `dest_protocol=blob`)

| Parameter | Required | Description |
|---|---|---|
| `blob_account` | ✅ | Azure Storage account name |
| `blob_container` | ✅ | Blob container name |
| `blob_prefix` | ✅ | Blob path prefix, e.g. `daily/incoming/` |
| `blob_identity_client_id` | ➖ | User-assigned MI client ID (empty = system-assigned MI) |
| `blob_sas_var_name` | ➖ | Airflow Variable key holding SAS token (empty = managed identity) |

#### Post-transfer extras

| Parameter | Required | Description |
|---|---|---|
| `new_name` | ➖ | New filename for `*_rename` / `*_move` directions |
| `archive_path` | ➖ | Archive base path (default: `dest_path/archive`) |
| `retention_days` | ➖ | Days to keep archives for `cleanup_only` (default: `30`) |
| `pre_command` | ➖ | Shell command to run before transfer (empty = skip) |
| `post_command` | ➖ | Shell command to run after transfer (empty = skip) |

---

## direction values

| Value | Protocol | What happens |
|---|---|---|
| `upload` | ftp/ftps/sftp | put source → dest, keep source (SRCOPT=0) |
| `upload_delete` | ftp/ftps/sftp | put source → dest, delete source after (SRCOPT=1) |
| `upload_rename` | ftp/ftps/sftp | put source → dest, rename source to `new_name` (SRCOPT=2) |
| `upload_move` | ftp/ftps/sftp | put source → dest, move source to `new_name` path (SRCOPT=3) |
| `download` | ftp/ftps/sftp | mget remote → local, keep remote (SRCOPT=0) |
| `download_delete` | ftp/ftps/sftp | mget remote → local, delete remote after (SRCOPT=1) |
| `download_rename` | ftp/ftps/sftp | mget remote → local, rename remote to `new_name` (SRCOPT=2) |
| `download_move` | ftp/ftps/sftp | mget remote → local, move remote to `new_name` path (SRCOPT=3) |
| `download_archive` | ftp/ftps/sftp | mget remote → local, archive locally by date |
| `archive_only` | — | no transfer — archive existing files in `dest_path` by date |
| `cleanup_only` | — | no transfer — delete archives older than `retention_days` |
| `s3_upload` | s3 | aws s3 cp source_path → s3://bucket/prefix |
| `s3_download` | s3 | aws s3 cp s3://bucket/prefix → dest_path |
| `blob_upload` | blob | azcopy copy source_path → blob account/container/prefix |
| `blob_download` | blob | azcopy copy blob account/container/prefix → dest_path |
| `s3_to_blob` | s3+blob | azcopy copy s3://bucket/prefix → blob account/container (no staging) |
| `blob_to_s3` | s3+blob | azcopy copy blob account/container → s3://bucket/prefix (no staging) |

---

## Quick Start

### Step 1: Copy and rename this template

```
{company}_{project}_file_transfer_unix_{env}.py
```

### Step 2: Edit identity variables at the top of the .py

```python
_company  = "nix"
_project  = "apxxxx"
_env      = "nonprod"
_dag_name = "file-transfer-unix"
```

### Step 3: Add Airflow Connection (SSH)

- **Connection ID**: value you will pass as `ssh_conn_id` in conf
- **Connection Type**: SSH
- **Host**: source server (direct) or relay/Wilson server (relay)
- **Username / Key**: user with SSH access

### Step 4: Add Airflow Variables (passwords)

- One Variable per password referenced by `password_var_name` or `src_password_var`
- **Value**: plaintext password — Airflow encrypts it at rest

### Step 5: Call from your business DAG

#### Two ways to call the reusable DAG

```mermaid
flowchart TD
    A([Caller DAG task]) --> B{How long is the transfer?\nHow many worker slots?}

    B -- Short transfer\nOR worker slots are plentiful --> C[Mode A — inline\nwait_for_completion=True]
    B -- Long transfer\nOR worker slots are scarce --> D[Mode B — trigger + sensor\nwait_for_completion=False]

    C --> E[TriggerDagRunOperator\nwait_for_completion=True\npoke_interval=30\nWorker slot held for full duration]

    D --> F[TriggerDagRunOperator\nwait_for_completion=False\nFires and releases slot immediately]
    F --> G[ExternalTaskSensor\nmode=reschedule\npoke_interval=60\nSlot held only during each poll check]
```

| | Mode A — inline | Mode B — trigger + sensor |
|---|---|---|
| `wait_for_completion` | `True` | `False` |
| Worker slot | Held for full transfer | Freed after trigger fires |
| Sensor needed | No | Yes — `ExternalTaskSensor` |
| Use when | Short transfer / simple DAG | Long transfer / many parallel jobs |

---

#### How pre_command and post_command work

- Both run on the **SSH target server** (same host as the transfer)
- Set to `""` (empty string) to skip — task exits immediately with no error
- `pre_command_args` / `post_command_args` are appended as arguments: `bash -c "$CMD $ARGS"`
- Special case: `chmod` — if no files match the glob, it warns and skips instead of failing

```
pre_command  runs BEFORE pull_from_source / validate_source / transfer_files
post_command runs AFTER  verify / post_transfer_action
```

Common uses:

| Use case | pre_command | pre_command_args |
|---|---|---|
| Create staging directory | `"mkdir -p /data/staging"` | `[]` |
| Change file permissions before transfer | `"chmod"` | `["664", "/data/input/*.*"]` |
| Touch a trigger file | `"touch /data/ready.flag"` | `[]` |
| Run a shell script | `"/opt/scripts/prepare.sh"` | `["--env", "nonprod"]` |

| Use case | post_command | post_command_args |
|---|---|---|
| Change permissions on delivered files | `"chmod"` | `["644", "/incoming/report/*.*"]` |
| Write a completion marker | `"touch /data/done.flag"` | `[]` |
| Clean up temp files | `"rm -f /data/staging/*.tmp"` | `[]` |
| Run a notification script | `"/opt/scripts/notify.sh"` | `["--job", "transfer_report"]` |

---

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── Direct: no pre/post command (skip both) ───────────────────────────────────
TriggerDagRunOperator(
    task_id='transfer_report',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "ssh_conn_id":       "ssh-unix-server",
        "source_path":       "/data/report/*.csv",
        "dest_host":         "dest-server",
        "dest_port":         "21",
        "dest_protocol":     "ftps",
        "dest_user":         "ftpuser",
        "dest_path":         "/incoming/report/",
        "password_var_name": "project-dest-password",
        "direction":         "upload",
        # pre_command / post_command omitted = empty = skipped
    },
)

# ── Direct: mkdir staging before transfer, chmod after ───────────────────────
TriggerDagRunOperator(
    task_id='transfer_with_commands',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "ssh_conn_id":          "ssh-unix-server",
        "source_path":          "/data/export/*.dat",
        "dest_host":            "dest-server",
        "dest_port":            "22",
        "dest_protocol":        "sftp",
        "dest_user":            "sftpuser",
        "dest_path":            "/var/incoming/",
        "password_var_name":    "project-dest-password",
        "direction":            "upload_delete",    # delete source after upload
        # create dest directory before transfer
        "pre_command":          "mkdir -p /var/incoming",
        "pre_command_args":     [],
        # fix permissions on delivered files after transfer
        "post_command":         "chmod",
        "post_command_args":    ["644", "/var/incoming/*.dat"],
    },
)

# ── Direct: run prepare script before, notify script after ───────────────────
TriggerDagRunOperator(
    task_id='transfer_with_scripts',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "ssh_conn_id":          "ssh-unix-server",
        "source_path":          "/data/daily/*.csv",
        "dest_host":            "dest-server",
        "dest_port":            "21",
        "dest_protocol":        "ftp",
        "dest_user":            "ftpuser",
        "dest_path":            "/upload/daily/",
        "password_var_name":    "project-dest-password",
        "direction":            "upload",
        "pre_command":          "/opt/scripts/prepare.sh",
        "pre_command_args":     ["--env", "nonprod"],
        "post_command":         "/opt/scripts/notify.sh",
        "post_command_args":    ["--job", "transfer_daily", "--status", "done"],
    },
)

# ── Relay: SSH target is Wilson relay, source is behind firewall ──────────────
TriggerDagRunOperator(
    task_id='transfer_via_relay',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "ssh_conn_id":        "ssh-wilson-relay",   # SSH to relay, NOT source
        "src_protocol":       "sftp",               # relay pulls from source via sftp
        "src_host":           "source-server",
        "src_port":           "22",
        "src_user":           "srcuser",
        "src_password_var":   "project-src-password",
        "wilson_staging_dir": "/tmp/airflow/staging/transfer_report",
        "source_path":        "/exports/*.dat",     # path on source server
        "dest_host":          "dest-server",
        "dest_port":          "21",
        "dest_protocol":      "ftps",
        "dest_user":          "ftpuser",
        "dest_path":          "/incoming/",
        "password_var_name":  "project-dest-password",
        "direction":          "upload",
        # pre/post commands run on the relay server
        "pre_command":        "mkdir -p /tmp/airflow/staging/transfer_report",
        "pre_command_args":   [],
        "post_command":       "",   # skip
        "post_command_args":  [],
    },
)
```

---

#### Mode B — TriggerDagRunOperator + ExternalTaskSensor (reschedule)

Use this pattern when the transfer is long-running or you have many parallel jobs and want to avoid
holding a worker slot for the full transfer duration.

```
Worker slot timeline:

Mode A (wait_for_completion=True):
  trigger ──── transfer running (e.g. 30 min) ──── done ──► slot freed

Mode B (wait_for_completion=False + ExternalTaskSensor reschedule):
  trigger ──► slot freed   (seconds)
  sensor:  wake─poll─sleep─wake─poll─sleep─wake─poll─done ──► slot freed
           slot held only ~2 sec per poll, freed between polls
```

```python
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# ── Mode B: fire and free, sensor watches completion ─────────────────────────
task_trigger_transfer = TriggerDagRunOperator(
    task_id='trigger_transfer_report',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=False,      # fire and release worker slot immediately
    conf={
        "ssh_conn_id":       "ssh-unix-server",
        "source_path":       "/data/report/*.csv",
        "dest_host":         "dest-server",
        "dest_port":         "21",
        "dest_protocol":     "ftps",
        "dest_user":         "ftpuser",
        "dest_path":         "/incoming/report/",
        "password_var_name": "project-dest-password",
        "direction":         "upload",
        "pre_command":       "mkdir -p /incoming/report",
        "pre_command_args":  [],
        "post_command":      "chmod",
        "post_command_args": ["644", "/incoming/report/*.csv"],
    },
)

task_wait_transfer = ExternalTaskSensor(
    task_id='wait_transfer_report',
    external_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    external_task_id=None,          # None = watch the whole DAG run, not one task
    mode='reschedule',              # releases worker slot between polls
    poke_interval=60,               # poll every 60 seconds
    timeout=3600,                   # fail if not done within 1 hour
    on_failure_callback=failure_callback,
)

# ── Wire dependencies: upstream → trigger → sensor → downstream ───────────────
task_prev_job >> task_trigger_transfer >> task_wait_transfer >> task_next_job
```

#### Mode B with multiple transfers — parallel fan-out

```python
# Trigger both transfers immediately (parallel, no dependency between them)
task_trigger_report = TriggerDagRunOperator(
    task_id='trigger_transfer_report',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=False,
    conf={
        "ssh_conn_id": "ssh-unix-server", "source_path": "/data/report/*.csv",
        "dest_host": "dest-server", "dest_port": "21", "dest_protocol": "ftps",
        "dest_user": "ftpuser", "dest_path": "/incoming/report/",
        "password_var_name": "project-dest-password", "direction": "upload",
    },
)

task_trigger_summary = TriggerDagRunOperator(
    task_id='trigger_transfer_summary',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=False,
    conf={
        "ssh_conn_id": "ssh-unix-server", "source_path": "/data/summary/*.xlsx",
        "dest_host": "dest-server", "dest_port": "21", "dest_protocol": "ftps",
        "dest_user": "ftpuser", "dest_path": "/incoming/summary/",
        "password_var_name": "project-dest-password", "direction": "upload_delete",
    },
)

# Watch each transfer independently
task_wait_report = ExternalTaskSensor(
    task_id='wait_transfer_report',
    external_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    external_task_id=None, mode='reschedule', poke_interval=60, timeout=3600,
)

task_wait_summary = ExternalTaskSensor(
    task_id='wait_transfer_summary',
    external_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    external_task_id=None, mode='reschedule', poke_interval=60, timeout=3600,
)

# Both must complete before downstream proceeds
task_prev_job >> [task_trigger_report, task_trigger_summary]
task_trigger_report  >> task_wait_report
task_trigger_summary >> task_wait_summary
[task_wait_report, task_wait_summary] >> task_next_job
```

---

## Notes

- lftp must be installed on the SSH target server (ftp/ftps/sftp)
- AWS CLI must be installed on the SSH target server (s3)
- azcopy must be installed on the SSH target server (blob)
- Passwords are read from Airflow Variables at runtime — never hardcoded in the DAG
- FTPS uses explicit TLS (AUTH TLS) — port `21` or `991`
- MVS dataset names in `dest_path` (no leading `/`) are single-quoted automatically
- Blob auth: `blob_sas_var_name` → SAS token; empty → managed identity (`azcopy login --identity`)
- `wait_for_completion=True` holds a worker slot during transfer — tune `poke_interval` to reduce polling
- Wilson staging dir is scoped per run: `wilson_staging_dir/run_id` — parallel runs never collide
