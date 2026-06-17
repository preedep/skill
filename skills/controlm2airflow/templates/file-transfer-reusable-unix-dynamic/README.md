# File Transfer Reusable DAG — Unix Dynamic (lftp/azcopy via SSH)

Reusable per-project DAG for Unix file transfers. Acts like a function — all parameters
are passed at runtime by the caller via `TriggerDagRunOperator conf={}`.

One DAG handles all Unix `FILE_TRANS` jobs in the project. No infrastructure is hardcoded.
Project team owns and maintains this DAG. Isolated blast radius — changes only affect this project.

`schedule=None` — always triggered by caller via `TriggerDagRunOperator`, never runs on its own.

## Transfer Pattern

```
Caller DAG
  └── TriggerDagRunOperator(conf={source_path, dest_path, direction, ...})
        └── this DAG ({company}-{project}-{dag_name}-{env})
              └── DynamicSSHOperator → SSH target → lftp / aws / azcopy → Destination
```

### Direct (single-hop)

```
SSH target = source server
source_path lives on the SSH target
```

### Two-hop via relay (Wilson)

```
SSH target = relay/Wilson server
pull_from_source: relay pulls source_path from src_host via lftp → wilson_staging_dir
transfer_files:   relay pushes wilson_staging_dir/* → destination
```

## DAG Workflow

```
direct (src_protocol empty/local):
  start → validate_params → pre_command → pull_from_source[skip] → validate_source
        → transfer_files → verify → post_transfer_action → post_command → end

two-hop via relay (src_protocol=sftp/ftps/ftp):
  start → validate_params → pre_command → pull_from_source → validate_source[staging]
        → transfer_files[staging→dest] → verify → post_transfer_action[+wilson cleanup]
        → post_command → end

archive/cleanup:
  start → validate_params → pre_command → pull_from_source[skip] → validate_source[skip]
        → transfer_files[skip] → verify[skip] → post_transfer_action → post_command → end
```

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
| `ssh_conn_id` | ✅ | Airflow SSH conn ID to the SSH target (source server for direct; relay/Wilson for two-hop) |

#### Two-hop relay (Wilson) — leave empty for direct transfer

| Parameter | Required | Description |
|---|---|---|
| `src_protocol` | ➖ | `""` or `"local"` = direct; `"sftp"` / `"ftps"` / `"ftp"` = relay pull from src_host |
| `src_host` | ✅† | Source server hostname — †required when `src_protocol` is sftp/ftps/ftp |
| `src_port` | ✅† | Source port: `22` (sftp) or `21` (ftp/ftps) |
| `src_user` | ✅† | Source username |
| `src_password_var` | ✅† | Airflow Variable key holding source password |
| `wilson_staging_dir` | ✅† | Staging dir on relay server (e.g. `/tmp/airflow/staging/job_xyz`) |
| `wilson_cleanup` | ➖ | `"true"` (default) = delete staging files after push; `"false"` = keep |

#### Transfer

| Parameter | Required | Description |
|---|---|---|
| `source_path` | ✅ | File path or glob on source server (two-hop) or on SSH target (direct/NAS) |
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
| `blob_sas_var_name` | ➖ | Airflow Variable key holding SAS token (empty = use managed identity) |

#### Post-transfer extras

| Parameter | Required | Description |
|---|---|---|
| `new_name` | ➖ | New filename for `*_rename` / `*_move` directions |
| `archive_path` | ➖ | Archive base path (default: `dest_path/archive`) |
| `retention_days` | ➖ | Days to keep archives for `cleanup_only` (default: `30`) |
| `pre_command` | ➖ | Shell command to run before transfer (empty = skip) |
| `post_command` | ➖ | Shell command to run after transfer (empty = skip) |

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
- **Host**: Unix source server (direct) or relay/Wilson server (two-hop)
- **Username / Key**: user with SSH access

### Step 4: Add Airflow Variables (passwords)

- One Variable per FTP/SFTP password referenced by `password_var_name` or `src_password_var`
- **Value**: plaintext password — Airflow encrypts it at rest

### Step 5: Call from your business DAG

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Direct upload: source lives on SSH target
TriggerDagRunOperator(
    task_id='transfer_report',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "ssh_conn_id":      "ssh-unix-server",
        "source_path":      "/data/report/*.csv",
        "dest_host":        "dest-server",
        "dest_port":        "21",
        "dest_protocol":    "ftps",
        "dest_user":        "ftpuser",
        "dest_path":        "/incoming/report/",
        "password_var_name": "project-dest-password",
        "direction":        "upload",
    },
)

# Two-hop: relay (Wilson) pulls from src_host, then pushes to dest
TriggerDagRunOperator(
    task_id='transfer_via_relay',
    trigger_dag_id='nix-apxxxx-file-transfer-unix-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "ssh_conn_id":        "ssh-wilson-relay",
        "src_protocol":       "sftp",
        "src_host":           "source-server",
        "src_port":           "22",
        "src_user":           "srcuser",
        "src_password_var":   "project-src-password",
        "wilson_staging_dir": "/tmp/airflow/staging/transfer_report",
        "source_path":        "/exports/*.dat",
        "dest_host":          "dest-server",
        "dest_port":          "21",
        "dest_protocol":      "ftps",
        "dest_user":          "ftpuser",
        "dest_path":          "/incoming/",
        "password_var_name":  "project-dest-password",
        "direction":          "upload",
    },
)
```

## Notes

- lftp must be installed on the SSH target server (ftp/ftps/sftp)
- AWS CLI must be installed on the SSH target server (s3)
- azcopy must be installed on the SSH target server (blob)
- Passwords are read from Airflow Variables at runtime — never hardcoded in the DAG
- FTPS uses explicit TLS (AUTH TLS) — port `21` or `991`
- MVS dataset names in `dest_path` (no leading `/`) are single-quoted automatically
- Blob auth: `blob_sas_var_name` → SAS token; empty → managed identity (`azcopy login --identity`)
- `wait_for_completion=True` holds a worker slot during transfer — tune `poke_interval` to reduce polling
