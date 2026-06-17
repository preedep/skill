# File Transfer Reusable DAG — Windows Dynamic (WinSCP/azcopy via PSRP)

Reusable per-project DAG for Windows file transfers. Acts like a function — all parameters
are passed at runtime by the caller via `TriggerDagRunOperator conf={}`.

One DAG handles all Windows `FILE_TRANS` jobs in the project. No infrastructure is hardcoded.
Project team owns and maintains this DAG. Isolated blast radius — changes only affect this project.

`schedule=None` — always triggered by caller via `TriggerDagRunOperator`, never runs on its own.

## Transfer Pattern

```
Caller DAG
  └── TriggerDagRunOperator(conf={source_path, dest_path, direction, ...})
        └── this DAG ({company}-{project}-{dag_name}-{env})
              └── DynamicPsrpOperator → Source Windows Server → WinSCP / aws / azcopy → Destination
```

## DAG Workflow

```
start → validate_params → pre_command → validate_source → transfer_files
      → verify → post_transfer_action → post_command → end
```

- `pre_command` / `post_command` self-skip when param is empty
- `validate_source` / `transfer_files` / `verify` self-skip for `archive_only` / `cleanup_only`

## Parameters

### Identity (set once in the .py file)

| Variable | Description | Example |
|---|---|---|
| `_company` | Company prefix | `"nix"` |
| `_project` | Project name | `"apxxxx"` |
| `_env` | Environment | `"nonprod"` |
| `_dag_name` | DAG name suffix | `"file-transfer-win"` |

### Runtime conf (passed per job from caller DAG)

#### Connection

| Parameter | Required | Description |
|---|---|---|
| `psrp_conn_id` | ✅ | Airflow WinRM/PSRP connection ID to the source Windows server |

#### Transfer

| Parameter | Required | Description |
|---|---|---|
| `source_path` | ✅ | File path or wildcard on source Windows server (e.g. `C:\data\*.csv`) |
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
| `archive_path` | ➖ | Archive base path (default: `dest_path\archive`) |
| `retention_days` | ➖ | Days to keep archives for `cleanup_only` (default: `30`) |
| `pre_command` | ➖ | PowerShell command to run before transfer (empty = skip) |
| `post_command` | ➖ | PowerShell command to run after transfer (empty = skip) |

## direction values

| Value | Protocol | What happens |
|---|---|---|
| `upload` | ftp/ftps/sftp | WinSCP put source → dest, keep source (SRCOPT=0) |
| `upload_delete` | ftp/ftps/sftp | WinSCP put source → dest, delete source after (SRCOPT=1) |
| `upload_rename` | ftp/ftps/sftp | WinSCP put source → dest, rename source to `new_name` (SRCOPT=2) |
| `upload_move` | ftp/ftps/sftp | WinSCP put source → dest, move source to `new_name` path (SRCOPT=3) |
| `download` | ftp/ftps/sftp | WinSCP get remote → local, keep remote (SRCOPT=0) |
| `download_delete` | ftp/ftps/sftp | WinSCP get remote → local, delete remote after (SRCOPT=1) |
| `download_rename` | ftp/ftps/sftp | WinSCP get remote → local, rename remote to `new_name` (SRCOPT=2) |
| `download_move` | ftp/ftps/sftp | WinSCP get remote → local, move remote to `new_name` path (SRCOPT=3) |
| `download_archive` | ftp/ftps/sftp | WinSCP get remote → local, archive locally by date |
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
{company}_{project}_file_transfer_win_{env}.py
```

### Step 2: Edit identity variables at the top of the .py

```python
_company  = "nix"
_project  = "apxxxx"
_env      = "nonprod"
_dag_name = "file-transfer-win"
```

### Step 3: Add Airflow Connection (PSRP/WinRM)

- **Connection ID**: value you will pass as `psrp_conn_id` in conf
- **Connection Type**: PSRP
- **Host**: Windows source server hostname
- **Username / Password**: Windows user with WinRM access (use `domain\\username` format)

### Step 4: Add Airflow Variable (destination password)

- **Key**: value you will pass as `password_var_name` in conf
- **Value**: FTP/SFTP password — Airflow encrypts it at rest

### Step 5: Call from your business DAG

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Upload Windows files to FTP destination
TriggerDagRunOperator(
    task_id='transfer_report',
    trigger_dag_id='nix-apxxxx-file-transfer-win-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "psrp_conn_id":     "psrp-windows-server",
        "source_path":      r"D:\data\report\*.csv",
        "dest_host":        "dest-server",
        "dest_port":        "21",
        "dest_protocol":    "ftps",
        "dest_user":        "ftpuser",
        "dest_path":        "/incoming/report/",
        "password_var_name": "project-dest-password",
        "direction":        "upload",
    },
)

# Upload to Azure Blob using managed identity
TriggerDagRunOperator(
    task_id='transfer_to_blob',
    trigger_dag_id='nix-apxxxx-file-transfer-win-nonprod',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "psrp_conn_id":   "psrp-windows-server",
        "source_path":    r"D:\exports\*.dat",
        "dest_protocol":  "blob",
        "blob_account":   "mystorageacct",
        "blob_container": "data-imports",
        "blob_prefix":    "daily/incoming/",
        "direction":      "blob_upload",
    },
)
```

## Notes

- WinSCP must be installed on the source Windows server at `C:\Program Files (x86)\WinSCP\WinSCP.com`
- AWS CLI must be installed on the source Windows server (s3)
- azcopy must be installed on the source Windows server (blob)
- Passwords are read from Airflow Variables at runtime — never hardcoded in the DAG
- Backslash in usernames (e.g. `domain\user`) is automatically URL-encoded to `domain%5Cuser` for WinSCP
- FTPS uses explicit TLS (AUTH TLS) — port `21` or `991`
- Blob auth: `blob_sas_var_name` → SAS token; empty → managed identity (`azcopy login --identity`)
- `wait_for_completion=True` holds a worker slot during transfer — tune `poke_interval` to reduce polling
