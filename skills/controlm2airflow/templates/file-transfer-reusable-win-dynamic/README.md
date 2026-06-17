# File Transfer Reusable DAG — Windows Dynamic (WinSCP via PSRP)

Reusable per-project DAG for Windows file transfers. Acts like a function — infrastructure
is fixed in this DAG, per-job file paths and commands are passed by the caller at runtime.

**Project team owns and maintains this DAG. Isolated blast radius — changes only affect this project.**

**schedule=None** — always triggered by caller via `TriggerDagRunOperator`, never runs on its own.

## Transfer Pattern

```
Caller DAG
  └── TriggerDagRunOperator(conf={source_path, destination_path, ...})
        └── this DAG (scb-PROJECT-file-transfer-win-ENV)
              └── PsrpOperator → Source Windows Server → WinSCP → Destination Server
```

## Fixed vs Dynamic Parameters

| Fixed in DAG config (set once) | Passed via conf from caller (per job) |
| ------------------------------ | ------------------------------------- |
| `PSRP_CONN_ID` | `source_path` |
| `REMOTE_HOST` | `destination_path` |
| `DESTINATION_HOST` | `pre_command` |
| `DESTINATION_PORT` | `post_command` |
| `DESTINATION_PROTOCOL` | `delete_source` |
| `DESTINATION_USER` | |
| `DESTINATION_PASSWORD_SECRET` | |

## DAG Workflow

```
start → pre_command → validate_source_files → transfer_files_winscp → verify_transfer
      → delete_source_files → archive_source_files → cleanup_old_archives → post_command → end
```

> `pre_command` and `post_command` self-skip when param is empty string.
> `delete_source_files` runs when `delete_source=True`; self-skips otherwise.
> `archive_source_files` and `cleanup_old_archives` run when `delete_source=False`; self-skip otherwise.

| Task | What it does | Timeout |
| ---- | ------------ | ------- |
| `pre_command` | Runs pre-transfer command (e.g. mkdir staging) | 5 min |
| `validate_source_files` | PowerShell `Test-Path` + `Get-ChildItem` to confirm source files exist | default |
| `transfer_files_winscp` | WinSCP inline `put` | 1 hour |
| `verify_transfer` | WinSCP inline `ls` on destination | 5 min |
| `delete_source_files` | `Remove-Item` source — when `delete_source=True` (%%SRCOPT=delete) | 5 min |
| `archive_source_files` | `Move-Item` to `archive\YYYYMMDD\` — when `delete_source=False` | 10 min |
| `cleanup_old_archives` | Delete archives older than 30 days — when `delete_source=False` | 10 min |
| `post_command` | Runs post-transfer command | 5 min |

## Quick Start

### Step 1: Copy and rename this template to your project repo

```
scb_projecta_file_transfer_win_dev.py
scb_projecta_file_transfer_win_dev.json
```

### Step 2: Fill config — infrastructure only (set once)

Edit `config/dev/scb_project_file_transfer_win_dynamic.json`:

| Field | Description | Example |
| ----- | ----------- | ------- |
| `COMPANY` | Company prefix | `"scb"` |
| `PROJECT` | Project name | `"projecta"` |
| `DAG_NAME` | DAG name suffix | `"file-transfer-win"` |
| `ENV` | Environment | `"dev"` |
| `PSRP_CONN_ID` | Airflow PSRP connection ID | `"psrp_windows_server_dev"` |
| `REMOTE_HOST` | Windows source server hostname | `"windows-server.example.com"` |
| `DESTINATION_HOST` | FTP/SFTP destination server hostname | `"dest-server.example.com"` |
| `DESTINATION_PORT` | Port | `"21"` (FTP) / `"22"` (SFTP) |
| `DESTINATION_PROTOCOL` | Protocol | `"ftp"` / `"sftp"` |
| `DESTINATION_USER` | FTP/SFTP username | `"domain\\username"` |
| `DESTINATION_PASSWORD_SECRET` | Airflow Variable key for destination password | `"projecta-transfer-password"` |

### Step 3: Add Airflow Connection (PSRP)

- **Connection ID**: matches `PSRP_CONN_ID`
- **Connection Type**: PSRP
- **Host**: Windows source server hostname
- **Username / Password**: Windows user with WinRM access

### Step 4: Add Airflow Variable (destination password)

- **Key**: matches `DESTINATION_PASSWORD_SECRET`
- **Value**: FTP/SFTP password for destination server

### Step 5: Call from your business DAG (one call per FILE_TRANS job)

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Job 1: transfer daily report
task_transfer_report = TriggerDagRunOperator(
    task_id='transfer_report',
    trigger_dag_id='scb-projecta-file-transfer-win-dev',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "source_path":      r"D:\data\report\*.csv",
        "destination_path": "/incoming/report/",
        "pre_command":      "",
        "post_command":     "",
        "delete_source":    False,
    },
    dag=dag,
)

# Job 2: transfer summary (different paths, same reusable DAG)
task_transfer_summary = TriggerDagRunOperator(
    task_id='transfer_summary',
    trigger_dag_id='scb-projecta-file-transfer-win-dev',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "source_path":      r"D:\data\summary\*.xlsx",
        "destination_path": "/incoming/summary/",
        "pre_command":      r"mkdir D:\staging\summary",
        "post_command":     "",
        "delete_source":    True,
    },
    dag=dag,
)
```

## Notes

- WinSCP must be installed on the source Windows server at `C:\Program Files (x86)\WinSCP\WinSCP.com`
- Backslash in usernames (e.g. `domain\user`) is automatically URL-encoded to `domain%5Cuser` for WinSCP
- Multiple FILE_TRANS jobs with different destinations need separate reusable DAGs (one per destination)
- `wait_for_completion=True` keeps a worker slot occupied during transfer — set `poke_interval` to avoid excessive polling
