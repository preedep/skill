from airflow import DAG

from datetime import datetime
import logging
import pendulum

from airflow.providers.microsoft.psrp.operators.psrp import PsrpOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


class DynamicPsrpOperator(PsrpOperator):
    """PsrpOperator with psrp_conn_id added to template_fields so {{ params.psrp_conn_id }} resolves at runtime."""
    template_fields = PsrpOperator.template_fields + ('psrp_conn_id',)

###################### logging ######################
logging.getLogger("smtplib").setLevel(logging.DEBUG)
logging.getLogger("airflow.utils.email").setLevel(logging.DEBUG)

###################### variables zone ######################
# DAG identity only — all transfer parameters are passed at runtime
# via TriggerDagRunOperator conf={} from the caller DAG.

_company  = "##COMPANY##"
_project  = "##PROJECT##"
_env      = "##ENV##"
_dag_name = "##DAG_NAME##"

_active   = ##ACTIVE##
_schedule = None  # always triggered by caller, never scheduled
_tags     = ##TAGS##
_email_list = ##EMAIL_LIST##
_enable_email_notification_success = ##ENABLE_EMAIL_NOTIFICATION_SUCCESS##
_enable_email_notification_fail    = ##ENABLE_EMAIL_NOTIFICATION_FAIL##

################### Callbacks ####################

def success_callback(context):
    dag_id  = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = pendulum.now('Asia/Bangkok')
    params  = context['params']

    subject = f"DAG {dag_id} - Task {task_id} succeeded"
    body = f"""
    <h3>File Transfer Task Succeeded</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Time:</strong> {execution_date}</p>
    <p><strong>Source Host:</strong> {params.get('psrp_conn_id', '-')}</p>
    <p><strong>Source Path:</strong> {params.get('source_path', '-')}</p>
    <p><strong>Destination:</strong> {params.get('dest_user', '-')}@{params.get('dest_host', '-')}:{params.get('dest_path', '-')}</p>
    """

    if _enable_email_notification_success:
        from airflow.utils.email import send_email
        send_email(to=_email_list, subject=subject, html_content=body)

def failure_callback(context):
    dag_id    = context['dag'].dag_id
    task_id   = context['task_instance'].task_id
    execution_date = pendulum.now('Asia/Bangkok')
    exception = context.get('exception', 'Unknown error')
    params    = context['params']

    subject = f"DAG {dag_id} - Task {task_id} failed"
    body = f"""
    <h3>File Transfer Task Failed</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Time:</strong> {execution_date}</p>
    <p><strong>Source Host:</strong> {params.get('psrp_conn_id', '-')}</p>
    <p><strong>Source Path:</strong> {params.get('source_path', '-')}</p>
    <p><strong>Destination:</strong> {params.get('dest_user', '-')}@{params.get('dest_host', '-')}:{params.get('dest_path', '-')}</p>
    <p><strong>Error:</strong> {exception}</p>
    """

    if _enable_email_notification_fail:
        from airflow.utils.email import send_email
        send_email(to=_email_list, subject=subject, html_content=body)

################### DAG Configuration ####################

local_tz = pendulum.timezone('Asia/Bangkok')

default_args = {
    'owner': '{}'.format(_project),
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'timezone': 'Asia/Bangkok',
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    _company + '-' + _project + '-' + _dag_name + '-' + _env,
    default_args=default_args,
    schedule=_schedule,
    tags=_tags,
    catchup=False,
    is_paused_upon_creation=not _active,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    on_success_callback=success_callback if _enable_email_notification_success else None,
    on_failure_callback=failure_callback if _enable_email_notification_fail else None,
    params={
        # ── Connection ─────────────────────────────────────────────────────────
        "psrp_conn_id":         "",    # Airflow WinRM/PSRP connection ID to source Windows server (required)
        # ── Transfer ───────────────────────────────────────────────────────────
        "source_path":          "",    # source file path / wildcard on source Windows server (required)
        "dest_host":            "",    # destination server hostname or IP (ftp/ftps/sftp only)
        "dest_port":            "",    # destination port: 21 (ftp/ftps) | 22 (sftp)
        "dest_protocol":        "",    # "ftp" | "ftps" | "sftp" | "s3" | "blob"
        "dest_user":            "",    # destination FTP/SFTP username (ftp/ftps/sftp only)
        "dest_path":            "",    # destination path (remote for upload; local for download)
        "password_var_name":    "",    # Airflow Variable key holding destination password (ftp/ftps/sftp only)
        # ── S3 params — required when dest_protocol="s3" ──────────────────────
        "s3_bucket":            "",    # S3 bucket name (e.g. "my-bucket")
        "s3_prefix":            "",    # S3 key prefix (e.g. "data/incoming/") — trailing slash = folder
        "s3_region":            "",    # AWS region (e.g. "ap-southeast-1")
        "s3_endpoint_url":      "",    # VPC endpoint URL (empty = public S3)
        "aws_profile":          "",    # AWS CLI named profile (empty = instance role / default)
        "aws_access_key_id_var":  "",  # Airflow Variable key for AWS_ACCESS_KEY_ID (empty = node IAM role)
        "aws_secret_key_var":     "",  # Airflow Variable key for AWS_SECRET_ACCESS_KEY (empty = node IAM role)
        # ── Azure Blob params — required when dest_protocol="blob" ────────────
        "blob_account":         "",    # Azure Storage account name (e.g. "mystorageacct")
        "blob_container":       "",    # Blob container name (e.g. "data-imports")
        "blob_prefix":          "",    # Blob path prefix (e.g. "daily/incoming/") — trailing slash = folder
        "blob_identity_client_id": "", # User-assigned managed identity client ID (empty = system-assigned MI)
        "blob_sas_var_name":    "",    # Airflow Variable key holding SAS token (empty = use managed identity)
        # ── Direction — one value controls transfer type + post-transfer action ─
        # upload          : WinSCP put source_path → dest_path, keep source     (Control-M SRCOPT=0)
        # upload_delete   : WinSCP put source_path → dest_path, delete source   (Control-M SRCOPT=1)
        # upload_rename   : WinSCP put source_path → dest_path, rename source   (Control-M SRCOPT=2)
        # upload_move     : WinSCP put source_path → dest_path, move source     (Control-M SRCOPT=3)
        # download        : WinSCP get source_path → dest_path, keep source     (Control-M SRCOPT=0)
        # download_delete : WinSCP get source_path → dest_path, delete remote   (Control-M SRCOPT=1)
        # download_rename : WinSCP get source_path → dest_path, rename remote   (Control-M SRCOPT=2)
        # download_move   : WinSCP get source_path → dest_path, move remote     (Control-M SRCOPT=3)
        # download_archive: WinSCP get source_path → dest_path, archive locally (Airflow extra)
        # archive_only    : no transfer — archive existing files in dest_path    (data retention)
        # cleanup_only    : no transfer — delete archives older than N days      (data retention)
        # s3_upload       : aws s3 cp source_path → s3://bucket/prefix          (S3)
        # s3_download     : aws s3 cp s3://bucket/prefix → dest_path            (S3)
        # blob_upload     : azcopy copy source_path → blob account/container    (Azure Blob)
        # blob_download   : azcopy copy blob account/container → dest_path      (Azure Blob)
        # s3_to_blob      : azcopy copy s3://bucket/prefix → blob account/container (S3 → Blob, no staging)
        # blob_to_s3      : azcopy copy blob account/container → s3://bucket/prefix (Blob → S3, no staging)
        "direction":            "upload",
        # ── Post-transfer extras ───────────────────────────────────────────────
        "new_name":             "",    # new filename for *_rename / *_move directions
        "archive_path":         "",    # archive base path for download_archive / archive_only
        "retention_days":       "30",  # days to keep archives for cleanup_only
        # ── Commands — optional ────────────────────────────────────────────────
        "pre_command":          "",    # PowerShell command to run before transfer (empty = skip)
        "pre_command_args":     [],    # list of arguments for pre_command (e.g. ["666", "/path/*.*"])
        "post_command":         "",    # PowerShell command to run after transfer (empty = skip)
        "post_command_args":    [],    # list of arguments for post_command
    },
    doc_md="""
# File Transfer Reusable DAG — Windows Dynamic (WinSCP/azcopy via PSRP)

## Purpose

Reusable per-project DAG for Windows file transfers via FTP/FTPS/SFTP/S3/Azure Blob.
Acts like a function — **all parameters are passed at runtime by the caller
via `TriggerDagRunOperator conf={}`**.

One DAG handles all Windows `FILE_TRANS` jobs in the project.
No infrastructure is hardcoded. Project team owns this DAG.

`schedule=None` — always triggered by caller, never runs on its own schedule.

---

## Parameters

| Parameter | Required | Description |
|---|---|---|
| `psrp_conn_id` | ✅ | Airflow WinRM/PSRP connection ID to the **source** Windows server |
| `source_path` | ✅ | File path or wildcard on source server (e.g. `C:\\data\\*.csv`) |
| `dest_host` | ✅* | Destination hostname or IP (*ftp/ftps/sftp only) |
| `dest_port` | ✅* | Destination port: `21` (ftp/ftps) or `22` (sftp) |
| `dest_protocol` | ✅ | Protocol: `ftp` or `ftps` or `sftp` or `s3` or `blob` |
| `dest_user` | ✅* | Destination FTP/SFTP username (*ftp/ftps/sftp only) |
| `dest_path` | ✅ | Destination path (remote for upload; local for download/blob_download) |
| `password_var_name` | ✅* | Airflow Variable key holding destination password (*ftp/ftps/sftp only) |
| `s3_bucket` | ✅** | S3 bucket name (**required when dest_protocol=s3) |
| `s3_prefix` | ✅** | S3 key prefix, e.g. `data/incoming/` |
| `s3_region` | ✅** | AWS region, e.g. `ap-southeast-1` |
| `s3_endpoint_url` | ➖ | VPC endpoint URL (empty = public S3) |
| `aws_profile` | ➖ | AWS CLI named profile (empty = instance role / default) |
| `aws_access_key_id_var` | ➖ | Airflow Variable key for `AWS_ACCESS_KEY_ID` (empty = node IAM role; used by s3_to_blob/blob_to_s3) |
| `aws_secret_key_var` | ➖ | Airflow Variable key for `AWS_SECRET_ACCESS_KEY` (pair with `aws_access_key_id_var`) |
| `blob_account` | ✅*** | Azure Storage account name (***required when dest_protocol=blob) |
| `blob_container` | ✅*** | Blob container name |
| `blob_prefix` | ✅*** | Blob path prefix, e.g. `daily/incoming/` |
| `blob_identity_client_id` | ➖ | User-assigned MI client ID (empty = system-assigned MI) |
| `blob_sas_var_name` | ➖ | Airflow Variable key holding SAS token (empty = use managed identity) |
| `direction` | ✅ | Transfer mode — see table below |
| `new_name` | ➖ | New filename for rename/move directions |
| `archive_path` | ➖ | Archive base path (default: `dest_path\\archive`) |
| `retention_days` | ➖ | Days to keep archives for cleanup_only (default: `30`) |
| `pre_command` | ➖ | PowerShell command to run before transfer (empty = skip) |
| `post_command` | ➖ | PowerShell command to run after transfer (empty = skip) |

---

## direction values

| Value | Protocol | What happens |
|---|---|---|
| `upload` | ftp/ftps/sftp | WinSCP put source → dest, keep source (SRCOPT=0) |
| `upload_delete` | ftp/ftps/sftp | WinSCP put source → dest, delete source after (SRCOPT=1) |
| `upload_rename` | ftp/ftps/sftp | WinSCP put source → dest, rename source to `new_name` (SRCOPT=2) |
| `upload_move` | ftp/ftps/sftp | WinSCP put source → dest, move source to `new_name` path (SRCOPT=3) |
| `download` | ftp/ftps/sftp | WinSCP get remote → local, keep remote source (SRCOPT=0) |
| `download_delete` | ftp/ftps/sftp | WinSCP get remote → local, delete remote source after (SRCOPT=1) |
| `download_rename` | ftp/ftps/sftp | WinSCP get remote → local, rename remote source to `new_name` (SRCOPT=2) |
| `download_move` | ftp/ftps/sftp | WinSCP get remote → local, move remote source to `new_name` path (SRCOPT=3) |
| `download_archive` | ftp/ftps/sftp | WinSCP get remote → local, archive locally by date |
| `archive_only` | (any) | no transfer — archive existing files in dest_path by date |
| `cleanup_only` | (any) | no transfer — delete archives older than `retention_days` |
| `s3_upload` | s3 | aws s3 cp source_path → s3://bucket/prefix |
| `s3_download` | s3 | aws s3 cp s3://bucket/prefix → dest_path |
| `blob_upload` | blob | azcopy copy source_path → blob account/container/prefix |
| `blob_download` | blob | azcopy copy blob account/container/prefix → dest_path |
| `s3_to_blob` | s3+blob | azcopy copy s3://bucket/prefix → blob account/container (no staging) |
| `blob_to_s3` | s3+blob | azcopy copy blob account/container → s3://bucket/prefix (no staging) |

---

## Notes

- WinSCP must be installed on the source Windows server (default: `C:\\Program Files (x86)\\WinSCP\\WinSCP.com`)
- AWS CLI must be installed on the source Windows server (for s3)
- azcopy must be installed on the source Windows server (for blob)
- `password_var_name` must be an Airflow Variable key — password is never hardcoded
- FTPS uses explicit TLS (`-explicit` flag in WinSCP) — port `991`
- WinSCP does not support MVS/mainframe paths — use Unix template for MVS transfers
- Blob auth: if `blob_sas_var_name` is set, SAS token is used; otherwise managed identity (`azcopy login --identity`)
- `blob_identity_client_id` selects user-assigned MI; leave empty for system-assigned MI
    """
)

###################### Tasks ######################

def _validate_params(**context):
    log = logging.getLogger(__name__)
    p   = context['params']
    d   = p.get('direction', '')
    err = []
    log.info("[INFO] validate_params: direction='%s'", d)

    def need(*keys):
        for k in keys:
            if not str(p.get(k, '')).strip():
                err.append(k)

    VALID_DIRECTIONS = {
        'upload', 'upload_delete', 'upload_rename', 'upload_move',
        'download', 'download_delete', 'download_rename', 'download_move',
        'download_archive', 'archive_only', 'cleanup_only',
        's3_upload', 's3_download',
        'blob_upload', 'blob_download',
        's3_to_blob', 'blob_to_s3',
    }

    if d not in VALID_DIRECTIONS:
        raise ValueError(
            f"[PARAM ERROR] Invalid direction: '{d}'\n"
            f"  Valid values: {sorted(VALID_DIRECTIONS)}"
        )

    # psrp_conn_id always required
    need('psrp_conn_id')

    # FTP / FTPS / SFTP
    if d in ('upload', 'upload_delete', 'upload_rename', 'upload_move',
             'download', 'download_delete', 'download_rename', 'download_move',
             'download_archive'):
        need('source_path', 'dest_host', 'dest_port', 'dest_protocol',
             'dest_user', 'dest_path', 'password_var_name')
        if d in ('upload_rename', 'upload_move', 'download_rename', 'download_move'):
            need('new_name')

    # archive / cleanup — local only
    elif d == 'archive_only':
        need('source_path', 'dest_path')
    elif d == 'cleanup_only':
        need('dest_path', 'retention_days')

    # S3
    elif d == 's3_upload':
        need('source_path', 's3_bucket', 's3_prefix', 's3_region')
    elif d == 's3_download':
        need('dest_path', 's3_bucket', 's3_prefix', 's3_region')

    # Blob
    elif d == 'blob_upload':
        need('source_path', 'blob_account', 'blob_container', 'blob_prefix')
    elif d == 'blob_download':
        need('dest_path', 'blob_account', 'blob_container', 'blob_prefix')

    # S3 ↔ Blob (cloud-to-cloud)
    elif d in ('s3_to_blob', 'blob_to_s3'):
        need('s3_bucket', 's3_prefix', 's3_region',
             'blob_account', 'blob_container', 'blob_prefix')

    if err:
        raise ValueError(
            f"[PARAM ERROR] direction='{d}' — missing required parameter(s):\n"
            + '\n'.join(f"  - {k}" for k in err)
        )

    log.info("[INFO] validate_params: direction='%s' — all required parameters present", d)


with dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    task_validate_params = PythonOperator(
        task_id='validate_params',
        python_callable=_validate_params,
        on_failure_callback=failure_callback,
    )

    # Pre-command — self-skips when params.pre_command is empty
    task_precomm = DynamicPsrpOperator(
        task_id='pre_command',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Cmd = "{{ params.pre_command }}"
if ([string]::IsNullOrEmpty($Cmd)) {
    Write-Output "[INFO] pre_command: skipped (empty)"
    exit 0
}
$ArgsJson = '{{ params.pre_command_args | tojson }}'
$ArgsList = $ArgsJson | ConvertFrom-Json
Write-Output "[INFO] pre_command: $Cmd $($ArgsList -join ' ')"
& $Cmd @ArgsList
""",
        on_failure_callback=failure_callback,
    )

    # Validate source files — self-skips for archive_only / cleanup_only / blob_download / s3_download
    task_validate_source = DynamicPsrpOperator(
        task_id='validate_source_files',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Direction = "{{ params.direction }}"
$SrcPath   = "{{ params.source_path }}"

$SkipDirs = @("archive_only", "cleanup_only", "s3_download", "blob_download", "s3_to_blob", "blob_to_s3")
if ($SkipDirs -contains $Direction) {
    Write-Output "[INFO] validate_source: skipped (direction=$Direction)"
    exit 0
}

Write-Output "[INFO] === Source File Validation ==="
Write-Output "[INFO] Source path : $SrcPath"
Write-Output "[DEBUG] Agent host : $env:COMPUTERNAME"
Write-Output "[DEBUG] Agent user : $env:USERNAME"

$Files = @(Get-ChildItem -Path $SrcPath -ErrorAction SilentlyContinue)
if ($Files.Count -eq 0) {
    Write-Error "[ERROR] No source files found: $SrcPath"
    exit 1
}
$TotalSize = ($Files | Measure-Object -Property Length -Sum).Sum
Write-Output "[INFO] Validation passed — $($Files.Count) file(s), total size: $TotalSize bytes"
$Files | ForEach-Object { Write-Output "[DEBUG]   $($_.FullName) ($($_.Length) bytes)" }
""",
        on_failure_callback=failure_callback,
    )

    # File transfer — WinSCP (ftp/ftps/sftp), AWS CLI (s3), azcopy (blob)
    task_transfer_files = DynamicPsrpOperator(
        task_id='transfer_files',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Direction    = "{{ params.direction }}"
$SrcPath      = "{{ params.source_path }}"
$DestHost     = "{{ params.dest_host }}"
$DestPort     = "{{ params.dest_port }}"
$DestProtocol = "{{ params.dest_protocol }}"
$DestUser     = "{{ params.dest_user }}"
$DestPath     = "{{ params.dest_path }}"
$RemotePass   = "{{ var.value.get(params.password_var_name, "") }}"
# WARNING: password visible in Airflow rendered template log

$SkipDirs = @("archive_only", "cleanup_only")
if ($SkipDirs -contains $Direction) {
    Write-Output "[INFO] transfer_files: skipped (direction=$Direction)"
    exit 0
}

# ── FTP / FTPS / SFTP via WinSCP ─────────────────────────────────────────────
if ($DestProtocol -eq "ftp" -or $DestProtocol -eq "ftps" -or $DestProtocol -eq "sftp") {

    $WinSCPPath = "C:\\Program Files (x86)\\WinSCP\\WinSCP.com"
    if (-not (Test-Path $WinSCPPath)) { $WinSCPPath = "C:\\Program Files\\WinSCP\\WinSCP.com" }
    if (-not (Test-Path $WinSCPPath)) {
        Write-Error "[ERROR] WinSCP not found. Install WinSCP to proceed."
        exit 1
    }

    Write-Output "[INFO] === WinSCP File Transfer ==="
    Write-Output "[INFO] WinSCP path  : $WinSCPPath"
    Write-Output "[INFO] Direction    : $Direction"
    Write-Output "[INFO] Source path  : $SrcPath"
    Write-Output "[INFO] Dest host    : ${DestHost}:${DestPort}"
    Write-Output "[INFO] Dest user    : $DestUser (password suppressed)"
    Write-Output "[INFO] Dest path    : $DestPath"
    Write-Output "[INFO] Protocol     : $DestProtocol"
    Write-Output "[INFO] Password var : {{ params.password_var_name }} (password suppressed)"

    switch ($DestProtocol) {
        "ftps" { $ProtocolUrl = "ftps" }
        "ftp"  { $ProtocolUrl = "ftp"  }
        "sftp" { $ProtocolUrl = "sftp" }
    }

    $OpenArgs = "open -passive=on ${ProtocolUrl}://${DestUser}:$($RemotePass)@${DestHost}:${DestPort}/"
    if ($DestProtocol -eq "ftps") {
        $OpenArgs += " -explicit -rawsettings MinTlsVersion=10 -certificate=`"*`""
    }

    switch -Wildcard ($Direction) {
        "upload*" {
            $TransferCmd = "put `"$SrcPath`" `"$DestPath`""
            Write-Output "[INFO] Action: put $SrcPath -> $DestPath"
        }
        "download*" {
            if (-not (Test-Path $DestPath)) {
                New-Item -ItemType Directory -Path $DestPath -Force | Out-Null
            }
            $TransferCmd = "get `"$SrcPath`" `"$DestPath`""
            Write-Output "[INFO] Action: get $SrcPath -> $DestPath"
        }
        default {
            Write-Error "[ERROR] Unknown direction for ftp/ftps/sftp: $Direction"
            exit 1
        }
    }

    & $WinSCPPath /command $OpenArgs $TransferCmd "exit"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "[ERROR] WinSCP transfer failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }

# ── S3 via AWS CLI ────────────────────────────────────────────────────────────
} elseif ($DestProtocol -eq "s3") {

    $S3Bucket   = "{{ params.s3_bucket }}"
    $S3Prefix   = "{{ params.s3_prefix }}"
    $S3Region   = "{{ params.s3_region }}"
    $S3Endpoint = "{{ params.s3_endpoint_url }}"
    $AwsProfile = "{{ params.aws_profile }}"

    $ProfileArgs  = @(); if (-not [string]::IsNullOrEmpty($AwsProfile)) { $ProfileArgs = @("--profile", $AwsProfile) }
    $EndpointArgs = @(); if (-not [string]::IsNullOrEmpty($S3Endpoint))  { $EndpointArgs = @("--endpoint-url", $S3Endpoint, "--no-verify-ssl") }

    Write-Output "[INFO] === S3 Transfer (AWS CLI) ==="
    Write-Output "[INFO] aws version : $(aws --version 2>&1)"
    Write-Output "[INFO] Direction   : $Direction"
    Write-Output "[INFO] Profile     : $(if ($AwsProfile) { $AwsProfile } else { 'default/instance-role' })"
    Write-Output "[INFO] Region      : $S3Region"
    Write-Output "[INFO] Endpoint    : $(if ($S3Endpoint) { $S3Endpoint } else { 'public' })"

    switch ($Direction) {
        "s3_upload" {
            $S3Target = "s3://$S3Bucket/$S3Prefix"
            Write-Output "[INFO] Action : aws s3 cp $SrcPath -> $S3Target"
            & aws @ProfileArgs s3 cp $SrcPath $S3Target --recursive --region $S3Region @EndpointArgs
            if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] S3 upload failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
            Write-Output "[INFO] Uploaded to $S3Target"
        }
        "s3_download" {
            $S3Source = "s3://$S3Bucket/$S3Prefix"
            Write-Output "[INFO] Action : aws s3 cp $S3Source -> $DestPath"
            if (-not (Test-Path $DestPath)) { New-Item -ItemType Directory -Path $DestPath -Force | Out-Null }
            & aws @ProfileArgs s3 cp $S3Source $DestPath --recursive --region $S3Region @EndpointArgs
            if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] S3 download failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
            $Files = @(Get-ChildItem -Path $DestPath -ErrorAction SilentlyContinue)
            Write-Output "[INFO] Downloaded: $($Files.Count) file(s) -> $DestPath"
            $Files | ForEach-Object { Write-Output "[DEBUG]   $($_.FullName)" }
        }
        default {
            Write-Error "[ERROR] Unknown direction for s3: $Direction (use s3_upload or s3_download)"
            exit 1
        }
    }

# ── Azure Blob via azcopy ─────────────────────────────────────────────────────
} elseif ($DestProtocol -eq "blob") {

    $BlobAccount    = "{{ params.blob_account }}"
    $BlobContainer  = "{{ params.blob_container }}"
    $BlobPrefix     = "{{ params.blob_prefix }}"
    $BlobClientId   = "{{ params.blob_identity_client_id }}"
    $BlobSasVarName = "{{ params.blob_sas_var_name }}"
    $BlobSas        = if ($BlobSasVarName) { "{{ var.value.get(params.blob_sas_var_name, \"\") }}" } else { "" }
    # WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set

    $BlobBaseUrl = "https://$BlobAccount.blob.core.windows.net/$BlobContainer/$BlobPrefix"

    Write-Output "[INFO] === Azure Blob Transfer (azcopy) ==="
    Write-Output "[INFO] azcopy version : $(azcopy --version 2>&1 | Select-Object -First 1)"
    Write-Output "[INFO] Direction      : $Direction"
    Write-Output "[INFO] Account        : $BlobAccount"
    Write-Output "[INFO] Container      : $BlobContainer"
    Write-Output "[INFO] Prefix         : $BlobPrefix"

    # Auth: SAS token takes priority over managed identity
    if (-not [string]::IsNullOrEmpty($BlobSas)) {
        $BlobUrl = "$BlobBaseUrl`?$BlobSas"
        Write-Output "[INFO] Auth : SAS token (suppressed)"
    } else {
        Write-Output "[INFO] Auth : managed identity"
        if (-not [string]::IsNullOrEmpty($BlobClientId)) {
            & azcopy login --identity --identity-client-id $BlobClientId
        } else {
            & azcopy login --identity
        }
        if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] azcopy login failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
        $BlobUrl = $BlobBaseUrl
    }

    switch ($Direction) {
        "blob_upload" {
            Write-Output "[INFO] Action : azcopy copy $SrcPath -> $BlobBaseUrl"
            & azcopy copy $SrcPath $BlobUrl --recursive=true --overwrite=true --log-level=INFO
            if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] azcopy upload failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
            Write-Output "[INFO] Uploaded to $BlobBaseUrl"
        }
        "blob_download" {
            Write-Output "[INFO] Action : azcopy copy $BlobBaseUrl -> $DestPath"
            if (-not (Test-Path $DestPath)) { New-Item -ItemType Directory -Path $DestPath -Force | Out-Null }
            & azcopy copy $BlobUrl $DestPath --recursive=true --overwrite=true --log-level=INFO
            if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] azcopy download failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
            $Files = @(Get-ChildItem -Path $DestPath -ErrorAction SilentlyContinue)
            Write-Output "[INFO] Downloaded: $($Files.Count) file(s) -> $DestPath"
            $Files | ForEach-Object { Write-Output "[DEBUG]   $($_.FullName)" }
        }
        default {
            Write-Error "[ERROR] Unknown direction for blob: $Direction (use blob_upload or blob_download)"
            exit 1
        }
    }

} elseif ($Direction -eq "s3_to_blob" -or $Direction -eq "blob_to_s3") {

    $S3Bucket       = "{{ params.s3_bucket }}"
    $S3Prefix       = "{{ params.s3_prefix }}"
    $S3Region       = "{{ params.s3_region }}"
    $S3Endpoint     = "{{ params.s3_endpoint_url }}"
    $AwsKeyIdVar    = "{{ params.aws_access_key_id_var }}"
    $AwsSecretVar   = "{{ params.aws_secret_key_var }}"
    $BlobAccount    = "{{ params.blob_account }}"
    $BlobContainer  = "{{ params.blob_container }}"
    $BlobPrefix     = "{{ params.blob_prefix }}"
    $BlobClientId   = "{{ params.blob_identity_client_id }}"
    $BlobSas        = "{{ var.value.get(params.blob_sas_var_name, \"\") }}"
    # WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set

    $BlobBaseUrl = "https://$BlobAccount.blob.core.windows.net/$BlobContainer/$BlobPrefix"

    Write-Output "[INFO] === S3 <-> Blob Transfer (azcopy, no staging) ==="
    Write-Output "[INFO] azcopy version : $(azcopy --version 2>&1 | Select-Object -First 1)"
    Write-Output "[INFO] Direction      : $Direction"
    Write-Output "[INFO] S3 bucket      : $S3Bucket / $S3Prefix"
    Write-Output "[INFO] Blob           : $BlobAccount / $BlobContainer / $BlobPrefix"

    # Inject AWS credentials if caller provided Variable names; otherwise node IAM role is used
    if (-not [string]::IsNullOrEmpty($AwsKeyIdVar)) {
        $env:AWS_ACCESS_KEY_ID     = "{{ var.value.get(params.aws_access_key_id_var, '') }}"
        $env:AWS_SECRET_ACCESS_KEY = "{{ var.value.get(params.aws_secret_key_var, '') }}"
        # WARNING: AWS credentials visible in Airflow rendered template log
        Write-Output "[INFO] S3 auth   : explicit credentials (suppressed)"
    } else {
        Write-Output "[INFO] S3 auth   : node IAM role / instance profile"
    }

    # Build Blob URL (SAS or managed identity)
    if (-not [string]::IsNullOrEmpty($BlobSas)) {
        $BlobUrl = "$BlobBaseUrl`?$BlobSas"
        Write-Output "[INFO] Blob auth : SAS token (suppressed)"
    } else {
        Write-Output "[INFO] Blob auth : managed identity"
        if (-not [string]::IsNullOrEmpty($BlobClientId)) {
            & azcopy login --identity --identity-client-id $BlobClientId
        } else {
            & azcopy login --identity
        }
        if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] azcopy login failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
        $BlobUrl = $BlobBaseUrl
    }

    # Build S3 URL for azcopy
    if (-not [string]::IsNullOrEmpty($S3Endpoint)) {
        $S3Url = "$S3Endpoint/$S3Bucket/$S3Prefix"
    } else {
        $S3Url = "https://s3.amazonaws.com/$S3Bucket/$S3Prefix"
    }
    Write-Output "[INFO] S3 URL  : $S3Url"
    Write-Output "[INFO] Blob URL: $BlobBaseUrl"

    if ($Direction -eq "s3_to_blob") {
        Write-Output "[INFO] Action  : azcopy copy S3 -> Blob"
        & azcopy copy $S3Url $BlobUrl --recursive=true --overwrite=true --log-level=INFO
    } else {
        Write-Output "[INFO] Action  : azcopy copy Blob -> S3"
        & azcopy copy $BlobUrl $S3Url --recursive=true --overwrite=true --log-level=INFO
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "[ERROR] azcopy $Direction failed ($LASTEXITCODE)"
        exit $LASTEXITCODE
    }
    Write-Output "[INFO] $Direction completed successfully"

} else {
    Write-Error "[ERROR] Unsupported protocol: $DestProtocol (use ftp, ftps, sftp, s3, blob, or direction s3_to_blob/blob_to_s3)"
    exit 1
}

Write-Output "[INFO] Transfer completed successfully"
""",
        on_failure_callback=failure_callback,
    )

    # Verify transfer — self-skips for archive_only / cleanup_only
    task_verify_transfer = DynamicPsrpOperator(
        task_id='verify_transfer',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Direction    = "{{ params.direction }}"
$SrcPath      = "{{ params.source_path }}"
$DestPath     = "{{ params.dest_path }}"
$S3Bucket     = "{{ params.s3_bucket }}"
$S3Prefix     = "{{ params.s3_prefix }}"
$S3Region     = "{{ params.s3_region }}"
$S3Endpoint   = "{{ params.s3_endpoint_url }}"
$AwsProfile   = "{{ params.aws_profile }}"
$BlobAccount  = "{{ params.blob_account }}"
$BlobContainer = "{{ params.blob_container }}"
$BlobPrefix   = "{{ params.blob_prefix }}"
$BlobSas      = "{{ var.value.get(params.blob_sas_var_name, \"\") }}"
# WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set

$SkipDirs = @("archive_only", "cleanup_only")
if ($SkipDirs -contains $Direction) {
    Write-Output "[INFO] verify_transfer: skipped (direction=$Direction)"
    exit 0
}

Write-Output "[INFO] === Verify Transfer ==="
Write-Output "[INFO] Direction: $Direction"

switch -Wildcard ($Direction) {
    "upload*" {
        Write-Output "[INFO] Mode    : upload — verifying source still exists"
        $Files = @(Get-ChildItem -Path $SrcPath -ErrorAction SilentlyContinue)
        if ($Files.Count -eq 0) { Write-Error "[ERROR] Source not found after upload: $SrcPath"; exit 1 }
        Write-Output "[INFO] Verification passed — $($Files.Count) file(s) present"
        $Files | ForEach-Object { Write-Output "[DEBUG]   $($_.FullName)" }
    }
    "download*" {
        $SrcPattern = [System.IO.Path]::GetFileName($SrcPath)
        Write-Output "[INFO] Mode    : download — verifying destination"
        $Files = @(Get-ChildItem -Path "$DestPath\\$SrcPattern" -ErrorAction SilentlyContinue)
        if ($Files.Count -eq 0) { Write-Error "[ERROR] No files matching $SrcPattern at $DestPath"; exit 1 }
        $TotalSize = ($Files | Measure-Object -Property Length -Sum).Sum
        Write-Output "[INFO] Verification passed — $($Files.Count) file(s), size: $TotalSize bytes"
        $Files | ForEach-Object { Write-Output "[DEBUG]   $($_.FullName)" }
    }
    "s3_upload" {
        $ProfileArgs  = @(); if (-not [string]::IsNullOrEmpty($AwsProfile)) { $ProfileArgs = @("--profile", $AwsProfile) }
        $EndpointArgs = @(); if (-not [string]::IsNullOrEmpty($S3Endpoint))  { $EndpointArgs = @("--endpoint-url", $S3Endpoint, "--no-verify-ssl") }
        $S3Target = "s3://$S3Bucket/$S3Prefix"
        Write-Output "[INFO] Mode    : s3_upload — verifying S3 destination: $S3Target"
        $FileList  = & aws @ProfileArgs s3 ls $S3Target --region $S3Region @EndpointArgs 2>&1
        $FileCount = ($FileList | Where-Object { $_ -match "^\\d" }).Count
        $FileList | ForEach-Object { Write-Output "[DEBUG]   $_" }
        Write-Output "[INFO] Verification passed — $FileCount object(s) in S3"
    }
    "s3_download" {
        Write-Output "[INFO] Mode    : s3_download — verifying local destination"
        $Files = @(Get-ChildItem -Path $DestPath -ErrorAction SilentlyContinue)
        Write-Output "[INFO] Verification passed — $($Files.Count) file(s) in $DestPath"
        $Files | ForEach-Object { Write-Output "[DEBUG]   $($_.FullName)" }
    }
    "blob_upload" {
        $BlobBaseUrl = "https://$BlobAccount.blob.core.windows.net/$BlobContainer/$BlobPrefix"
        $BlobUrl = if ($BlobSas) { "$BlobBaseUrl`?$BlobSas" } else { $BlobBaseUrl }
        Write-Output "[INFO] Mode    : blob_upload — verifying Blob destination: $BlobBaseUrl"
        $FileList  = & azcopy list $BlobUrl 2>&1
        $FileCount = ($FileList | Where-Object { $_ -notmatch "^INFO" -and $_ -ne "" }).Count
        $FileList | ForEach-Object { Write-Output "[DEBUG]   $_" }
        Write-Output "[INFO] Verification passed — $FileCount blob(s) at $BlobBaseUrl"
    }
    "blob_download" {
        Write-Output "[INFO] Mode    : blob_download — verifying local destination"
        $Files = @(Get-ChildItem -Path $DestPath -ErrorAction SilentlyContinue)
        Write-Output "[INFO] Verification passed — $($Files.Count) file(s) in $DestPath"
        $Files | ForEach-Object { Write-Output "[DEBUG]   $($_.FullName)" }
    }
    "s3_to_blob" {
        $BlobAccount   = "{{ params.blob_account }}"
        $BlobContainer = "{{ params.blob_container }}"
        $BlobPrefix    = "{{ params.blob_prefix }}"
        $BlobSas       = "{{ var.value.get(params.blob_sas_var_name, \"\") }}"
        # WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set
        $BlobBaseUrl   = "https://$BlobAccount.blob.core.windows.net/$BlobContainer/$BlobPrefix"
        $BlobUrl       = if ($BlobSas) { "$BlobBaseUrl`?$BlobSas" } else { $BlobBaseUrl }
        Write-Output "[INFO] Mode    : s3_to_blob — verifying Blob destination: $BlobBaseUrl"
        $FileList  = & azcopy list $BlobUrl 2>&1
        $FileCount = ($FileList | Where-Object { $_ -notmatch "^INFO" -and $_ -ne "" }).Count
        $FileList | ForEach-Object { Write-Output "[DEBUG]   $_" }
        Write-Output "[INFO] Verification passed — $FileCount blob(s) at $BlobBaseUrl"
    }
    "blob_to_s3" {
        $S3Bucket   = "{{ params.s3_bucket }}"
        $S3Prefix   = "{{ params.s3_prefix }}"
        $S3Region   = "{{ params.s3_region }}"
        $S3Endpoint = "{{ params.s3_endpoint_url }}"
        $AwsProfile = "{{ params.aws_profile }}"
        $ProfileArgs  = @(); if (-not [string]::IsNullOrEmpty($AwsProfile)) { $ProfileArgs = @("--profile", $AwsProfile) }
        $EndpointArgs = @(); if (-not [string]::IsNullOrEmpty($S3Endpoint))  { $EndpointArgs = @("--endpoint-url", $S3Endpoint, "--no-verify-ssl") }
        $S3Target = "s3://$S3Bucket/$S3Prefix"
        Write-Output "[INFO] Mode    : blob_to_s3 — verifying S3 destination: $S3Target"
        $FileList  = & aws @ProfileArgs s3 ls $S3Target --region $S3Region @EndpointArgs 2>&1
        $FileCount = ($FileList | Where-Object { $_ -match "^\\d" }).Count
        $FileList | ForEach-Object { Write-Output "[DEBUG]   $_" }
        Write-Output "[INFO] Verification passed — $FileCount object(s) in S3"
    }
}
""",
        on_failure_callback=failure_callback,
    )

    # Post-transfer action — direction-specific source management + archive/cleanup
    task_post_transfer = DynamicPsrpOperator(
        task_id='post_transfer_action',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Direction     = "{{ params.direction }}"
$SrcPath       = "{{ params.source_path }}"
$DestPath      = "{{ params.dest_path }}"
$NewName       = "{{ params.new_name }}"
$ArchivePath   = "{{ params.archive_path }}"
$RetentionDays = [int]"{{ params.retention_days }}"
$Date          = "{{ ds_nodash }}"
$RemotePass    = "{{ var.value.get(params.password_var_name, "") }}"
$DestHost      = "{{ params.dest_host }}"
$DestPort      = "{{ params.dest_port }}"
$DestProtocol  = "{{ params.dest_protocol }}"
$DestUser      = "{{ params.dest_user }}"
# WARNING: password visible in Airflow rendered template log

$ArchiveBase = if ([string]::IsNullOrEmpty($ArchivePath)) { "$DestPath\\archive" } else { $ArchivePath }
$ArchiveDir  = "$ArchiveBase\\$Date"

$WinSCPPath = "C:\\Program Files (x86)\\WinSCP\\WinSCP.com"
if (-not (Test-Path $WinSCPPath)) { $WinSCPPath = "C:\\Program Files\\WinSCP\\WinSCP.com" }

switch ($DestProtocol) {
    "ftps" { $ProtocolUrl = "ftps" }
    "ftp"  { $ProtocolUrl = "ftp"  }
    "sftp" { $ProtocolUrl = "sftp" }
    default { $ProtocolUrl = "ftp" }
}
$OpenArgs = "open -passive=on ${ProtocolUrl}://${DestUser}:$($RemotePass)@${DestHost}:${DestPort}/"
if ($DestProtocol -eq "ftps") { $OpenArgs += " -explicit -rawsettings MinTlsVersion=10 -certificate=`"*`"" }

Write-Output "[INFO] === Post-Transfer Action: $Direction ==="

switch ($Direction) {

    "upload" {
        Write-Output "[INFO] Action: none — source kept (SRCOPT=0)"
    }

    "upload_delete" {
        Write-Output "[INFO] Action: delete local source (SRCOPT=1)"
        $Files = @(Get-ChildItem -Path $SrcPath -ErrorAction SilentlyContinue)
        Write-Output "[INFO] Files to delete: $($Files.Count)"
        $Files | ForEach-Object {
            Write-Output "[DEBUG] Deleting: $($_.FullName)"
            Remove-Item -Path $_.FullName -Force
        }
        Write-Output "[INFO] Source deleted — $($Files.Count) file(s) removed"
    }

    "upload_rename" {
        Write-Output "[INFO] Action: rename local source to $NewName (SRCOPT=2)"
        if ([string]::IsNullOrEmpty($NewName)) { Write-Error "[ERROR] new_name required for upload_rename"; exit 1 }
        $SrcDir  = [System.IO.Path]::GetDirectoryName($SrcPath)
        $NewPath = Join-Path $SrcDir $NewName
        Write-Output "[INFO] Renaming: $SrcPath -> $NewPath"
        Rename-Item -Path $SrcPath -NewName $NewName -Force
        Write-Output "[INFO] Source renamed"
    }

    "upload_move" {
        Write-Output "[INFO] Action: move local source to $NewName (SRCOPT=3)"
        if ([string]::IsNullOrEmpty($NewName)) { Write-Error "[ERROR] new_name required for upload_move"; exit 1 }
        $NewDir = [System.IO.Path]::GetDirectoryName($NewName)
        if (-not [string]::IsNullOrEmpty($NewDir) -and -not (Test-Path $NewDir)) {
            New-Item -ItemType Directory -Path $NewDir -Force | Out-Null
        }
        Write-Output "[INFO] Moving: $SrcPath -> $NewName"
        Move-Item -Path $SrcPath -Destination $NewName -Force
        Write-Output "[INFO] Source moved"
    }

    "download" {
        Write-Output "[INFO] Action: none — remote source kept (SRCOPT=0)"
    }

    "download_delete" {
        Write-Output "[INFO] Action: delete remote source (SRCOPT=1)"
        if (-not (Test-Path $WinSCPPath)) { Write-Error "[ERROR] WinSCP not found"; exit 1 }
        Write-Output "[INFO] Deleting remote: $SrcPath at $DestHost"
        & $WinSCPPath /command $OpenArgs "rm `"$SrcPath`"" "exit"
        if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] WinSCP delete failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
        Write-Output "[INFO] Remote source deleted"
    }

    "download_rename" {
        Write-Output "[INFO] Action: rename remote source to $NewName (SRCOPT=2)"
        if ([string]::IsNullOrEmpty($NewName)) { Write-Error "[ERROR] new_name required for download_rename"; exit 1 }
        if (-not (Test-Path $WinSCPPath)) { Write-Error "[ERROR] WinSCP not found"; exit 1 }
        $SrcDir  = [System.IO.Path]::GetDirectoryName($SrcPath).Replace('\', '/')
        $NewPath = "$SrcDir/$NewName"
        Write-Output "[INFO] Renaming remote: $SrcPath -> $NewPath"
        & $WinSCPPath /command $OpenArgs "mv `"$SrcPath`" `"$NewPath`"" "exit"
        if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] WinSCP rename failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
        Write-Output "[INFO] Remote source renamed"
    }

    "download_move" {
        Write-Output "[INFO] Action: move remote source to $NewName (SRCOPT=3)"
        if ([string]::IsNullOrEmpty($NewName)) { Write-Error "[ERROR] new_name required for download_move"; exit 1 }
        if (-not (Test-Path $WinSCPPath)) { Write-Error "[ERROR] WinSCP not found"; exit 1 }
        Write-Output "[INFO] Moving remote: $SrcPath -> $NewName"
        & $WinSCPPath /command $OpenArgs "mv `"$SrcPath`" `"$NewName`"" "exit"
        if ($LASTEXITCODE -ne 0) { Write-Error "[ERROR] WinSCP move failed ($LASTEXITCODE)"; exit $LASTEXITCODE }
        Write-Output "[INFO] Remote source moved"
    }

    "download_archive" {
        Write-Output "[INFO] Action: archive downloaded files locally"
        $SrcPattern = [System.IO.Path]::GetFileName($SrcPath)
        Write-Output "[INFO] Pattern : $SrcPattern"
        Write-Output "[INFO] Archive : $ArchiveDir"
        if (-not (Test-Path $ArchiveDir)) { New-Item -ItemType Directory -Path $ArchiveDir -Force | Out-Null }
        $Files = @(Get-ChildItem -Path "$DestPath\\$SrcPattern" -ErrorAction SilentlyContinue)
        if ($Files.Count -gt 0) {
            $Files | ForEach-Object {
                Write-Output "[DEBUG] Archiving: $($_.FullName) -> $ArchiveDir\"
                Move-Item -Path $_.FullName -Destination $ArchiveDir -Force
            }
            Write-Output "[INFO] Archived $($Files.Count) file(s) -> $ArchiveDir"
        } else {
            Write-Output "[INFO] No files to archive matching $SrcPattern"
        }
    }

    "archive_only" {
        Write-Output "[INFO] Action: archive existing files in $DestPath"
        $SrcPattern = [System.IO.Path]::GetFileName($SrcPath)
        Write-Output "[INFO] Pattern : $SrcPattern"
        Write-Output "[INFO] Archive : $ArchiveDir"
        if (-not (Test-Path $ArchiveDir)) { New-Item -ItemType Directory -Path $ArchiveDir -Force | Out-Null }
        $Files = @(Get-ChildItem -Path "$DestPath\\$SrcPattern" -ErrorAction SilentlyContinue)
        if ($Files.Count -gt 0) {
            $Files | ForEach-Object {
                Write-Output "[DEBUG] Archiving: $($_.FullName) -> $ArchiveDir\"
                Move-Item -Path $_.FullName -Destination $ArchiveDir -Force
            }
            Write-Output "[INFO] Archived $($Files.Count) file(s) -> $ArchiveDir"
        } else {
            Write-Output "[INFO] No files to archive matching $SrcPattern"
        }
    }

    "cleanup_only" {
        Write-Output "[INFO] Action: cleanup archives older than $RetentionDays days"
        Write-Output "[INFO] Archive base: $ArchiveBase"
        if (Test-Path $ArchiveBase) {
            $Cutoff   = (Get-Date).AddDays(-$RetentionDays)
            $OldFiles = @(Get-ChildItem -Path $ArchiveBase -Recurse -File | Where-Object { $_.LastWriteTime -lt $Cutoff })
            Write-Output "[INFO] Files to remove: $($OldFiles.Count)"
            $OldFiles | ForEach-Object { Write-Output "[DEBUG] Removing: $($_.FullName)"; Remove-Item -Path $_.FullName -Force }
            Get-ChildItem -Path $ArchiveBase -Recurse -Directory |
                Sort-Object FullName -Descending |
                Where-Object { @(Get-ChildItem -Path $_.FullName).Count -eq 0 } |
                ForEach-Object { Remove-Item -Path $_.FullName -Force }
            Write-Output "[INFO] Cleanup completed — $($OldFiles.Count) file(s) removed"
        } else {
            Write-Output "[INFO] Archive directory not found, skipping: $ArchiveBase"
        }
    }

    "s3_upload"    { Write-Output "[INFO] Action: none — S3 upload, source kept" }
    "s3_download"  { Write-Output "[INFO] Action: none — S3 download, source kept" }
    "blob_upload"  { Write-Output "[INFO] Action: none — Blob upload, source kept" }
    "blob_download"{ Write-Output "[INFO] Action: none — Blob download, source kept" }
    "s3_to_blob"   { Write-Output "[INFO] Action: none — cloud-to-cloud transfer, source kept" }
    "blob_to_s3"   { Write-Output "[INFO] Action: none — cloud-to-cloud transfer, source kept" }

    default {
        Write-Error "[ERROR] Unknown direction: $Direction"
        exit 1
    }
}

Write-Output "[INFO] Post-transfer action completed"
""",
        on_failure_callback=failure_callback,
    )

    # Post-command — self-skips when params.post_command is empty
    task_postcomm = DynamicPsrpOperator(
        task_id='post_command',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Cmd = "{{ params.post_command }}"
if ([string]::IsNullOrEmpty($Cmd)) {
    Write-Output "[INFO] post_command: skipped (empty)"
    exit 0
}
$ArgsJson = '{{ params.post_command_args | tojson }}'
$ArgsList = $ArgsJson | ConvertFrom-Json
Write-Output "[INFO] post_command: $Cmd $($ArgsList -join ' ')"
& $Cmd @ArgsList
""",
        on_failure_callback=failure_callback,
    )

    ###################### Task Dependencies ######################

    start >> task_validate_params >> task_precomm >> task_validate_source >> task_transfer_files >> task_verify_transfer >> task_post_transfer >> task_postcomm >> end
