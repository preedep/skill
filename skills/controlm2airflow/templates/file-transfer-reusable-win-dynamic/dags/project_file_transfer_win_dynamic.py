from airflow import DAG

from datetime import datetime, timedelta
import logging
import pendulum

from airflow.providers.microsoft.psrp.operators.psrp import PsrpOperator
from airflow.operators.empty import EmptyOperator
try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.dagrun_operator import TriggerDagRunOperator

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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
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
        # Connection — passed by caller, varies per job
        "psrp_conn_id":      "",    # Airflow PSRP connection ID to source Windows server (required)
        # Transfer — passed by caller, varies per job
        "source_path":       "",    # source file path on Windows server, supports wildcards (required)
        "dest_host":         "",    # destination server hostname (required)
        "dest_port":         "",    # destination port — required: 21 (ftp/ftps) | 22 (sftp)
        "dest_protocol":     "",    # required: ftp | ftps | sftp
        "dest_user":         "",    # destination FTP/SFTP username (required)
        "dest_path":         "",    # destination path on remote server (required)
        "password_var_name": "",    # Airflow Variable key holding destination password (required)
        # Commands — optional
        "pre_command":       "",    # command to run before transfer (empty = skip)
        "post_command":      "",    # command to run after transfer (empty = skip)
        # Post-transfer action
        "delete_source":     False, # True = delete source after transfer (%%SRCOPT=delete)
    },
    doc_md="""
    # File Transfer Reusable DAG — Windows Dynamic (WinSCP via PSRP)

    Reusable per-project DAG for Windows file transfers. Acts like a function —
    **all parameters are passed at runtime by the caller via TriggerDagRunOperator conf={}**.

    No hardcoded infrastructure. One DAG handles all Windows FILE_TRANS jobs in the project.
    Project team owns this DAG — changes only affect this project.

    **schedule=None** — always triggered by caller, never runs on its own schedule.

    ## How to call this DAG

    ```python
    TriggerDagRunOperator(
        task_id="transfer_report",
        trigger_dag_id="scb-PROJECT-file-transfer-win-ENV",
        wait_for_completion=True,
        poke_interval=30,
        conf={
            "psrp_conn_id":      "psrp_winsrv01",
            "source_path":       r"D:\\data\\report\\*.csv",
            "dest_host":         "dest-server.example.com",
            "dest_port":         "21",
            "dest_protocol":     "ftp",
            "dest_user":         "domain\\\\svc_transfer",
            "dest_path":         "/incoming/report/",
            "password_var_name": "projecta_transfer_password",
            "pre_command":       "",
            "post_command":      "",
            "delete_source":     False,
        },
    )
    ```

    ## DAG Workflow
    ```
    start → pre_command → validate_source → transfer_winscp → verify
          → delete_source | (archive → cleanup) → post_command → end
    ```
    - `pre_command` / `post_command` self-skip when param is empty string
    - `delete_source=True`  → delete source after transfer (%%SRCOPT=delete), skip archive
    - `delete_source=False` → archive source to `archive\\YYYYMMDD\\` subfolder

    ## Notes
    - WinSCP must be installed on the source Windows server at
      `C:\\Program Files (x86)\\WinSCP\\WinSCP.com`
    - `password_var_name` must be an Airflow Variable key — password is never hardcoded
    - Backslash in `dest_user` (e.g. `domain\\user`) is URL-encoded to `domain%5Cuser` for WinSCP
    """
)

###################### Tasks ######################

with dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    # Pre-command — self-skips when params.pre_command is empty string
    task_precomm = PsrpOperator(
        task_id='pre_command',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Cmd = "{{ params.pre_command }}"
if ($Cmd -eq "") {
    Write-Host "[INFO] pre_command: skipped (empty)"
    exit 0
}
Write-Host "[INFO] pre_command: $Cmd"
Invoke-Expression $Cmd
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] pre_command failed: exit $LASTEXITCODE"
    exit $LASTEXITCODE
}
""",
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    # Validate source files exist before transfer
    task_validate_source = PsrpOperator(
        task_id='validate_source_files',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$SourcePath = "{{ params.source_path }}"

Write-Host "[INFO] === Source File Validation ==="
Write-Host "[INFO] Source: $SourcePath"

if (Test-Path $SourcePath) {
    $Files = Get-ChildItem -Path $SourcePath -File
    $FileCount = $Files.Count
    Write-Host "[INFO] Files found: $FileCount"
    if ($FileCount -eq 0) {
        Write-Host "[ERROR] No files found to transfer"
        exit 1
    }
    $Files | ForEach-Object {
        Write-Host "[INFO]   - $($_.Name) ($([math]::Round($_.Length/1MB, 2)) MB)"
    }
    Write-Host "[INFO] Validation passed"
    exit 0
} else {
    Write-Host "[ERROR] Source path not found: $SourcePath"
    exit 1
}
""",
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    # WinSCP file transfer — all connection and path params from params
    task_transfer_files = PsrpOperator(
        task_id='transfer_files_winscp',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$REMOTE_PASS = "{{ var.value[params.password_var_name] }}"
$WinSCPPath  = "C:\\Program Files (x86)\\WinSCP\\WinSCP.com"
$SrcPath     = "{{ params.source_path }}"
$DestHost    = "{{ params.dest_host }}"
$DestPort    = "{{ params.dest_port }}"
$DestProtocol = "{{ params.dest_protocol }}"
$DestUser    = "{{ params.dest_user }}".Replace("\\", "%5C")
$DestPath    = "{{ params.dest_path }}"

Write-Host "[INFO] === WinSCP File Transfer ==="
Write-Host "[INFO] Source     : $SrcPath"
Write-Host "[INFO] Destination: {{ params.dest_user }}@${DestHost}:${DestPath}"
Write-Host "[INFO] Protocol   : ${DestProtocol} port ${DestPort}"
# WARNING: password visible in Airflow rendered template log

& $WinSCPPath /command `
    "open -passive=on ${DestProtocol}://${DestUser}:$($REMOTE_PASS)@${DestHost}:${DestPort}/ -explicit -rawsettings MinTlsVersion=10 -certificate=`"*`"" `
    "put `"$SrcPath`" `"$DestPath`"" `
    "exit"

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] WinSCP transfer failed with exit code $LASTEXITCODE"
    exit 1
}

Write-Host "[INFO] Transfer completed successfully"
""",
        cmd_timeout=3600,
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    # Verify file exists on destination after transfer
    task_verify_transfer = PsrpOperator(
        task_id='verify_transfer',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$REMOTE_PASS = "{{ var.value[params.password_var_name] }}"
$WinSCPPath  = "C:\\Program Files (x86)\\WinSCP\\WinSCP.com"
$DestHost    = "{{ params.dest_host }}"
$DestPort    = "{{ params.dest_port }}"
$DestProtocol = "{{ params.dest_protocol }}"
$DestUser    = "{{ params.dest_user }}".Replace("\\", "%5C")
$DestPath    = "{{ params.dest_path }}"

Write-Host "[INFO] === Verify Transfer ==="
Write-Host "[INFO] Checking: ${DestHost}:${DestPath}"

& $WinSCPPath /command `
    "open -passive=on ${DestProtocol}://${DestUser}:$($REMOTE_PASS)@${DestHost}:${DestPort}/ -explicit -rawsettings MinTlsVersion=10 -certificate=`"*`"" `
    "ls `"$DestPath`"" `
    "exit"

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Verification failed — path not found on destination"
    exit 1
}

Write-Host "[INFO] Verification passed"
""",
        cmd_timeout=300,
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    # Delete source after transfer — self-skips when delete_source=False
    task_delete_source = PsrpOperator(
        task_id='delete_source_files',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$DeleteSource = "{{ params.delete_source }}"
$SrcPath      = "{{ params.source_path }}"

if ($DeleteSource -ne "True") {
    Write-Host "[INFO] delete_source: skipped (delete_source=False)"
    exit 0
}

Write-Host "[INFO] === Delete Source Files (SRCOPT=delete) ==="
Write-Host "[INFO] Path: $SrcPath"

if (Test-Path $SrcPath) {
    Remove-Item -Path $SrcPath -Force
    Write-Host "[INFO] Source deleted successfully"
} else {
    Write-Host "[INFO] Source path not found, skipping delete"
}
exit 0
""",
        cmd_timeout=300,
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    # Archive source files — self-skips when delete_source=True
    task_archive_source = PsrpOperator(
        task_id='archive_source_files',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$DeleteSource = "{{ params.delete_source }}"
$SrcPath      = "{{ params.source_path }}"
$Date         = "{{ ds_nodash }}"
$ArchiveBase  = Split-Path $SrcPath -Parent
$ArchiveDir   = Join-Path $ArchiveBase "archive\\$Date"

if ($DeleteSource -eq "True") {
    Write-Host "[INFO] archive_source: skipped (delete_source=True)"
    exit 0
}

Write-Host "[INFO] === Archive Source Files ==="
Write-Host "[INFO] Source : $SrcPath"
Write-Host "[INFO] Archive: $ArchiveDir"

if (-not (Test-Path $ArchiveDir)) {
    New-Item -ItemType Directory -Path $ArchiveDir -Force | Out-Null
}

$Files = Get-ChildItem -Path $SrcPath -File
foreach ($File in $Files) {
    Move-Item -Path $File.FullName -Destination (Join-Path $ArchiveDir $File.Name) -Force
    Write-Host "[INFO] Archived: $($File.Name)"
}

Write-Host "[INFO] Archive completed: $($Files.Count) files moved"
exit 0
""",
        cmd_timeout=600,
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    # Cleanup archives older than 30 days — self-skips when delete_source=True
    task_cleanup_archives = PsrpOperator(
        task_id='cleanup_old_archives',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$DeleteSource  = "{{ params.delete_source }}"
$SrcPath       = "{{ params.source_path }}"
$ArchiveBase   = Join-Path (Split-Path $SrcPath -Parent) "archive"
$RetentionDays = 30
$CutoffDate    = (Get-Date).AddDays(-$RetentionDays)

if ($DeleteSource -eq "True") {
    Write-Host "[INFO] cleanup_archives: skipped (delete_source=True)"
    exit 0
}

Write-Host "[INFO] === Cleanup Old Archives ==="
Write-Host "[INFO] Archive Base: $ArchiveBase"
Write-Host "[INFO] Retention   : $RetentionDays days"

if (Test-Path $ArchiveBase) {
    $OldFiles = Get-ChildItem -Path $ArchiveBase -Recurse -File |
                Where-Object { $_.LastWriteTime -lt $CutoffDate }
    $OldFiles | ForEach-Object {
        Write-Host "[INFO] Removing: $($_.FullName)"
        Remove-Item $_.FullName -Force
    }
    Write-Host "[INFO] Cleanup completed: $($OldFiles.Count) files removed"
} else {
    Write-Host "[INFO] Archive directory not found, skipping cleanup"
}
exit 0
""",
        cmd_timeout=600,
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    # Post-command — self-skips when params.post_command is empty string
    task_postcomm = PsrpOperator(
        task_id='post_command',
        psrp_conn_id="{{ params.psrp_conn_id }}",
        powershell="""
$ErrorActionPreference = 'Stop'
$Cmd = "{{ params.post_command }}"
if ($Cmd -eq "") {
    Write-Host "[INFO] post_command: skipped (empty)"
    exit 0
}
Write-Host "[INFO] post_command: $Cmd"
Invoke-Expression $Cmd
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] post_command failed: exit $LASTEXITCODE"
    exit $LASTEXITCODE
}
""",
        wsman_options={"ssl": False},
        on_failure_callback=failure_callback,
    )

    ###################### Task Dependencies ######################

    # Single linear flow — delete_source and archive tasks self-skip via params check
    start >> task_precomm >> task_validate_source >> task_transfer_files >> task_verify_transfer >> task_delete_source >> task_archive_source >> task_cleanup_archives >> task_postcomm >> end
