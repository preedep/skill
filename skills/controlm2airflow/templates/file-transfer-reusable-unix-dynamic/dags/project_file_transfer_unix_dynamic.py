from airflow import DAG

from datetime import datetime, timedelta
import logging
import pendulum

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.empty import EmptyOperator

###################### logging ######################
import smtplib
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
    <p><strong>Source Host:</strong> {params.get('ssh_conn_id', '-')}</p>
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
    <p><strong>Source Host:</strong> {params.get('ssh_conn_id', '-')}</p>
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
        "ssh_conn_id":       "",    # Airflow SSH connection ID to source Unix server (required)
        # Transfer — passed by caller, varies per job
        "source_path":       "",    # source file path on remote server, supports wildcards (required)
        "dest_host":         "",    # destination server hostname (required)
        "dest_port":         "",    # destination port — required: 21 (ftp/ftps) | 22 (sftp)
        "dest_protocol":     "",    # required: ftp | ftps | sftp
        "dest_user":         "",    # destination FTP/SFTP username (required)
        "dest_path":         "",    # local destination path to download files into (required)
        "password_var_name": "",    # Airflow Variable key holding destination password (required)
        # Commands — optional
        "pre_command":       "",    # command to run before transfer (empty = skip)
        "post_command":      "",    # command to run after transfer (empty = skip)
        # Post-transfer action
        "delete_source":     False, # True = delete source after transfer (%%SRCOPT=delete)
    },
    doc_md="""
    # File Transfer Reusable DAG — Unix Dynamic (lftp via SSH)

    Reusable per-project DAG for Unix file transfers. Acts like a function —
    **all parameters are passed at runtime by the caller via TriggerDagRunOperator conf={}**.

    No hardcoded infrastructure. One DAG handles all Unix FILE_TRANS jobs in the project.
    Project team owns this DAG — changes only affect this project.

    **schedule=None** — always triggered by caller, never runs on its own schedule.

    ## How to call this DAG

    ```python
    TriggerDagRunOperator(
        task_id="transfer_report",
        trigger_dag_id="scb-PROJECT-file-transfer-unix-ENV",
        wait_for_completion=True,
        poke_interval=30,
        conf={
            "ssh_conn_id":       "ssh_unixsrv01",
            "source_path":       "/data/source/report/*.csv",
            "dest_host":         "dest-server.example.com",
            "dest_port":         "21",
            "dest_protocol":     "ftp",
            "dest_user":         "ftpuser",
            "dest_path":         "/local/incoming/report/",
            "password_var_name": "projecta_transfer_password",
            "pre_command":       "",
            "post_command":      "",
            "delete_source":     False,
        },
    )
    ```

    ## DAG Workflow
    ```
    start → pre_command → validate_source → transfer_lftp → verify
          → delete_source | (archive → cleanup) → post_command → end
    ```
    - `pre_command` / `post_command` self-skip when param is empty string
    - `delete_source=True`  → delete source via lftp rm (%%SRCOPT=delete), skip archive
    - `delete_source=False` → archive downloaded files to `dest_path/archive/YYYYMMDD/`

    ## Notes
    - lftp must be installed on the source Unix server
    - `password_var_name` must be an Airflow Variable key — password is never hardcoded
    """
)

###################### Tasks ######################

with dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    # Pre-command — self-skips when params.pre_command is empty string
    task_precomm = SSHOperator(
        task_id='pre_command',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] pre_command failed at line $LINENO — exit $?"' ERR
CMD="{{ params.pre_command }}"
if [ -z "$CMD" ]; then
    echo "[INFO] pre_command: skipped (empty)"
    exit 0
fi
echo "[INFO] pre_command: $CMD"
eval "$CMD"
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    # Validate source files exist — ls on the source server (SSHOperator runs on source via ssh_conn_id)
    task_validate_source = SSHOperator(
        task_id='validate_source_files',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Validation failed at line $LINENO — exit $?"' ERR

SRC_PATH="{{ params.source_path }}"

echo "[INFO] === Source File Validation ==="
echo "[INFO] Source path: ${SRC_PATH}"

if ls ${SRC_PATH} 1>/dev/null 2>&1; then
    FILE_COUNT=$(ls ${SRC_PATH} 2>/dev/null | wc -l)
    ls -lh ${SRC_PATH}
    echo "[INFO] Validation passed — ${FILE_COUNT} file(s) found"
else
    echo "[ERROR] No source files found: ${SRC_PATH}"
    exit 1
fi
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    # lftp file transfer — all connection and path params from params
    task_transfer_files = SSHOperator(
        task_id='transfer_files_lftp',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Transfer failed at line $LINENO — exit $?"' ERR

REMOTE_PASS="{{ var.value[params.password_var_name] }}"
SRC_PATH="{{ params.source_path }}"
DEST_HOST="{{ params.dest_host }}"
DEST_PORT="{{ params.dest_port }}"
DEST_PROTOCOL="{{ params.dest_protocol }}"
DEST_USER="{{ params.dest_user }}"
LOCAL_PATH="{{ params.dest_path }}"
# WARNING: password visible in Airflow rendered template log

echo "[INFO] === lftp File Transfer ==="
echo "[INFO] Source     : ${DEST_USER}@${DEST_HOST}:${SRC_PATH}"
echo "[INFO] Destination: ${LOCAL_PATH}"
echo "[INFO] Protocol   : ${DEST_PROTOCOL} port ${DEST_PORT}"

mkdir -p "$LOCAL_PATH"

lftp -c \
    "open -u ${DEST_USER},${REMOTE_PASS} ${DEST_PROTOCOL}://${DEST_HOST}:${DEST_PORT}; \
     set ssl:verify-certificate false; \
     set ftp:ssl-protect-data true; \
     set ftp:ssl-protect-list true; \
     set xfer:clobber true; \
     lcd ${LOCAL_PATH}; \
     mget ${SRC_PATH}; \
     bye"

echo "[INFO] Transfer completed successfully"
BASH
""",
        cmd_timeout=3600,
        on_failure_callback=failure_callback,
    )

    # Verify files exist at local destination after transfer
    task_verify_transfer = SSHOperator(
        task_id='verify_transfer',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Verify failed at line $LINENO — exit $?"' ERR

LOCAL_PATH="{{ params.dest_path }}"
SRC_FILE=$(basename "{{ params.source_path }}")

echo "[INFO] === Verify Transfer ==="
echo "[INFO] Checking: ${LOCAL_PATH}/${SRC_FILE}"

if ls "$LOCAL_PATH"/$SRC_FILE 1>/dev/null 2>&1; then
    ls -lh "$LOCAL_PATH"/$SRC_FILE
    echo "[INFO] Verification passed"
else
    echo "[ERROR] File not found at ${LOCAL_PATH}/${SRC_FILE}"
    exit 1
fi
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    # Delete source after transfer — self-skips when delete_source=False
    task_delete_source = SSHOperator(
        task_id='delete_source_files',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Delete failed at line $LINENO — exit $?"' ERR

DELETE_SOURCE="{{ params.delete_source }}"
SRC_PATH="{{ params.source_path }}"
REMOTE_PASS="{{ var.value[params.password_var_name] }}"
DEST_HOST="{{ params.dest_host }}"
DEST_PORT="{{ params.dest_port }}"
DEST_PROTOCOL="{{ params.dest_protocol }}"
DEST_USER="{{ params.dest_user }}"

if [ "$DELETE_SOURCE" != "True" ]; then
    echo "[INFO] delete_source: skipped (delete_source=False)"
    exit 0
fi

echo "[INFO] === Delete Source Files (SRCOPT=delete) ==="
echo "[INFO] Remote: ${DEST_HOST}:${SRC_PATH}"

lftp -c \
    "open -u ${DEST_USER},${REMOTE_PASS} ${DEST_PROTOCOL}://${DEST_HOST}:${DEST_PORT}; \
     set ssl:verify-certificate false; \
     set ftp:ssl-protect-data true; \
     set ftp:ssl-protect-list true; \
     rm ${SRC_PATH}; \
     bye"

echo "[INFO] Source deleted successfully"
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    # Archive downloaded files — self-skips when delete_source=True
    task_archive_source = SSHOperator(
        task_id='archive_source_files',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Archive failed at line $LINENO — exit $?"' ERR

DELETE_SOURCE="{{ params.delete_source }}"
LOCAL_PATH="{{ params.dest_path }}"
SRC_FILE=$(basename "{{ params.source_path }}")
DATE="{{ ds_nodash }}"
ARCHIVE_DIR="${LOCAL_PATH}/archive/${DATE}"

if [ "$DELETE_SOURCE" = "True" ]; then
    echo "[INFO] archive_source: skipped (delete_source=True)"
    exit 0
fi

echo "[INFO] === Archive Downloaded Files ==="
echo "[INFO] File   : $SRC_FILE"
echo "[INFO] Archive: $ARCHIVE_DIR"

mkdir -p "$ARCHIVE_DIR"

if ls "$LOCAL_PATH"/$SRC_FILE 1>/dev/null 2>&1; then
    mv "$LOCAL_PATH"/$SRC_FILE "$ARCHIVE_DIR/"
    echo "[INFO] Archived: $SRC_FILE"
else
    echo "[INFO] File not found, skipping archive"
fi
BASH
""",
        cmd_timeout=600,
        on_failure_callback=failure_callback,
    )

    # Cleanup archives older than 30 days — self-skips when delete_source=True
    task_cleanup_archives = SSHOperator(
        task_id='cleanup_old_archives',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Cleanup failed at line $LINENO — exit $?"' ERR

DELETE_SOURCE="{{ params.delete_source }}"
LOCAL_PATH="{{ params.dest_path }}"
ARCHIVE_BASE="${LOCAL_PATH}/archive"
RETENTION_DAYS=30

if [ "$DELETE_SOURCE" = "True" ]; then
    echo "[INFO] cleanup_archives: skipped (delete_source=True)"
    exit 0
fi

echo "[INFO] === Cleanup Old Archives ==="
echo "[INFO] Archive Base: $ARCHIVE_BASE"
echo "[INFO] Retention   : $RETENTION_DAYS days"

if [ -d "$ARCHIVE_BASE" ]; then
    REMOVED_COUNT=$(find "$ARCHIVE_BASE" -type f -mtime +$RETENTION_DAYS | wc -l)
    find "$ARCHIVE_BASE" -type f -mtime +$RETENTION_DAYS -exec ls -lh {} \;
    find "$ARCHIVE_BASE" -type f -mtime +$RETENTION_DAYS -delete
    echo "[INFO] Cleanup completed: $REMOVED_COUNT files removed"
else
    echo "[INFO] Archive directory not found, skipping cleanup"
fi
BASH
""",
        cmd_timeout=600,
        on_failure_callback=failure_callback,
    )

    # Post-command — self-skips when params.post_command is empty string
    task_postcomm = SSHOperator(
        task_id='post_command',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] post_command failed at line $LINENO — exit $?"' ERR
CMD="{{ params.post_command }}"
if [ -z "$CMD" ]; then
    echo "[INFO] post_command: skipped (empty)"
    exit 0
fi
echo "[INFO] post_command: $CMD"
eval "$CMD"
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    ###################### Task Dependencies ######################

    # Single linear flow — delete_source and archive tasks self-skip via params check
    start >> task_precomm >> task_validate_source >> task_transfer_files >> task_verify_transfer >> task_delete_source >> task_archive_source >> task_cleanup_archives >> task_postcomm >> end
