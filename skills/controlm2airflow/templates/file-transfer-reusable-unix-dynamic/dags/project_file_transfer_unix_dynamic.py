from airflow import DAG

from datetime import datetime
import logging
import pendulum

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


class DynamicSSHOperator(SSHOperator):
    """SSHOperator with ssh_conn_id added to template_fields so {{ params.ssh_conn_id }} resolves at runtime."""
    template_fields = SSHOperator.template_fields + ('ssh_conn_id',)

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
        "ssh_conn_id":          "",    # Airflow SSH connection ID to source Unix server (required)
        # ── Transfer ───────────────────────────────────────────────────────────
        "source_path":          "",    # source file path / glob on source server (required)
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
        # upload          : lftp put source_path → dest_path, keep source        (Control-M SRCOPT=0)
        # upload_delete   : lftp put source_path → dest_path, delete source      (Control-M SRCOPT=1)
        # upload_rename   : lftp put source_path → dest_path, rename source      (Control-M SRCOPT=2)
        # upload_move     : lftp put source_path → dest_path, move source        (Control-M SRCOPT=3)
        # download        : lftp mget source_path → dest_path, keep source       (Control-M SRCOPT=0)
        # download_delete : lftp mget source_path → dest_path, delete remote src (Control-M SRCOPT=1)
        # download_rename : lftp mget source_path → dest_path, rename remote src (Control-M SRCOPT=2)
        # download_move   : lftp mget source_path → dest_path, move remote src   (Control-M SRCOPT=3)
        # download_archive: lftp mget source_path → dest_path, archive locally   (Airflow extra)
        # archive_only    : no transfer — archive existing files in dest_path     (data retention)
        # cleanup_only    : no transfer — delete archives older than N days       (data retention)
        # s3_upload       : aws s3 cp source_path → s3://bucket/prefix           (S3)
        # s3_download     : aws s3 cp s3://bucket/prefix → dest_path             (S3)
        # blob_upload     : azcopy copy source_path → blob container/prefix      (Azure Blob)
        # blob_download   : azcopy copy blob container/prefix → dest_path        (Azure Blob)
        # s3_to_blob      : azcopy copy s3://bucket/prefix → blob container      (S3 → Blob, no staging)
        # blob_to_s3      : azcopy copy blob container → s3://bucket/prefix      (Blob → S3, no staging)
        "direction":            "upload",
        # ── Post-transfer extras ───────────────────────────────────────────────
        "new_name":             "",    # new filename for *_rename / *_move directions
        "archive_path":         "",    # archive base path for download_archive / archive_only
        "retention_days":       "30",  # days to keep archives for cleanup_only
        # ── Commands — optional ────────────────────────────────────────────────
        "pre_command":          "",    # shell command to run before transfer (empty = skip)
        "pre_command_args":     [],    # list of arguments for pre_command (e.g. ["666", "/path/*.*"])
        "post_command":         "",    # shell command to run after transfer (empty = skip)
        "post_command_args":    [],    # list of arguments for post_command
    },
    doc_md="""
# File Transfer Reusable DAG — Unix Dynamic (lftp/azcopy via SSH)

## Purpose

Reusable per-project DAG for Unix file transfers via FTP/FTPS/SFTP/S3/Azure Blob.
Acts like a function — **all parameters are passed at runtime by the caller
via `TriggerDagRunOperator conf={}`**.

One DAG handles all Unix `FILE_TRANS` jobs in the project.
No infrastructure is hardcoded. Project team owns this DAG.

`schedule=None` — always triggered by caller, never runs on its own schedule.

---

## Parameters

| Parameter | Required | Description |
|---|---|---|
| `ssh_conn_id` | ✅ | Airflow SSH connection ID to the **source** Unix server |
| `source_path` | ✅ | File path or glob on source server (e.g. `/data/*.csv`) |
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
| `archive_path` | ➖ | Archive base path (default: `dest_path/archive`) |
| `retention_days` | ➖ | Days to keep archives for cleanup_only (default: `30`) |
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
| `download` | ftp/ftps/sftp | mget remote → local, keep remote source (SRCOPT=0) |
| `download_delete` | ftp/ftps/sftp | mget remote → local, delete remote source after (SRCOPT=1) |
| `download_rename` | ftp/ftps/sftp | mget remote → local, rename remote source to `new_name` (SRCOPT=2) |
| `download_move` | ftp/ftps/sftp | mget remote → local, move remote source to `new_name` path (SRCOPT=3) |
| `download_archive` | ftp/ftps/sftp | mget remote → local, archive locally by date |
| `archive_only` | (any) | no transfer — archive existing files in dest_path by date |
| `cleanup_only` | (any) | no transfer — delete archives older than `retention_days` |
| `s3_upload` | s3 | aws s3 cp source_path → s3://bucket/prefix |
| `s3_download` | s3 | aws s3 cp s3://bucket/prefix → dest_path |
| `blob_upload` | blob | azcopy copy source_path → blob account/container/prefix |
| `blob_download` | blob | azcopy copy blob account/container/prefix → dest_path |
| `s3_to_blob` | s3+blob | azcopy copy s3://bucket/prefix → blob account/container (no staging) |
| `blob_to_s3` | s3+blob | azcopy copy blob account/container → s3://bucket/prefix (no staging) |

---

## DAG Workflow

```
ftp/ftps/sftp:   start → pre_command → validate_source → transfer_files → verify → post_transfer_action → post_command → end
s3/blob:         start → pre_command → validate_source → transfer_files → verify → post_transfer_action → post_command → end
archive/cleanup: start → pre_command → [validate: skip] → [transfer: skip] → [verify: skip] → post_transfer_action → post_command → end
```

---

## Notes

- lftp must be installed on the source Unix server (for ftp/ftps/sftp)
- AWS CLI must be installed on the source Unix server (for s3)
- azcopy must be installed on the source Unix server (for blob)
- `password_var_name` must be an Airflow Variable key — password is never hardcoded
- FTPS uses explicit TLS (AUTH TLS) — use `dest_protocol="ftps"`, port `991`
- MVS dataset names in `dest_path` (no leading `/`) are single-quoted automatically
- Blob auth: if `blob_sas_var_name` is set, SAS token is used; otherwise managed identity (`azcopy login --identity`)
- `blob_identity_client_id` selects user-assigned MI; leave empty for system-assigned MI
- `s3_endpoint_url` is required for private VPC endpoint
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

    # ssh_conn_id always required
    need('ssh_conn_id')

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
    task_precomm = DynamicSSHOperator(
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
# Build full command string — run via bash -c so globs (e.g. /path/*.*) are expanded
ARGS_JSON='{{ params.pre_command_args | tojson }}'
ARGS_STR=$(echo "$ARGS_JSON" | python3 -c "import json,sys; print(' '.join(json.load(sys.stdin)))" 2>/dev/null || true)
FULL_CMD="$CMD${ARGS_STR:+ $ARGS_STR}"
echo "[INFO] pre_command: $FULL_CMD"
# For chmod: check that at least one file matches the glob before running
if [ "$CMD" = "chmod" ] && [ -n "$ARGS_STR" ]; then
    GLOB_PATH=$(echo "$ARGS_STR" | awk '{print $NF}')
    MATCH_COUNT=$(ls -1 $GLOB_PATH 2>/dev/null | wc -l | tr -d ' ') || MATCH_COUNT=0
    if [ "$MATCH_COUNT" -eq 0 ]; then
        echo "[WARN] pre_command: chmod skipped — no files matched: $GLOB_PATH"
        exit 0
    fi
    echo "[INFO] pre_command: chmod target — $MATCH_COUNT file(s) matched"
fi
bash -c "$FULL_CMD"
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    # Validate source files exist — self-skips for archive_only / cleanup_only / blob_download / s3_download
    task_validate_source = DynamicSSHOperator(
        task_id='validate_source_files',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Validation failed at line $LINENO — exit $?"' ERR

DIRECTION="{{ params.direction }}"
SRC_PATH="{{ params.source_path }}"

case "${DIRECTION}" in
    archive_only|cleanup_only|s3_download|blob_download|s3_to_blob|blob_to_s3)
        echo "[INFO] validate_source: skipped (direction=${DIRECTION})"
        exit 0
        ;;
esac

echo "[INFO] === Source File Validation ==="
echo "[INFO] Source path : ${SRC_PATH}"
echo "[DEBUG] Agent host : $(hostname)"
echo "[DEBUG] Agent user : $(whoami)"

if ls ${SRC_PATH} 1>/dev/null 2>&1; then
    FILE_COUNT=$(ls ${SRC_PATH} 2>/dev/null | wc -l | tr -d ' ')
    TOTAL_SIZE=$(du -sh ${SRC_PATH} 2>/dev/null | awk '{print $1}' || echo 'n/a')
    ls -lh ${SRC_PATH} | awk '{print "[DEBUG]   " $0}'
    echo "[INFO] Validation passed — ${FILE_COUNT} file(s), total size: ${TOTAL_SIZE}"
else
    echo "[ERROR] No source files found: ${SRC_PATH}"
    exit 1
fi
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    # File transfer — handles ftp/ftps/sftp (lftp), s3 (AWS CLI), blob (azcopy)
    task_transfer_files = DynamicSSHOperator(
        task_id='transfer_files',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Transfer failed at line $LINENO — exit $?"' ERR

DIRECTION="{{ params.direction }}"
SRC_PATH="{{ params.source_path }}"
DEST_HOST="{{ params.dest_host }}"
DEST_PORT="{{ params.dest_port }}"
DEST_PROTOCOL="{{ params.dest_protocol }}"
DEST_USER="{{ params.dest_user }}"
DEST_PATH="{{ params.dest_path }}"
REMOTE_PASS="{{ var.value.get(params.password_var_name, "") }}"
# WARNING: password visible in Airflow rendered template log

case "${DIRECTION}" in
    archive_only|cleanup_only)
        echo "[INFO] transfer_files: skipped (direction=${DIRECTION})"
        exit 0
        ;;
esac

# MVS datasets have no leading slash — wrap in single quotes for mainframe FTP
mvs_quote() {
    local p="$1"
    if [[ "${p}" != /* && "${p}" != \\* ]]; then echo "'${p}'"; else echo "${p}"; fi
}

# ── FTP / FTPS via lftp ───────────────────────────────────────────────────────
do_ftp_transfer() {
    echo "[INFO] === lftp File Transfer ==="
    echo "[INFO] lftp version   : $(lftp --version 2>&1 | head -1)"
    echo "[INFO] Direction      : ${DIRECTION}"
    echo "[INFO] Source path    : ${SRC_PATH}"
    echo "[INFO] Dest host      : ${DEST_HOST}:${DEST_PORT}"
    echo "[INFO] Dest user      : ${DEST_USER} (password suppressed)"
    echo "[INFO] Dest path      : ${DEST_PATH}"
    echo "[INFO] Protocol       : ${DEST_PROTOCOL}"
    echo "[DEBUG] DNS           : $(getent hosts ${DEST_HOST} 2>/dev/null | awk '{print $1}' || echo 'n/a')"
    echo "[DEBUG] Port check    : $(bash -c 'echo > /dev/tcp/${DEST_HOST}/${DEST_PORT}' 2>/dev/null && echo reachable || echo unreachable)"

    LFTP_RC=$(mktemp /tmp/lftprc.XXXXXX)
    cat > "${LFTP_RC}" << 'LFTPRC'
set ssl:verify-certificate false
set ssl:ca-file ""
set ftp:ssl-force true
set ftp:ssl-auth TLS
set ftp:ssl-protect-data true
set ftp:passive-mode yes
set cmd:verbose false
set xfer:log true
set xfer:clobber true
LFTPRC

    case "${DIRECTION}" in
        upload|upload_delete|upload_rename|upload_move)
            RPATH=$(mvs_quote "${DEST_PATH}")
            echo "[INFO] Action : put ${SRC_PATH} → ${DEST_HOST}:${RPATH}"
            lftp -f "${LFTP_RC}" \
                -e "open -u ${DEST_USER},${REMOTE_PASS} ftp://${DEST_HOST}:${DEST_PORT}; \
                    put -a \"${SRC_PATH}\" -o \"${RPATH}\"; \
                    bye"
            ;;
        download|download_delete|download_rename|download_move|download_archive)
            echo "[INFO] Action : mget ${DEST_HOST}:${SRC_PATH} → ${DEST_PATH}"
            mkdir -p "${DEST_PATH}"
            BEFORE=$(ls "${DEST_PATH}" 2>/dev/null | wc -l | tr -d ' ')
            lftp -f "${LFTP_RC}" \
                -e "open -u ${DEST_USER},${REMOTE_PASS} ftp://${DEST_HOST}:${DEST_PORT}; \
                    lcd ${DEST_PATH}; \
                    mget ${SRC_PATH}; \
                    bye"
            AFTER=$(ls "${DEST_PATH}" 2>/dev/null | wc -l | tr -d ' ')
            echo "[INFO] Files downloaded: $(( AFTER - BEFORE ))"
            ls -lh "${DEST_PATH}" 2>/dev/null | grep -v '^total' | awk '{print "[DEBUG]   " $0}' || true
            ;;
        *)
            echo "[ERROR] Unknown direction for ftp/ftps: ${DIRECTION}"
            rm -f "${LFTP_RC}"; exit 1
            ;;
    esac
    rm -f "${LFTP_RC}"
}

# ── SFTP via lftp ─────────────────────────────────────────────────────────────
do_sftp_transfer() {
    echo "[INFO] === SFTP File Transfer (lftp) ==="
    echo "[INFO] Direction   : ${DIRECTION}"
    echo "[INFO] Source path : ${SRC_PATH}"
    echo "[INFO] Dest host   : ${DEST_HOST}:${DEST_PORT}"
    echo "[INFO] Dest user   : ${DEST_USER} (password suppressed)"
    echo "[INFO] Dest path   : ${DEST_PATH}"

    LFTP_RC=$(mktemp /tmp/lftprc.XXXXXX)
    cat > "${LFTP_RC}" << 'LFTPRC'
set sftp:auto-confirm true
set cmd:verbose false
set xfer:log true
set xfer:clobber true
LFTPRC

    case "${DIRECTION}" in
        upload|upload_delete|upload_rename|upload_move)
            echo "[INFO] Action : sftp put ${SRC_PATH} → ${DEST_HOST}:${DEST_PATH}"
            lftp -f "${LFTP_RC}" \
                -e "open -u ${DEST_USER},${REMOTE_PASS} sftp://${DEST_HOST}:${DEST_PORT}; \
                    put -a \"${SRC_PATH}\" -o \"${DEST_PATH}\"; \
                    bye"
            ;;
        download|download_delete|download_rename|download_move|download_archive)
            echo "[INFO] Action : sftp mget ${DEST_HOST}:${SRC_PATH} → ${DEST_PATH}"
            mkdir -p "${DEST_PATH}"
            BEFORE=$(ls "${DEST_PATH}" 2>/dev/null | wc -l | tr -d ' ')
            lftp -f "${LFTP_RC}" \
                -e "open -u ${DEST_USER},${REMOTE_PASS} sftp://${DEST_HOST}:${DEST_PORT}; \
                    lcd ${DEST_PATH}; \
                    mget ${SRC_PATH}; \
                    bye"
            AFTER=$(ls "${DEST_PATH}" 2>/dev/null | wc -l | tr -d ' ')
            echo "[INFO] Files downloaded: $(( AFTER - BEFORE ))"
            ls -lh "${DEST_PATH}" 2>/dev/null | grep -v '^total' | awk '{print "[DEBUG]   " $0}' || true
            ;;
        *)
            echo "[ERROR] Unknown direction for sftp: ${DIRECTION}"
            rm -f "${LFTP_RC}"; exit 1
            ;;
    esac
    rm -f "${LFTP_RC}"
}

# ── S3 via AWS CLI ────────────────────────────────────────────────────────────
do_s3_transfer() {
    S3_BUCKET="{{ params.s3_bucket }}"
    S3_PREFIX="{{ params.s3_prefix }}"
    S3_REGION="{{ params.s3_region }}"
    S3_ENDPOINT="{{ params.s3_endpoint_url }}"
    AWS_PROFILE="{{ params.aws_profile }}"

    if [ -n "${AWS_PROFILE}" ]; then AWS_CMD="aws --profile ${AWS_PROFILE}"; else AWS_CMD="aws"; fi
    ENDPOINT_ARGS=""; [ -n "${S3_ENDPOINT}" ] && ENDPOINT_ARGS="--endpoint-url ${S3_ENDPOINT} --no-verify-ssl"

    echo "[INFO] === S3 Transfer (AWS CLI) ==="
    echo "[INFO] aws version  : $(aws --version 2>&1)"
    echo "[INFO] Direction    : ${DIRECTION}"
    echo "[INFO] Profile      : ${AWS_PROFILE:-default/instance-role}"
    echo "[INFO] Region       : ${S3_REGION}"
    echo "[INFO] Endpoint     : ${S3_ENDPOINT:-public}"

    case "${DIRECTION}" in
        s3_upload)
            S3_TARGET="s3://${S3_BUCKET}/${S3_PREFIX}"
            echo "[INFO] Action  : aws s3 cp ${SRC_PATH} → ${S3_TARGET}"
            ${AWS_CMD} s3 cp "${SRC_PATH}" "${S3_TARGET}" \
                --recursive --region "${S3_REGION}" ${ENDPOINT_ARGS}
            echo "[INFO] Uploaded:"
            ${AWS_CMD} s3 ls "${S3_TARGET}" --region "${S3_REGION}" ${ENDPOINT_ARGS} \
                | awk '{print "[DEBUG]   " $0}' || true
            ;;
        s3_download)
            S3_SOURCE="s3://${S3_BUCKET}/${S3_PREFIX}"
            echo "[INFO] Action  : aws s3 cp ${S3_SOURCE} → ${DEST_PATH}"
            mkdir -p "${DEST_PATH}"
            ${AWS_CMD} s3 cp "${S3_SOURCE}" "${DEST_PATH}" \
                --recursive --region "${S3_REGION}" ${ENDPOINT_ARGS}
            FILE_COUNT=$(ls "${DEST_PATH}" 2>/dev/null | wc -l | tr -d ' ')
            echo "[INFO] Downloaded: ${FILE_COUNT} file(s) → ${DEST_PATH}"
            ls -lh "${DEST_PATH}" 2>/dev/null | grep -v '^total' | awk '{print "[DEBUG]   " $0}' || true
            ;;
        *)
            echo "[ERROR] Unknown direction for s3: ${DIRECTION} (use s3_upload or s3_download)"
            exit 1
            ;;
    esac
}

# ── Azure Blob via azcopy ─────────────────────────────────────────────────────
do_blob_transfer() {
    BLOB_ACCOUNT="{{ params.blob_account }}"
    BLOB_CONTAINER="{{ params.blob_container }}"
    BLOB_PREFIX="{{ params.blob_prefix }}"
    BLOB_IDENTITY_CLIENT_ID="{{ params.blob_identity_client_id }}"
    BLOB_SAS="{{ var.value.get(params.blob_sas_var_name, "") }}"
    # WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set

    echo "[INFO] === Azure Blob Transfer (azcopy) ==="
    echo "[INFO] azcopy version : $(azcopy --version 2>&1 | head -1)"
    echo "[INFO] Direction      : ${DIRECTION}"
    echo "[INFO] Account        : ${BLOB_ACCOUNT}"
    echo "[INFO] Container      : ${BLOB_CONTAINER}"
    echo "[INFO] Prefix         : ${BLOB_PREFIX}"

    BLOB_BASE_URL="https://${BLOB_ACCOUNT}.blob.core.windows.net/${BLOB_CONTAINER}/${BLOB_PREFIX}"

    if [ -n "${BLOB_SAS}" ]; then
        BLOB_URL="${BLOB_BASE_URL}?${BLOB_SAS}"
        echo "[INFO] Auth : SAS token (suppressed)"
    else
        echo "[INFO] Auth : managed identity"
        if [ -n "${BLOB_IDENTITY_CLIENT_ID}" ]; then
            azcopy login --identity --identity-client-id "${BLOB_IDENTITY_CLIENT_ID}"
        else
            azcopy login --identity
        fi
        BLOB_URL="${BLOB_BASE_URL}"
    fi

    case "${DIRECTION}" in
        blob_upload)
            echo "[INFO] Action  : azcopy copy ${SRC_PATH} → ${BLOB_BASE_URL}"
            azcopy copy "${SRC_PATH}" "${BLOB_URL}" \
                --recursive=true \
                --overwrite=true \
                --log-level=INFO
            echo "[INFO] Uploaded to ${BLOB_BASE_URL}"
            azcopy list "${BLOB_URL%%\\?*}" 2>/dev/null | awk '{print "[DEBUG]   " $0}' || true
            ;;
        blob_download)
            echo "[INFO] Action  : azcopy copy ${BLOB_BASE_URL} → ${DEST_PATH}"
            mkdir -p "${DEST_PATH}"
            azcopy copy "${BLOB_URL}" "${DEST_PATH}" \
                --recursive=true \
                --overwrite=true \
                --log-level=INFO
            FILE_COUNT=$(ls "${DEST_PATH}" 2>/dev/null | wc -l | tr -d ' ')
            echo "[INFO] Downloaded: ${FILE_COUNT} file(s) → ${DEST_PATH}"
            ls -lh "${DEST_PATH}" 2>/dev/null | grep -v '^total' | awk '{print "[DEBUG]   " $0}' || true
            ;;
        *)
            echo "[ERROR] Unknown direction for blob: ${DIRECTION} (use blob_upload or blob_download)"
            exit 1
            ;;
    esac
}

# ── S3 ↔ Blob via azcopy (no staging) ────────────────────────────────────────
do_cloud_to_cloud_transfer() {
    S3_BUCKET="{{ params.s3_bucket }}"
    S3_PREFIX="{{ params.s3_prefix }}"
    S3_REGION="{{ params.s3_region }}"
    S3_ENDPOINT="{{ params.s3_endpoint_url }}"
    AWS_KEY_ID_VAR="{{ params.aws_access_key_id_var }}"
    BLOB_ACCOUNT="{{ params.blob_account }}"
    BLOB_CONTAINER="{{ params.blob_container }}"
    BLOB_PREFIX="{{ params.blob_prefix }}"
    BLOB_IDENTITY_CLIENT_ID="{{ params.blob_identity_client_id }}"
    BLOB_SAS="{{ var.value.get(params.blob_sas_var_name, "") }}"
    # WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set

    echo "[INFO] === S3 ↔ Blob Transfer (azcopy, no staging) ==="
    echo "[INFO] azcopy version : $(azcopy --version 2>&1 | head -1)"
    echo "[INFO] Direction      : ${DIRECTION}"

    if [ -n "${AWS_KEY_ID_VAR}" ]; then
        export AWS_ACCESS_KEY_ID="{{ var.value.get(params.aws_access_key_id_var, '') }}"
        export AWS_SECRET_ACCESS_KEY="{{ var.value.get(params.aws_secret_key_var, '') }}"
        # WARNING: AWS credentials visible in Airflow rendered template log
        echo "[INFO] S3 auth   : explicit credentials (suppressed)"
    else
        echo "[INFO] S3 auth   : node IAM role / instance profile"
    fi

    BLOB_BASE_URL="https://${BLOB_ACCOUNT}.blob.core.windows.net/${BLOB_CONTAINER}/${BLOB_PREFIX}"

    if [ -n "${BLOB_SAS}" ]; then
        BLOB_URL="${BLOB_BASE_URL}?${BLOB_SAS}"
        echo "[INFO] Blob auth : SAS token (suppressed)"
    else
        echo "[INFO] Blob auth : managed identity"
        if [ -n "${BLOB_IDENTITY_CLIENT_ID}" ]; then
            azcopy login --identity --identity-client-id "${BLOB_IDENTITY_CLIENT_ID}"
        else
            azcopy login --identity
        fi
        BLOB_URL="${BLOB_BASE_URL}"
    fi

    if [ -n "${S3_ENDPOINT}" ]; then
        S3_URL="${S3_ENDPOINT}/${S3_BUCKET}/${S3_PREFIX}"
    else
        S3_URL="https://s3.amazonaws.com/${S3_BUCKET}/${S3_PREFIX}"
    fi
    echo "[INFO] S3  URL : ${S3_URL}"
    echo "[INFO] Blob URL: ${BLOB_BASE_URL}"

    if [ "${DIRECTION}" = "s3_to_blob" ]; then
        echo "[INFO] Action  : azcopy copy S3 → Blob"
        azcopy copy "${S3_URL}" "${BLOB_URL}" \
            --recursive=true \
            --overwrite=true \
            --log-level=INFO
    else
        echo "[INFO] Action  : azcopy copy Blob → S3"
        azcopy copy "${BLOB_URL}" "${S3_URL}" \
            --recursive=true \
            --overwrite=true \
            --log-level=INFO
    fi
    echo "[INFO] ${DIRECTION} completed successfully"
}

# ── Dispatch ──────────────────────────────────────────────────────────────────
case "${DEST_PROTOCOL}" in
    ftp|ftps)         do_ftp_transfer ;;
    sftp)             do_sftp_transfer ;;
    s3)               do_s3_transfer ;;
    blob)             do_blob_transfer ;;
    *)
        case "${DIRECTION}" in
            s3_to_blob|blob_to_s3) do_cloud_to_cloud_transfer ;;
            *) echo "[ERROR] Unsupported protocol: ${DEST_PROTOCOL} (use ftp, ftps, sftp, s3, blob, or direction s3_to_blob/blob_to_s3)"; exit 1 ;;
        esac
        ;;
esac

echo "[INFO] Transfer completed successfully"
BASH
""",
        cmd_timeout=3600,
        on_failure_callback=failure_callback,
    )

    # Verify transfer — self-skips for archive_only / cleanup_only
    task_verify_transfer = DynamicSSHOperator(
        task_id='verify_transfer',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Verify failed at line $LINENO — exit $?"' ERR

DIRECTION="{{ params.direction }}"
SRC_PATH="{{ params.source_path }}"
DEST_PATH="{{ params.dest_path }}"
S3_BUCKET="{{ params.s3_bucket }}"
S3_PREFIX="{{ params.s3_prefix }}"
S3_REGION="{{ params.s3_region }}"
S3_ENDPOINT="{{ params.s3_endpoint_url }}"
AWS_PROFILE="{{ params.aws_profile }}"
BLOB_ACCOUNT="{{ params.blob_account }}"
BLOB_CONTAINER="{{ params.blob_container }}"
BLOB_PREFIX="{{ params.blob_prefix }}"
BLOB_SAS="{{ var.value.get(params.blob_sas_var_name, "") }}"
# WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set

case "${DIRECTION}" in
    archive_only|cleanup_only)
        echo "[INFO] verify_transfer: skipped (direction=${DIRECTION})"
        exit 0
        ;;
esac

echo "[INFO] === Verify Transfer ==="
echo "[INFO] Direction: ${DIRECTION}"

verify_lftp_upload() {
    if ls ${SRC_PATH} 1>/dev/null 2>&1; then
        FILE_COUNT=$(ls ${SRC_PATH} 2>/dev/null | wc -l | tr -d ' ')
        ls -lh ${SRC_PATH} | awk '{print "[DEBUG]   " $0}'
        echo "[INFO] Verification passed — ${FILE_COUNT} file(s) present"
    else
        echo "[ERROR] Source file not found after upload: ${SRC_PATH}"
        exit 1
    fi
}

verify_lftp_download() {
    SRC_PATTERN=$(basename "${SRC_PATH}")
    if ls "${DEST_PATH}"/${SRC_PATTERN} 1>/dev/null 2>&1; then
        FILE_COUNT=$(ls "${DEST_PATH}"/${SRC_PATTERN} 2>/dev/null | wc -l | tr -d ' ')
        TOTAL_SIZE=$(du -sh "${DEST_PATH}"/${SRC_PATTERN} 2>/dev/null | awk '{print $1}' || echo 'n/a')
        ls -lh "${DEST_PATH}"/${SRC_PATTERN} | awk '{print "[DEBUG]   " $0}'
        echo "[INFO] Verification passed — ${FILE_COUNT} file(s), size: ${TOTAL_SIZE}"
    else
        echo "[ERROR] No files matching ${SRC_PATTERN} at ${DEST_PATH}"
        exit 1
    fi
}

verify_s3() {
    if [ -n "${AWS_PROFILE}" ]; then AWS_CMD="aws --profile ${AWS_PROFILE}"; else AWS_CMD="aws"; fi
    ENDPOINT_ARGS=""; [ -n "${S3_ENDPOINT}" ] && ENDPOINT_ARGS="--endpoint-url ${S3_ENDPOINT} --no-verify-ssl"
    S3_TARGET="s3://${S3_BUCKET}/${S3_PREFIX}"
    FILE_LIST=$(${AWS_CMD} s3 ls "${S3_TARGET}" --region "${S3_REGION}" ${ENDPOINT_ARGS} 2>&1)
    FILE_COUNT=$(echo "${FILE_LIST}" | grep -c "^[0-9]" || true)
    echo "${FILE_LIST}" | awk '{print "[DEBUG]   " $0}'
    echo "[INFO] Verification passed — ${FILE_COUNT} object(s) in S3"
}

verify_blob() {
    BLOB_BASE_URL="https://${BLOB_ACCOUNT}.blob.core.windows.net/${BLOB_CONTAINER}/${BLOB_PREFIX}"
    if [ -n "${BLOB_SAS}" ]; then BLOB_URL="${BLOB_BASE_URL}?${BLOB_SAS}"; else BLOB_URL="${BLOB_BASE_URL}"; fi
    echo "[INFO] Blob URL : ${BLOB_BASE_URL}"
    FILE_LIST=$(azcopy list "${BLOB_URL%%\\?*}" 2>&1 || true)
    FILE_COUNT=$(echo "${FILE_LIST}" | grep -vc '^INFO\|^$' || true)
    echo "${FILE_LIST}" | awk '{print "[DEBUG]   " $0}'
    echo "[INFO] Verification passed — ${FILE_COUNT} blob(s) at ${BLOB_BASE_URL}"
}

verify_local() {
    FILE_COUNT=$(ls "${DEST_PATH}" 2>/dev/null | wc -l | tr -d ' ')
    ls -lh "${DEST_PATH}" 2>/dev/null | grep -v '^total' | awk '{print "[DEBUG]   " $0}' || true
    echo "[INFO] Verification passed — ${FILE_COUNT} file(s) in ${DEST_PATH}"
}

case "${DIRECTION}" in
    upload|upload_delete|upload_rename|upload_move)
        echo "[INFO] Mode : upload — verifying source still exists"
        verify_lftp_upload
        ;;
    download|download_delete|download_rename|download_move|download_archive)
        echo "[INFO] Mode : download — verifying destination"
        verify_lftp_download
        ;;
    s3_upload)
        echo "[INFO] Mode : s3_upload — verifying S3 destination"
        verify_s3
        ;;
    s3_download)
        echo "[INFO] Mode : s3_download — verifying local destination"
        verify_local
        ;;
    blob_upload)
        echo "[INFO] Mode : blob_upload — verifying Blob destination"
        verify_blob
        ;;
    blob_download)
        echo "[INFO] Mode : blob_download — verifying local destination"
        verify_local
        ;;
    s3_to_blob)
        echo "[INFO] Mode : s3_to_blob — verifying Blob destination"
        BLOB_ACCOUNT="{{ params.blob_account }}"
        BLOB_CONTAINER="{{ params.blob_container }}"
        BLOB_PREFIX="{{ params.blob_prefix }}"
        BLOB_SAS="{{ var.value.get(params.blob_sas_var_name, "") }}"
        # WARNING: SAS token visible in Airflow rendered template log if blob_sas_var_name is set
        verify_blob
        ;;
    blob_to_s3)
        echo "[INFO] Mode : blob_to_s3 — verifying S3 destination"
        S3_BUCKET="{{ params.s3_bucket }}"
        S3_PREFIX="{{ params.s3_prefix }}"
        S3_REGION="{{ params.s3_region }}"
        S3_ENDPOINT="{{ params.s3_endpoint_url }}"
        AWS_PROFILE="{{ params.aws_profile }}"
        verify_s3
        ;;
esac
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    # Post-transfer action — direction-specific source management + archive/cleanup
    task_post_transfer = DynamicSSHOperator(
        task_id='post_transfer_action',
        ssh_conn_id="{{ params.ssh_conn_id }}",
        command="""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] Post-transfer action failed at line $LINENO — exit $?"' ERR

DIRECTION="{{ params.direction }}"
SRC_PATH="{{ params.source_path }}"
DEST_PATH="{{ params.dest_path }}"
NEW_NAME="{{ params.new_name }}"
ARCHIVE_PATH="{{ params.archive_path }}"
RETENTION_DAYS="{{ params.retention_days }}"
DATE="{{ ds_nodash }}"
REMOTE_PASS="{{ var.value.get(params.password_var_name, "") }}"
DEST_HOST="{{ params.dest_host }}"
DEST_PORT="{{ params.dest_port }}"
DEST_USER="{{ params.dest_user }}"
# WARNING: password visible in Airflow rendered template log

ARCHIVE_BASE="${ARCHIVE_PATH:-${DEST_PATH}/archive}"
ARCHIVE_DIR="${ARCHIVE_BASE}/${DATE}"

echo "[INFO] === Post-Transfer Action: ${DIRECTION} ==="

# Temp rc file for lftp post-actions (delete/rename/move on remote)
LFTP_RC=$(mktemp /tmp/lftprc.XXXXXX)
cat > "${LFTP_RC}" << 'LFTPRC'
set ssl:verify-certificate false
set ssl:ca-file ""
set ftp:ssl-force true
set ftp:ssl-auth TLS
set ftp:ssl-protect-data true
set ftp:passive-mode yes
set cmd:verbose false
LFTPRC

mvs_quote() {
    local p="$1"
    if [[ "${p}" != /* && "${p}" != \\* ]]; then echo "'${p}'"; else echo "${p}"; fi
}

case "${DIRECTION}" in

    upload)
        echo "[INFO] Action: none — source kept (SRCOPT=0)"
        ;;

    upload_delete)
        echo "[INFO] Action: delete local source (SRCOPT=1)"
        FILE_COUNT=$(ls ${SRC_PATH} 2>/dev/null | wc -l | tr -d ' ')
        rm -f ${SRC_PATH}
        echo "[INFO] Source deleted — ${FILE_COUNT} file(s) removed"
        ;;

    upload_rename)
        echo "[INFO] Action: rename local source to ${NEW_NAME} (SRCOPT=2)"
        [ -z "${NEW_NAME}" ] && { echo "[ERROR] new_name is required for upload_rename"; rm -f "${LFTP_RC}"; exit 1; }
        SRC_DIR=$(dirname "${SRC_PATH}")
        mv "${SRC_PATH}" "${SRC_DIR}/${NEW_NAME}"
        echo "[INFO] Source renamed: ${SRC_PATH} → ${SRC_DIR}/${NEW_NAME}"
        ;;

    upload_move)
        echo "[INFO] Action: move local source to ${NEW_NAME} (SRCOPT=3)"
        [ -z "${NEW_NAME}" ] && { echo "[ERROR] new_name is required for upload_move"; rm -f "${LFTP_RC}"; exit 1; }
        mkdir -p "$(dirname "${NEW_NAME}")"
        mv "${SRC_PATH}" "${NEW_NAME}"
        echo "[INFO] Source moved: ${SRC_PATH} → ${NEW_NAME}"
        ;;

    download)
        echo "[INFO] Action: none — remote source kept (SRCOPT=0)"
        ;;

    download_delete)
        echo "[INFO] Action: delete remote source (SRCOPT=1)"
        RPATH=$(mvs_quote "${SRC_PATH}")
        lftp -f "${LFTP_RC}" \
            -e "open -u ${DEST_USER},${REMOTE_PASS} ftp://${DEST_HOST}:${DEST_PORT}; \
                rm \"${RPATH}\"; \
                bye"
        echo "[INFO] Remote source deleted: ${SRC_PATH}"
        ;;

    download_rename)
        echo "[INFO] Action: rename remote source to ${NEW_NAME} (SRCOPT=2)"
        [ -z "${NEW_NAME}" ] && { echo "[ERROR] new_name is required for download_rename"; rm -f "${LFTP_RC}"; exit 1; }
        SRC_DIR=$(dirname "${SRC_PATH}")
        RPATH_SRC=$(mvs_quote "${SRC_PATH}")
        RPATH_DST=$(mvs_quote "${SRC_DIR}/${NEW_NAME}")
        lftp -f "${LFTP_RC}" \
            -e "open -u ${DEST_USER},${REMOTE_PASS} ftp://${DEST_HOST}:${DEST_PORT}; \
                mv \"${RPATH_SRC}\" \"${RPATH_DST}\"; \
                bye"
        echo "[INFO] Remote source renamed: ${SRC_PATH} → ${SRC_DIR}/${NEW_NAME}"
        ;;

    download_move)
        echo "[INFO] Action: move remote source to ${NEW_NAME} (SRCOPT=3)"
        [ -z "${NEW_NAME}" ] && { echo "[ERROR] new_name is required for download_move"; rm -f "${LFTP_RC}"; exit 1; }
        RPATH_SRC=$(mvs_quote "${SRC_PATH}")
        RPATH_DST=$(mvs_quote "${NEW_NAME}")
        lftp -f "${LFTP_RC}" \
            -e "open -u ${DEST_USER},${REMOTE_PASS} ftp://${DEST_HOST}:${DEST_PORT}; \
                mv \"${RPATH_SRC}\" \"${RPATH_DST}\"; \
                bye"
        echo "[INFO] Remote source moved: ${SRC_PATH} → ${NEW_NAME}"
        ;;

    download_archive)
        echo "[INFO] Action: archive downloaded files locally"
        SRC_PATTERN=$(basename "${SRC_PATH}")
        mkdir -p "${ARCHIVE_DIR}"
        if ls "${DEST_PATH}"/${SRC_PATTERN} 1>/dev/null 2>&1; then
            FILE_COUNT=$(ls "${DEST_PATH}"/${SRC_PATTERN} 2>/dev/null | wc -l | tr -d ' ')
            mv "${DEST_PATH}"/${SRC_PATTERN} "${ARCHIVE_DIR}/"
            echo "[INFO] Archived ${FILE_COUNT} file(s) → ${ARCHIVE_DIR}"
        else
            echo "[INFO] No files to archive matching ${SRC_PATTERN}"
        fi
        ;;

    archive_only)
        echo "[INFO] Action: archive existing files in ${DEST_PATH}"
        SRC_PATTERN=$(basename "${SRC_PATH}")
        mkdir -p "${ARCHIVE_DIR}"
        if ls "${DEST_PATH}"/${SRC_PATTERN} 1>/dev/null 2>&1; then
            FILE_COUNT=$(ls "${DEST_PATH}"/${SRC_PATTERN} 2>/dev/null | wc -l | tr -d ' ')
            mv "${DEST_PATH}"/${SRC_PATTERN} "${ARCHIVE_DIR}/"
            echo "[INFO] Archived ${FILE_COUNT} file(s) → ${ARCHIVE_DIR}"
        else
            echo "[INFO] No files to archive matching ${SRC_PATTERN}"
        fi
        ;;

    cleanup_only)
        echo "[INFO] Action: cleanup archives older than ${RETENTION_DAYS} days"
        echo "[INFO] Archive base: ${ARCHIVE_BASE}"
        if [ -d "${ARCHIVE_BASE}" ]; then
            REMOVED=$(find "${ARCHIVE_BASE}" -type f -mtime +"${RETENTION_DAYS}" | wc -l | tr -d ' ')
            find "${ARCHIVE_BASE}" -type f -mtime +"${RETENTION_DAYS}" -exec ls -lh {} \;
            find "${ARCHIVE_BASE}" -type f -mtime +"${RETENTION_DAYS}" -delete
            find "${ARCHIVE_BASE}" -mindepth 1 -type d -empty -delete 2>/dev/null || true
            echo "[INFO] Cleanup completed — ${REMOVED} file(s) removed"
        else
            echo "[INFO] Archive directory not found, skipping: ${ARCHIVE_BASE}"
        fi
        ;;

    s3_upload|s3_download)
        echo "[INFO] Action: none — S3 transfer, source kept"
        ;;

    blob_upload|blob_download)
        echo "[INFO] Action: none — Blob transfer, source kept"
        ;;

    s3_to_blob|blob_to_s3)
        echo "[INFO] Action: none — cloud-to-cloud transfer, source kept"
        ;;

    *)
        echo "[ERROR] Unknown direction: ${DIRECTION}"
        rm -f "${LFTP_RC}"; exit 1
        ;;
esac

rm -f "${LFTP_RC}"
echo "[INFO] Post-transfer action completed"
BASH
""",
        cmd_timeout=600,
        on_failure_callback=failure_callback,
    )

    # Post-command — self-skips when params.post_command is empty
    task_postcomm = DynamicSSHOperator(
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
ARGS_JSON='{{ params.post_command_args | tojson }}'
ARGS_STR=$(echo "$ARGS_JSON" | python3 -c "import json,sys; print(' '.join(json.load(sys.stdin)))" 2>/dev/null || true)
FULL_CMD="$CMD${ARGS_STR:+ $ARGS_STR}"
echo "[INFO] post_command: $FULL_CMD"
if [ "$CMD" = "chmod" ] && [ -n "$ARGS_STR" ]; then
    GLOB_PATH=$(echo "$ARGS_STR" | awk '{print $NF}')
    MATCH_COUNT=$(ls -1 $GLOB_PATH 2>/dev/null | wc -l | tr -d ' ') || MATCH_COUNT=0
    if [ "$MATCH_COUNT" -eq 0 ]; then
        echo "[WARN] post_command: chmod skipped — no files matched: $GLOB_PATH"
        exit 0
    fi
    echo "[INFO] post_command: chmod target — $MATCH_COUNT file(s) matched"
fi
bash -c "$FULL_CMD"
BASH
""",
        cmd_timeout=300,
        on_failure_callback=failure_callback,
    )

    ###################### Task Dependencies ######################

    start >> task_validate_params >> task_precomm >> task_validate_source >> task_transfer_files >> task_verify_transfer >> task_post_transfer >> task_postcomm >> end
