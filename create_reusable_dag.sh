#!/usr/bin/env bash
# create_reusable_dag.sh — Generate reusable file-transfer DAGs (Unix and/or Windows)
#                          from parameters, without an XML input.
#                          Stamps ##PLACEHOLDER## tokens in the tracked templates and
#                          writes the final .py files to output/reusable/.
#                          Also scans input/test_nonprod/*.xml and writes a JSON catalogue
#                          of each scenario's transfer parameters.
#
# Usage:
#   ./create_reusable_dag.sh --unix   [parameters...]   # Unix DAG only
#   ./create_reusable_dag.sh --win    [parameters...]   # Windows DAG only
#   ./create_reusable_dag.sh --both   [parameters...]   # both DAGs (default)
#   ./create_reusable_dag.sh --scan                     # scan XMLs → write JSON catalogue
#   ./create_reusable_dag.sh --list                     # list required parameters
#   ./create_reusable_dag.sh --help
#
# Required parameters (key=value after the mode flag):
#   company          Company prefix (e.g. nix)
#   project          App ID (e.g. apxxxx)
#   app_code         App code (e.g. poc)
#   env              Environment: dev | sit | uat | prod | nonprod
#   dag_name         DAG name suffix (e.g. file-transfer-unix-daily)
#
# Optional parameters:
#   active           true | false (default: true)
#   tags             Comma-separated tags (default: company,project,app_code,env)
#   email_list       Comma-separated email addresses (default: [])
#   email_success    true | false (default: false)
#   email_fail       true | false (default: false)
#   output_dir       Output directory (default: output/reusable)
#
# Example:
#   ./create_reusable_dag.sh --both \
#       company=nix project=apxxxx app_code=poc env=nonprod \
#       dag_name=transfer-daily \
#       email_list=ops@example.com,alerts@example.com \
#       email_fail=true
#
# Output:
#   output/reusable/<company>-<project>-<app_code>-file-transfer-unix-<dag_name>-<env>.py
#   output/reusable/<company>-<project>-<app_code>-file-transfer-win-<dag_name>-<env>.py
#
# Templates (tracked in git):
#   skills/controlm2airflow/templates/file-transfer-reusable-unix-dynamic/dags/project_file_transfer_unix_dynamic.py
#   skills/controlm2airflow/templates/file-transfer-reusable-win-dynamic/dags/project_file_transfer_win_dynamic.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="${SCRIPT_DIR}/logs"
SUMMARY_LOG="${LOG_DIR}/create_reusable_dag_${TIMESTAMP}.log"

UNIX_TEMPLATE="${SCRIPT_DIR}/skills/controlm2airflow/templates/file-transfer-reusable-unix-dynamic/dags/project_file_transfer_unix_dynamic.py"
WIN_TEMPLATE="${SCRIPT_DIR}/skills/controlm2airflow/templates/file-transfer-reusable-win-dynamic/dags/project_file_transfer_win_dynamic.py"

mkdir -p "${LOG_DIR}"

# ─── Colours ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()   { echo -e "$*" | tee -a "${SUMMARY_LOG}"; }
pass()  { log "${GREEN}[PASS]${NC}  $*"; }
fail()  { log "${RED}[FAIL]${NC}  $*"; }
info()  { log "${YELLOW}[INFO]${NC}  $*"; }
debug() { log "${CYAN}[DEBUG]${NC} $*"; }
step()  { log "${BOLD}[STEP]${NC}  $*"; }
sep()   { log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }

# ─── Usage ────────────────────────────────────────────────────────────────────
usage() {
    sed -n '2,45p' "$0" | sed 's/^# \{0,1\}//'
}

# ─── Argument parsing ─────────────────────────────────────────────────────────
MODE="both"
PARAMS=()

if [ $# -eq 0 ]; then
    usage
    exit 0
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --unix|-u)   MODE="unix"; shift ;;
        --win|-w)    MODE="win";  shift ;;
        --both|-b)   MODE="both"; shift ;;
        --scan|-s)   MODE="scan"; shift ;;
        --list|-l)   usage; exit 0 ;;
        --help|-h)   usage; exit 0 ;;
        *=*)         PARAMS+=("$1"); shift ;;
        *)
            echo "[ERROR] Unknown argument: $1"
            echo "Run '$0 --help' for usage."
            exit 1
            ;;
    esac
done

# ─── Parse key=value params (bash 3 compatible) ───────────────────────────────
P_company=""; P_project=""; P_app_code=""; P_env=""; P_dag_name=""
P_active="false"; P_tags=""; P_email_list=""; P_email_success="false"
P_email_fail="false"; P_output_dir=""

for kv in "${PARAMS[@]+"${PARAMS[@]}"}"; do
    key="${kv%%=*}"; val="${kv#*=}"
    case "${key}" in
        company)       P_company="${val}" ;;
        project)       P_project="${val}" ;;
        app_code)      P_app_code="${val}" ;;
        env)           P_env="${val}" ;;
        dag_name)      P_dag_name="${val}" ;;
        active)        P_active="${val}" ;;
        tags)          P_tags="${val}" ;;
        email_list)    P_email_list="${val}" ;;
        email_success) P_email_success="${val}" ;;
        email_fail)    P_email_fail="${val}" ;;
        output_dir)    P_output_dir="${val}" ;;
        *)
            echo "[WARN]  Unknown parameter: ${key}=${val}" ;;
    esac
done

# ─── Validate required params (not needed for --scan) ────────────────────────
if [ "${MODE}" != "scan" ]; then
    MISSING=()
    for req in company project app_code env dag_name; do
        eval "val=\$P_${req}"
        [ -z "${val}" ] && MISSING+=("${req}")
    done

    if [ ${#MISSING[@]} -gt 0 ]; then
        echo "[ERROR] Missing required parameters: ${MISSING[*]}"
        echo "Run '$0 --help' for usage."
        exit 1
    fi
fi

# ─── Derive values ────────────────────────────────────────────────────────────
COMPANY="${P_company}"
PROJECT="${P_project}"
APP_CODE="${P_app_code}"
ENV="${P_env}"
DAG_NAME="${P_dag_name}"
ACTIVE="${P_active}"
OUTPUT_DIR="${P_output_dir:-${SCRIPT_DIR}/output/reusable}"

# Tags: default to comma-separated list → Python list literal
if [ -n "${P_tags}" ]; then
    # Convert "a,b,c" → ["a", "b", "c"]
    TAGS_PY="[$(echo "${P_tags}" | sed 's/,/, /g' | sed "s/[^, ][^,]*/\"&\"/g")]"
else
    TAGS_PY="[\"${COMPANY}\", \"${PROJECT}\", \"${APP_CODE}\", \"${ENV}\"]"
fi

# Email list: convert "a@x,b@x" → ["a@x", "b@x"]
if [ -n "${P_email_list}" ]; then
    EMAIL_LIST_PY="[$(echo "${P_email_list}" | sed 's/,/, /g' | sed "s/[^, ][^,]*/\"&\"/g")]"
else
    EMAIL_LIST_PY="[]"
fi

# Boolean caps for Python
ACTIVE_PY="$(echo "${ACTIVE}" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')"
EMAIL_SUCCESS_PY="$(echo "${P_email_success}" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')"
EMAIL_FAIL_PY="$(echo "${P_email_fail}" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')"

mkdir -p "${OUTPUT_DIR}"

# ─── Banner ───────────────────────────────────────────────────────────────────
log "========================================"
log "  create_reusable_dag.sh"
log "  $(date '+%Y-%m-%d %H:%M:%S')"
log "  mode     = ${MODE}"
log "  company  = ${COMPANY}"
log "  project  = ${PROJECT}"
log "  app_code = ${APP_CODE}"
log "  dag_name = ${DAG_NAME}"
log "  env      = ${ENV}"
log "  output   = ${OUTPUT_DIR}"
log "========================================"
log ""

OVERALL_OK=true

# ─── Scan mode — parse XMLs and write JSON catalogue ─────────────────────────
if [ "${MODE}" = "scan" ]; then
    INPUT_DIR="${SCRIPT_DIR}/input/test_nonprod"
    CONN_REF="${SCRIPT_DIR}/skills/controlm2airflow/raws/connection_id_nonprod.md"
    NODE_WIN_REF="${SCRIPT_DIR}/skills/controlm2airflow/raws/node_id_win.md"
    OUT_JSON="${SCRIPT_DIR}/output/reusable/test_nonprod_catalogue.json"

    mkdir -p "${SCRIPT_DIR}/output/reusable"

    sep
    step "Scanning input/test_nonprod/*.xml → JSON catalogue"
    info "Input  : ${INPUT_DIR}/*.xml"
    info "Output : ${OUT_JSON}"
    sep

    if [ ! -d "${INPUT_DIR}" ]; then
        fail "Input directory not found: ${INPUT_DIR}"
        exit 1
    fi

    xml_files=("${INPUT_DIR}"/*.xml)
    if [ ${#xml_files[@]} -eq 0 ] || [ ! -f "${xml_files[0]}" ]; then
        fail "No XML files found in ${INPUT_DIR}/"
        exit 1
    fi

    info "Found ${#xml_files[@]} XML file(s)"

    python3 - "${INPUT_DIR}" "${CONN_REF}" "${OUT_JSON}" "${NODE_WIN_REF}" << 'PYEOF'
import sys, os, json, re
from xml.etree import ElementTree as ET

input_dir    = sys.argv[1]
conn_ref     = sys.argv[2]
out_json     = sys.argv[3]
node_win_ref = sys.argv[4] if len(sys.argv) > 4 else ""

# ── load node_id_win.md → set of Windows node IDs (case-insensitive) ────────
win_nodes = set()
if node_win_ref and os.path.exists(node_win_ref):
    for line in open(node_win_ref, encoding='utf-8'):
        m = re.match(r'\|\s*([^|]+?)\s*\|\s*Windows\s*\|', line, re.IGNORECASE)
        if m:
            win_nodes.add(m.group(1).strip().lower())

# ── parse connection_id_nonprod.md ──────────────────────────────────────────
conn_map  = {}   # job_name → {ssh_conn_id, dest_user, secret_name}
extra_map = {}   # job_name → {s3_endpoint_url, ...}
if os.path.exists(conn_ref):
    for line in open(conn_ref, encoding='utf-8'):
        # main table: | job | nodeid → host | ssh_conn_id | dest_user | secret |
        m = re.match(r'\|\s*(\S+)\s*\|[^|]*\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|', line)
        if m:
            conn_map[m.group(1)] = {
                "ssh_conn_id": m.group(2),
                "dest_user":   m.group(3),   # FTP/SFTP destination login (Remote User column)
                "secret_name": m.group(4),
            }
        # additional endpoints table: | job | Type | Value |
        e = re.match(r'\|\s*(\S+)\s*\|\s*S3 private endpoint\s*\|\s*`([^`]+)`\s*\|', line)
        if e:
            extra_map.setdefault(e.group(1), {})["s3_endpoint_url"] = e.group(2)

def ftp_var(variables, name, fallback_name=None):
    val = variables.get(name, "")
    if not val and fallback_name:
        val = variables.get(fallback_name, "")
    return val

catalogue = []

for fname in sorted(os.listdir(input_dir)):
    if not fname.endswith('.xml'):
        continue

    scenario = fname[:-4]
    fpath = os.path.join(input_dir, fname)
    tree = ET.parse(fpath)
    root = tree.getroot()

    for folder in root.iter('FOLDER'):
        folder_timefrom = folder.attrib.get('TIMEFROM', '')
        folder_name     = folder.attrib.get('FOLDER_NAME', '')

        for job in folder.iter('JOB'):
            appl_type = job.attrib.get('APPL_TYPE', '')
            if appl_type != 'FILE_TRANS':
                continue

            job_name    = job.attrib.get('JOBNAME', scenario)
            nodeid      = job.attrib.get('NODEID', '')
            run_as      = job.attrib.get('RUN_AS', '')
            timefrom    = job.attrib.get('TIMEFROM', folder_timefrom)
            cyclic      = job.attrib.get('CYCLIC', '0')
            interval    = job.attrib.get('INTERVAL', '')
            platform    = folder.attrib.get('PLATFORM', '')

            # collect all %%FTP-* variables
            variables = {}
            for var in job.iter('VARIABLE'):
                vname = var.attrib.get('NAME', '').lstrip('%')   # strip leading %%
                vval  = var.attrib.get('VALUE', '')
                variables[vname] = vval

            transfer_num = int(variables.get('FTP-TRANSFER_NUM', '1') or '1')

            conn = conn_map.get(scenario, {})

            # determine source OS — node_id_win.md is authoritative; fall back to PLATFORM attribute
            nodeid_lower = nodeid.lower()
            if nodeid_lower in win_nodes:
                source_os = 'Windows'
            elif platform.upper() == 'WINDOWS':
                source_os = 'Windows'
            else:
                source_os = 'Unix'

            # determine reusable DAG type
            dag_type = 'file-transfer-win' if source_os == 'Windows' else 'file-transfer-unix'

            # FTP-S3 bucket → destination type
            s3_bucket = variables.get('FTP-S3_BUCKET_NAME', '')

            slots = []
            for n in range(1, transfer_num + 1):
                lpath      = ftp_var(variables, f'FTP-LPATH{n}',      'FTP-LPATH')
                rpath      = ftp_var(variables, f'FTP-RPATH{n}',      'FTP-RPATH')
                rhost      = ftp_var(variables, f'FTP-RHOST{n}',      'FTP-RHOST')
                rport      = ftp_var(variables, f'FTP-RPORT{n}',      'FTP-RPORT')
                ruser      = ftp_var(variables, f'FTP-RUSER{n}',      'FTP-RUSER')
                conntype   = ftp_var(variables, f'FTP-CONNTYPE{n}',   'FTP-CONNTYPE')
                conntype2  = ftp_var(variables, f'FTP-CONNTYPE2{n}',  'FTP-CONNTYPE2')
                upload     = ftp_var(variables, f'FTP-UPLOAD{n}',     'FTP-UPLOAD')
                srcopt     = ftp_var(variables, f'FTP-SRCOPT{n}',     'FTP-SRCOPT')
                file_type  = ftp_var(variables, f'FTP-FILE_TYPE{n}',  'FTP-FILE_TYPE')
                precomm    = ftp_var(variables, f'FTP-PRECOMM{n}',    f'FTP-PRECOMM1{n}')
                postcomm   = ftp_var(variables, f'FTP-POSTCOMM{n}',   'FTP-POSTCOMM')
                # %%FTP-PREPARAM1{p}{n}: HOST=1 (source), p=param index, n=slot
                precomm_args = []
                for p in range(1, 10):
                    v = variables.get(f'FTP-PREPARAM1{p}{n}', '')
                    if v:
                        precomm_args.append(v)
                    else:
                        break
                postcomm_args = []
                for p in range(1, 10):
                    v = variables.get(f'FTP-POSTPARAM{p}{n}', '') or variables.get(f'FTP-POSTPARAM1{p}{n}', '')
                    if v:
                        postcomm_args.append(v)
                    else:
                        break
                rostype    = ftp_var(variables, f'FTP-ROSTYPE{n}',    'FTP-ROSTYPE')
                recfm      = ftp_var(variables, f'FTP-RECFM{n}',      'FTP-RECFM')
                lrecl      = ftp_var(variables, f'FTP-LRECL{n}',      'FTP-LRECL')
                dest_new   = ftp_var(variables, f'FTP-DEST_NEWNAME{n}', 'FTP-DEST_NEWNAME')

                # protocol mapping
                protocol = 'ftp'
                if conntype2 == 'FTP-SSL':
                    protocol = 'ftps'
                elif conntype2 == 'SFTP':
                    protocol = 'sftp'
                elif s3_bucket:
                    protocol = 's3'

                transfer_dir = 'upload' if upload == '1' else 'download'

                srcopt_meaning = {
                    '0': 'keep source',
                    '1': 'keep source',
                    '2': 'rename source',
                    '3': 'move source',
                    '4': 'delete source',
                }.get(srcopt, srcopt)

                slot = {
                    "slot": n,
                    "transfer_direction": transfer_dir,
                    "protocol":           protocol,
                    "lpath":              lpath,
                    "rpath":              rpath,
                    "rhost":              rhost,
                    "rport":              rport or ("991" if protocol == "ftps" else "21"),
                    "ruser":              ruser or conn.get("dest_user", ""),
                    "file_type":          file_type or "I",
                    "rostype":            rostype,
                    "srcopt":             srcopt,
                    "srcopt_meaning":     srcopt_meaning,
                    "pre_command":        precomm,
                    "pre_command_args":   precomm_args,
                    "post_command":       postcomm,
                    "post_command_args":  postcomm_args,
                    "dest_newname":       dest_new,
                }
                if recfm:
                    slot["recfm"] = recfm
                if lrecl:
                    slot["lrecl"] = lrecl
                if s3_bucket:
                    slot["s3_bucket"] = s3_bucket
                slots.append(slot)

            extra = extra_map.get(scenario, {})
            entry = {
                "scenario":        scenario,
                "xml_file":        fname,
                "folder_name":     folder_name,
                "job_name":        job_name,
                "nodeid":          nodeid,
                "run_as":          run_as,
                "source_os":       source_os,
                "reusable_dag":    dag_type,
                "timefrom":        timefrom,
                "cyclic":          cyclic == '1',
                "interval":        interval,
                "transfer_num":    transfer_num,
                "ssh_conn_id":     conn.get("ssh_conn_id", ""),
                "dest_user":       conn.get("dest_user", ""),   # from connection_id_nonprod.md Remote User column
                "secret_name":     conn.get("secret_name", ""),
                "s3_endpoint_url": extra.get("s3_endpoint_url", ""),  # from ## Additional endpoints table
                "transfers":       slots,
            }
            catalogue.append(entry)

with open(out_json, 'w', encoding='utf-8') as f:
    json.dump(catalogue, f, indent=2, ensure_ascii=False)

print(f"[OK] Written {len(catalogue)} scenario(s) → {out_json}")

# ── generate Airflow trigger conf JSON ──────────────────────────────────────
# One entry per transfer slot — ready to paste into Airflow "Trigger DAG w/ config"
trigger_conf = []

for entry in catalogue:
    dag_type = entry["reusable_dag"]   # file-transfer-unix | file-transfer-win
    is_win   = dag_type == "file-transfer-win"

    for slot in entry["transfers"]:
        # map catalogue fields → DAG params keys
        # Unix:    ssh_conn_id, source_path, dest_host, dest_port, dest_protocol,
        #          dest_user, dest_path, password_var_name, pre_command, post_command, direction
        # Windows: psrp_conn_id (instead of ssh_conn_id), same rest

        raw_dir  = slot["transfer_direction"]   # "upload" | "download"
        srcopt   = slot["srcopt"]
        protocol = slot["protocol"]

        if protocol == "s3":
            direction   = "s3_upload" if raw_dir == "upload" else "s3_download"
            source_path = slot["lpath"]
            s3_prefix   = slot["rpath"].lstrip("/")
            conf = {
                "source_path":     source_path,
                "dest_protocol":   "s3",
                "s3_bucket":       slot.get("s3_bucket", ""),
                "s3_prefix":       s3_prefix,
                "s3_region":       "ap-southeast-1",
                "s3_endpoint_url": entry.get("s3_endpoint_url", ""),
                "aws_profile":     "",
                "pre_command":      slot["pre_command"],
                "pre_command_args": slot["pre_command_args"],
                "post_command":     slot["post_command"],
                "post_command_args":slot["post_command_args"],
                "direction":        direction,
            }
        else:
            srcopt_dir_map = {
                ("upload",   "1"): "upload",
                ("upload",   "2"): "upload_rename",
                ("upload",   "3"): "upload_move",
                ("upload",   "4"): "upload_delete",
                ("download", "1"): "download",
                ("download", "2"): "download_rename",
                ("download", "3"): "download_move",
                ("download", "4"): "download_delete",
            }
            direction   = srcopt_dir_map.get((raw_dir, srcopt), raw_dir)
            source_path = slot["lpath"] if raw_dir == "upload" else slot["rpath"]
            dest_path   = slot["rpath"] if raw_dir == "upload" else slot["lpath"]
            # dest_user: connection_id_nonprod.md is authoritative; fall back to ruser from XML
            dest_user   = entry.get("dest_user") or slot["ruser"]
            conf = {
                "source_path":       source_path,
                "dest_host":         slot["rhost"],
                "dest_port":         slot["rport"],
                "dest_protocol":     protocol,
                "dest_user":         dest_user,
                "dest_path":         dest_path,
                "password_var_name": entry["secret_name"],
                "pre_command":       slot["pre_command"],
                "pre_command_args":  slot["pre_command_args"],
                "post_command":      slot["post_command"],
                "post_command_args": slot["post_command_args"],
                "direction":         direction,
            }

        if is_win:
            conf["psrp_conn_id"] = entry["ssh_conn_id"]
        else:
            conf["ssh_conn_id"] = entry["ssh_conn_id"]

        label = f"{entry['scenario']}_slot{slot['slot']}"
        if entry["transfer_num"] == 1:
            label = entry["scenario"]

        trigger_conf.append({
            "_label":       label,
            "_scenario":    entry["scenario"],
            "_slot":        slot["slot"],
            "_dag":         dag_type,
            "_direction":   direction,
            "_notes":       (
                f"SRCOPT={slot['srcopt']} ({slot['srcopt_meaning']})"
                + (f" | rostype={slot['rostype']}" if slot.get("rostype") else "")
                + (f" | recfm={slot['recfm']} lrecl={slot['lrecl']}" if slot.get("recfm") else "")
                + (f" | s3_bucket={slot['s3_bucket']}" if slot.get("s3_bucket") else "")
                + (f" | dest_newname={slot['dest_newname']}" if slot.get("dest_newname") else "")
            ),
            "conf": conf,
        })

out_trigger = out_json.replace("catalogue.json", "trigger_conf.json")
with open(out_trigger, 'w', encoding='utf-8') as f:
    json.dump(trigger_conf, f, indent=2, ensure_ascii=False)

print(f"[OK] Written {len(trigger_conf)} trigger conf(s) → {out_trigger}")
PYEOF

    STATUS=$?
    log ""
    OUT_TRIGGER="${SCRIPT_DIR}/output/reusable/test_nonprod_trigger_conf.json"
    if [ ${STATUS} -eq 0 ]; then
        pass "Scan complete"
        info "Catalogue  : ${OUT_JSON}"
        info "Trigger conf: ${OUT_TRIGGER}"
        info "$(python3 -c "
import json
d = json.load(open('${OUT_JSON}'))
t = json.load(open('${OUT_TRIGGER}'))
print(f'{len(d)} scenarios, {len(t)} trigger conf(s): ' + ', '.join(s[\"scenario\"] for s in d))
" 2>/dev/null || true)"
    else
        fail "Scan failed — see log above"
        exit 1
    fi
    sep
    exit 0
fi
# ─── end scan ─────────────────────────────────────────────────────────────────

# ─── Stamp-and-write helper ───────────────────────────────────────────────────
# stamp_template <template_file> <output_file> <dag_suffix>
# dag_suffix: "unix" | "win"  — used only in logging
stamp_template() {
    local tmpl="$1"
    local out_file="$2"
    local dag_type="$3"

    # Determine DAG name suffix for each type
    local dag_name_val
    if [ "${dag_type}" = "unix" ]; then
        dag_name_val="file-transfer-unix-${DAG_NAME}"
    else
        dag_name_val="file-transfer-win-${DAG_NAME}"
    fi

    sep
    step "Generating ${dag_type} DAG"
    info "Template : ${tmpl}"
    info "Output   : ${out_file}"
    info "DAG ID   : ${COMPANY}-${PROJECT}-${APP_CODE}-${dag_name_val}-${ENV}"
    sep

    if [ ! -f "${tmpl}" ]; then
        fail "Template not found: ${tmpl}"
        OVERALL_OK=false
        return
    fi

    debug "Stamping placeholders:"
    debug "  ##COMPANY##                           → ${COMPANY}"
    debug "  ##PROJECT##                           → ${PROJECT}"
    debug "  ##ENV##                               → ${ENV}"
    debug "  ##DAG_NAME##                          → ${dag_name_val}"
    debug "  ##ACTIVE##                            → ${ACTIVE_PY}"
    debug "  ##TAGS##                              → ${TAGS_PY}"
    debug "  ##EMAIL_LIST##                        → ${EMAIL_LIST_PY}"
    debug "  ##ENABLE_EMAIL_NOTIFICATION_SUCCESS## → ${EMAIL_SUCCESS_PY}"
    debug "  ##ENABLE_EMAIL_NOTIFICATION_FAIL##    → ${EMAIL_FAIL_PY}"

    sed \
        -e "s/##COMPANY##/${COMPANY}/g" \
        -e "s/##PROJECT##/${PROJECT}/g" \
        -e "s/##APP_CODE##/${APP_CODE}/g" \
        -e "s/##ENV##/${ENV}/g" \
        -e "s/##DAG_NAME##/${dag_name_val}/g" \
        -e "s/##ACTIVE##/${ACTIVE_PY}/g" \
        -e "s|##TAGS##|${TAGS_PY}|g" \
        -e "s|##EMAIL_LIST##|${EMAIL_LIST_PY}|g" \
        -e "s/##ENABLE_EMAIL_NOTIFICATION_SUCCESS##/${EMAIL_SUCCESS_PY}/g" \
        -e "s/##ENABLE_EMAIL_NOTIFICATION_FAIL##/${EMAIL_FAIL_PY}/g" \
        "${tmpl}" > "${out_file}"

    local line_count
    line_count=$(wc -l < "${out_file}")
    info "Written  : ${out_file} (${line_count} lines)"

    # ── syntax check ──────────────────────────────────────────────────────────
    local py_bin="${SCRIPT_DIR}/.venv/bin/python"
    local flakes_bin="${SCRIPT_DIR}/.venv/bin/pyflakes"

    if [ ! -x "${py_bin}" ]; then
        py_bin="$(command -v python3 2>/dev/null || true)"
    fi
    if [ ! -x "${flakes_bin}" ]; then
        flakes_bin="$(command -v pyflakes 2>/dev/null || true)"
    fi

    local syntax_ok=true

    if [ -n "${py_bin}" ] && [ -x "${py_bin}" ]; then
        info "Syntax check: ${py_bin} ${out_file}"
        if "${py_bin}" "${out_file}" >> "${SUMMARY_LOG}" 2>&1; then
            pass "Syntax OK  : $(basename "${out_file}")"
        else
            fail "Syntax FAIL: $(basename "${out_file}")"
            syntax_ok=false
            OVERALL_OK=false
        fi
    else
        info "Skipping syntax check — python3 not found"
    fi

    if [ -n "${flakes_bin}" ] && [ -x "${flakes_bin}" ]; then
        info "Lint check : ${flakes_bin} ${out_file}"
        if "${flakes_bin}" "${out_file}" >> "${SUMMARY_LOG}" 2>&1; then
            pass "Lint OK    : $(basename "${out_file}")"
        else
            fail "Lint FAIL  : $(basename "${out_file}")"
            syntax_ok=false
            OVERALL_OK=false
        fi
    else
        info "Skipping lint check — pyflakes not found"
    fi

    # ── shellcheck: extract bash heredocs and lint each one ───────────────────
    local sc_bin
    sc_bin="$(command -v shellcheck 2>/dev/null || true)"
    if [ -n "${sc_bin}" ]; then
        local sc_tmp_dir
        sc_tmp_dir="$(mktemp -d /tmp/sc_XXXXXX)"
        # Extract each bash -s << 'BASH' ... BASH block from the .py file into
        # separate .sh files, then run shellcheck on each.
        python3 - "${out_file}" "${sc_tmp_dir}" << 'SCPY'
import sys, re, os

src = open(sys.argv[1], encoding='utf-8').read()
out_dir = sys.argv[2]

# Match bash -s << 'BASH'\n...\nBASH (non-greedy, across lines)
pattern = re.compile(r"bash -s << 'BASH'\n(.*?)\nBASH\n", re.DOTALL)

# Also capture task_id of the enclosing SSHOperator to name the file
task_pattern = re.compile(r"task_id='([^']+)'")

for i, m in enumerate(pattern.finditer(src)):
    script_body = m.group(1)
    # find nearest task_id before this match
    task_ids = task_pattern.findall(src[:m.start()])
    label = task_ids[-1] if task_ids else f"block_{i}"
    label = re.sub(r'[^a-zA-Z0-9_-]', '_', label)
    sh_path = os.path.join(out_dir, f"{i:02d}_{label}.sh")
    with open(sh_path, 'w', encoding='utf-8') as f:
        f.write('#!/usr/bin/env bash\n')
        f.write(script_body + '\n')
    print(sh_path)
SCPY

        local sc_ok=true
        local sh_files=("${sc_tmp_dir}"/*.sh)
        if [ ${#sh_files[@]} -eq 0 ] || [ ! -f "${sh_files[0]}" ]; then
            info "ShellCheck : no bash heredocs found to check"
        else
            info "ShellCheck : ${#sh_files[@]} bash block(s) to check"
            for sh in "${sh_files[@]}"; do
                local block_name
                block_name="$(basename "${sh}")"
                # SC2086: word splitting on unquoted glob vars is intentional (e.g. ls $SRC_PATH)
                # SC2012: ls preferred over find for human-readable output — filenames are controlled
                # SC2010: ls | grep for display only — not used for logic
                # SC2154: Jinja {{ params.x }} renders look like unset bash vars to shellcheck
                if "${sc_bin}" --shell=bash \
                    --exclude=SC2086,SC2012,SC2010,SC2154 \
                    "${sh}" >> "${SUMMARY_LOG}" 2>&1; then
                    pass "ShellCheck OK  : ${block_name}"
                else
                    fail "ShellCheck FAIL: ${block_name}"
                    sc_ok=false
                    syntax_ok=false
                    OVERALL_OK=false
                fi
            done
        fi
        rm -rf "${sc_tmp_dir}"
        if $sc_ok; then
            pass "ShellCheck OK  : $(basename "${out_file}")"
        fi
    else
        info "Skipping ShellCheck — shellcheck not found (brew install shellcheck)"
    fi

    if $syntax_ok; then
        pass "${dag_type} DAG PASSED — $(basename "${out_file}")"
    else
        fail "${dag_type} DAG FAILED — see ${SUMMARY_LOG}"
    fi
    log ""
}

# ─── Generate Unix DAG ────────────────────────────────────────────────────────
if [ "${MODE}" = "unix" ] || [ "${MODE}" = "both" ]; then
    UNIX_OUT="${OUTPUT_DIR}/${COMPANY}-${PROJECT}-${APP_CODE}-file-transfer-unix-${DAG_NAME}-${ENV}.py"
    stamp_template "${UNIX_TEMPLATE}" "${UNIX_OUT}" "unix"
fi

# ─── Generate Windows DAG ─────────────────────────────────────────────────────
if [ "${MODE}" = "win" ] || [ "${MODE}" = "both" ]; then
    WIN_OUT="${OUTPUT_DIR}/${COMPANY}-${PROJECT}-${APP_CODE}-file-transfer-win-${DAG_NAME}-${ENV}.py"
    stamp_template "${WIN_TEMPLATE}" "${WIN_OUT}" "win"
fi

# ─── Summary ──────────────────────────────────────────────────────────────────
sep
log "  SUMMARY"
log ""

if [ "${MODE}" = "unix" ] || [ "${MODE}" = "both" ]; then
    log "  Unix DAG  : ${UNIX_OUT}"
fi
if [ "${MODE}" = "win" ] || [ "${MODE}" = "both" ]; then
    log "  Win DAG   : ${WIN_OUT}"
fi

log ""
log "  Output dir: ${OUTPUT_DIR}/"
log "  Log       : ${SUMMARY_LOG}"
log ""

if $OVERALL_OK; then
    pass "All DAGs generated successfully"
    sep
    exit 0
else
    fail "One or more DAGs failed — check log above"
    sep
    exit 1
fi
