#!/usr/bin/env bash
# run_tests_nonprod.sh — run controlm2airflow skill on input/test_nonprod/*.xml
#                        and optionally generate reusable file-transfer DAGs from parameters
#
# Usage:
#   ./run_tests_nonprod.sh                        # run all input/test_nonprod/*.xml
#   ./run_tests_nonprod.sh input/test_nonprod/dest_rename.xml  # specific file(s)
#   ./run_tests_nonprod.sh --scenario chmod        # files matching keyword
#   ./run_tests_nonprod.sh --scenario pre post     # files matching any keyword
#   ./run_tests_nonprod.sh --list                  # list available scenarios
#
#   # Generate a reusable file-transfer DAG (no XML — pass params directly):
#   ./run_tests_nonprod.sh --reusable \
#       dag_name=transfer_daily \
#       ssh_conn_id=ssh-benseni-ctrlm \
#       protocol=ftps \
#       rhost=10.4.53.2 \
#       rport=991 \
#       ruser=airflow \
#       rpass_var=airflow-alld-secret \
#       lpath=/SERVERDATA/YUT_AIRFLOW/input \
#       rpath=MYDS.INPUT \
#       transfer_type=upload \
#       file_type=A \
#       rostype=MVS \
#       schedule="30 22 * * *"
#
# Reusable DAG parameters (--reusable key=value ...):
#   dag_name       DAG name suffix (required)
#   ssh_conn_id    Airflow SSH connection ID (required)
#   protocol       ftp / ftps / sftp (required)
#   rhost          Remote hostname or IP (required)
#   rport          Remote port (required)
#   ruser          Remote FTP username (required)
#   rpass_var      Airflow Variable name holding the password (required)
#   lpath          Local file path or glob (required)
#   rpath          Remote path or MVS dataset name (required)
#   transfer_type  upload / download (default: upload)
#   file_type      A (ASCII) / I (binary) (default: I)
#   rostype        Unix / Windows / MVS (default: Unix)
#   schedule       Cron expression (default: "0 22 * * *")
#   recfm          MVS RECFM (e.g. FB) — MVS only
#   lrecl          MVS LRECL (e.g. 400) — MVS only
#
# Scenario keywords match against the input filename (case-insensitive substring).
# Examples: dest, source, chmod, makedir, rename, move
#
# Fixed parameters for this suite:
#   company  = nix
#   project  = apxxxx
#   app_code = poc
#   env      = nonprod
#
# Output: output/test_nonprod/   (shared dir — DAGs named by folder+TIMEFROM group)
# Logs:   logs/run_tests_nonprod_<timestamp>.log
#         logs/<basename>_<timestamp>.log  per case
#
# Test cases:
#   dest_rename   TIMEFROM=1630  FILE_TRANS FTP-SSL upload, pre-rename, delete-source, DEST_NEWNAME
#   post_chmod    (none)         FILE_TRANS 5 downloads + POSTCOMM chmod, CYCLIC=1
#   pre_chmod     (none)         FILE_TRANS 5 uploads + PRECOMM chmod, S3 bucket
#   pre_makedir   (none)         FILE_TRANS Windows, PRECOMM mkdir on remote, 4 uploads, OR INCOND
#   source_move   TIMEFROM=2230  FILE_TRANS FTP-SSL upload to MVS, SRCOPT=3 (move source)
#   source_rename (none)         FILE_TRANS FTP-SSL download, SRCOPT=2 (rename source on remote)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/input/test_nonprod"
OUTPUT_DIR="${SCRIPT_DIR}/output/test_nonprod"
LOG_DIR="${SCRIPT_DIR}/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SUMMARY_LOG="${LOG_DIR}/run_tests_nonprod_${TIMESTAMP}.log"

mkdir -p "${OUTPUT_DIR}" "${LOG_DIR}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "$*" | tee -a "${SUMMARY_LOG}"; }
pass() { log "${GREEN}[PASS]${NC} $*"; }
fail() { log "${RED}[FAIL]${NC} $*"; }
info() { log "${YELLOW}[INFO]${NC} $*"; }

# fixed conversion params for this suite
COMPANY="nix"
PROJECT="apxxxx"
APP_CODE="poc"
ENV="nonprod"

usage() {
    sed -n '2,62p' "$0" | sed 's/^# \{0,1\}//'
    echo ""
    echo "Available scenarios (input/test_nonprod/*.xml):"
    for f in "${INPUT_DIR}"/*.xml; do
        [ -f "${f}" ] && echo "  $(basename "${f}" .xml)"
    done
}

# parse arguments
SCENARIOS=()
INPUT_FILES=()
REUSABLE=false
REUSABLE_PARAMS=()

while [ $# -gt 0 ]; do
    case "$1" in
        --list|-l)
            usage; exit 0 ;;
        --scenario|-s)
            shift
            while [ $# -gt 0 ] && [[ "$1" != --* ]]; do
                SCENARIOS+=("$1"); shift
            done ;;
        --reusable|-r)
            REUSABLE=true; shift
            while [ $# -gt 0 ] && [[ "$1" != --* ]]; do
                REUSABLE_PARAMS+=("$1"); shift
            done ;;
        --help|-h)
            usage; exit 0 ;;
        *.xml)
            INPUT_FILES+=("$1"); shift ;;
        *)
            SCENARIOS+=("$1"); shift ;;
    esac
done

# ─── Reusable DAG generation (no XML input) ──────────────────────────────────
if $REUSABLE; then
    CASE_LABEL="reusable_dag"
    CASE_LOG="${LOG_DIR}/${CASE_LABEL}_${TIMESTAMP}.log"
    STREAM_LOG="${LOG_DIR}/${CASE_LABEL}_${TIMESTAMP}_stream.jsonl"

    # parse key=value params (bash 3 compatible — no associative arrays)
    P_dag_name=""; P_ssh_conn_id=""; P_protocol=""; P_rhost=""; P_rport=""
    P_ruser=""; P_rpass_var=""; P_lpath=""; P_rpath=""
    P_transfer_type=""; P_file_type=""; P_rostype=""; P_schedule=""
    P_recfm=""; P_lrecl=""

    for kv in "${REUSABLE_PARAMS[@]+"${REUSABLE_PARAMS[@]}"}"; do
        key="${kv%%=*}"; val="${kv#*=}"
        case "${key}" in
            dag_name)      P_dag_name="${val}" ;;
            ssh_conn_id)   P_ssh_conn_id="${val}" ;;
            protocol)      P_protocol="${val}" ;;
            rhost)         P_rhost="${val}" ;;
            rport)         P_rport="${val}" ;;
            ruser)         P_ruser="${val}" ;;
            rpass_var)     P_rpass_var="${val}" ;;
            lpath)         P_lpath="${val}" ;;
            rpath)         P_rpath="${val}" ;;
            transfer_type) P_transfer_type="${val}" ;;
            file_type)     P_file_type="${val}" ;;
            rostype)       P_rostype="${val}" ;;
            schedule)      P_schedule="${val}" ;;
            recfm)         P_recfm="${val}" ;;
            lrecl)         P_lrecl="${val}" ;;
            *) echo "[WARN] Unknown parameter: ${key}=${val}" ;;
        esac
    done

    # required param check
    for req in dag_name ssh_conn_id protocol rhost rport ruser rpass_var lpath rpath; do
        eval "val=\$P_${req}"
        if [ -z "${val}" ]; then
            echo "[ERROR] --reusable missing required parameter: ${req}"
            echo "Run './run_tests_nonprod.sh --help' for usage."
            exit 1
        fi
    done

    # defaults
    TRANSFER_TYPE="${P_transfer_type:-upload}"
    FILE_TYPE="${P_file_type:-I}"
    ROSTYPE="${P_rostype:-Unix}"
    SCHEDULE="${P_schedule:-0 22 * * *}"
    RECFM="${P_recfm:-}"
    LRECL="${P_lrecl:-}"
    DAG_NAME="${P_dag_name}"

    log "========================================"
    log "  controlm2airflow — reusable DAG generation"
    log "  $(date '+%Y-%m-%d %H:%M:%S')"
    log "  company=${COMPANY} project=${PROJECT} app_code=${APP_CODE} env=${ENV}"
    log "  dag_name=${DAG_NAME}"
    log "========================================"
    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    info "Generating reusable file-transfer DAG: ${DAG_NAME}"
    info "Output : output/test_nonprod/"
    info "Params : protocol=${P_protocol} rhost=${P_rhost}:${P_rport} transfer=${TRANSFER_TYPE} type=${FILE_TYPE} rostype=${ROSTYPE}"
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    info "Running claude skill..."

    MVS_PARAMS=""
    if [ -n "${RECFM}" ]; then
        MVS_PARAMS="recfm          = ${RECFM}"$'\n'"lrecl          = ${LRECL}"
    fi

    BEFORE_TS=$(date +%s)

    {
        printf 'Follow the skill defined in skills/controlm2airflow/skill.md.\n\n'
        printf 'Generate a reusable file-transfer Airflow DAG — NOT from a Control-M XML, but from the parameters below.\n'
        printf 'This is a standalone parameterised DAG that performs a single file transfer on a schedule.\n'
        printf 'Use SSHOperator with lftp (or aws s3 for S3 protocol). Apply all skill.md rules:\n'
        printf '  - professional logging (INFO/DEBUG), file condition checks, lftp debug lines\n'
        printf '  - FTPS explicit TLS settings if protocol=ftps\n'
        printf '  - MVS dataset quoting (RPATH_QUOTED) if rostype=MVS\n'
        printf '  - success_callback / failure_callback canonical template\n'
        printf '  - all variables as module-level _ prefixed vars\n\n'
        printf 'Output directory: %s/\n\n' "${OUTPUT_DIR}"
        printf 'Parameters:\n'
        printf 'company        = %s\n' "${COMPANY}"
        printf 'app_id         = %s\n' "${PROJECT}"
        printf 'app_code       = %s\n' "${APP_CODE}"
        printf 'env            = %s\n' "${ENV}"
        printf 'dag_name       = %s\n' "${DAG_NAME}"
        printf 'ssh_conn_id    = %s\n' "${P_ssh_conn_id}"
        printf 'protocol       = %s\n' "${P_protocol}"
        printf 'rhost          = %s\n' "${P_rhost}"
        printf 'rport          = %s\n' "${P_rport}"
        printf 'ruser          = %s\n' "${P_ruser}"
        printf 'rpass_var      = %s\n' "${P_rpass_var}"
        printf 'lpath          = %s\n' "${P_lpath}"
        printf 'rpath          = %s\n' "${P_rpath}"
        printf 'transfer_type  = %s\n' "${TRANSFER_TYPE}"
        printf 'file_type      = %s\n' "${FILE_TYPE}"
        printf 'rostype        = %s\n' "${ROSTYPE}"
        printf 'schedule       = %s\n' "${SCHEDULE}"
        if [ -n "${MVS_PARAMS}" ]; then printf '%s\n' "${MVS_PARAMS}"; fi
        printf '\n'
        printf 'DAG filename: %s-%s-%s-%s-%s.py\n' "${COMPANY}" "${PROJECT}" "${APP_CODE}" "${DAG_NAME}" "${ENV}"
        printf '\n'
        printf 'Connection IDs, remote users, and secrets for this environment are defined in:\n'
        printf '  skills/controlm2airflow/raws/connection_id_nonprod.md\n'
        printf 'Verify every generated DAG with: python <dag>.py && pyflakes <dag>.py (both must exit 0).\n'
    } | claude --print --output-format stream-json --verbose --allowedTools "Read,Write,Bash" \
        2>&1 | tee "${STREAM_LOG}" \
        | python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line.startswith('{'):
        print(line, flush=True)
        continue
    try:
        obj = json.loads(line)
        if obj.get('type') == 'assistant' and 'message' in obj:
            for block in obj['message'].get('content', []):
                if block.get('type') == 'text':
                    print(block['text'], end='', flush=True)
    except json.JSONDecodeError:
        print(line, flush=True)
" 2>&1 | tee "${CASE_LOG}" || true

    AFTER_TS=$(date +%s)

    token_info="$(extract_tokens "${STREAM_LOG}" 2>/dev/null || echo "input=0 output=0 cache_read=0 cost=0.000000")"
    case_input=$(echo "${token_info}"  | sed -nE 's/.*input=([0-9]+).*/\1/p')
    case_output=$(echo "${token_info}" | sed -nE 's/.*output=([0-9]+).*/\1/p')
    case_cache=$(echo "${token_info}"  | sed -nE 's/.*cache_read=([0-9]+).*/\1/p')
    case_cost=$(echo "${token_info}"   | sed -nE 's/.*cost=([0-9.]+).*/\1/p')
    info "Tokens : input=${case_input:-0} output=${case_output:-0} cache_read=${case_cache:-0} cost=\$${case_cost:-0.000000}"

    # verify generated DAG
    dag_files=()
    while IFS= read -r -d '' f; do
        dag_files+=("$f")
    done < <(find "${OUTPUT_DIR}" -name "*.py" -newer "${LOG_DIR}/${CASE_LABEL}_${TIMESTAMP}.log" -print0 2>/dev/null)

    # fallback: any .py newer than start time
    if [ ${#dag_files[@]} -eq 0 ]; then
        while IFS= read -r -d '' f; do
            dag_files+=("$f")
        done < <(find "${OUTPUT_DIR}" -name "*.py" -newer "${SUMMARY_LOG}" -print0 2>/dev/null)
    fi

    if [ ${#dag_files[@]} -eq 0 ]; then
        fail "reusable — no .py output generated in ${OUTPUT_DIR}/"
        log ""
        log "  Output : ${OUTPUT_DIR}/"
        log "  Log    : ${SUMMARY_LOG}"
        exit 1
    fi

    info "Generated ${#dag_files[@]} DAG file(s):"
    syntax_ok=true
    for dag in "${dag_files[@]}"; do
        info "  $(basename "${dag}") ($(wc -l < "${dag}") lines)"
        if .venv/bin/python "${dag}" >> "${CASE_LOG}" 2>&1; then
            pass "Syntax OK : $(basename "${dag}")"
        else
            fail "Syntax FAIL: $(basename "${dag}")"
            syntax_ok=false
        fi
        if .venv/bin/pyflakes "${dag}" >> "${CASE_LOG}" 2>&1; then
            pass "Lint OK   : $(basename "${dag}")"
        else
            fail "Lint FAIL : $(basename "${dag}")"
            syntax_ok=false
        fi
    done

    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if $syntax_ok; then
        pass "Reusable DAG PASSED — ${DAG_NAME}"
        log ""
        log "  Output : ${OUTPUT_DIR}/"
        log "  Log    : ${CASE_LOG}"
        log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        exit 0
    else
        fail "Reusable DAG FAILED — ${DAG_NAME} (see ${CASE_LOG})"
        log ""
        log "  Output : ${OUTPUT_DIR}/"
        log "  Log    : ${CASE_LOG}"
        log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        exit 1
    fi
fi
# ─── end reusable ─────────────────────────────────────────────────────────────

# resolve final file list
if [ ${#INPUT_FILES[@]} -gt 0 ]; then
    : # explicit paths — use as-is
elif [ ${#SCENARIOS[@]} -gt 0 ]; then
    for f in "${INPUT_DIR}"/*.xml; do
        [ -f "${f}" ] || continue
        base="$(basename "${f}" .xml)"
        for kw in "${SCENARIOS[@]}"; do
            if echo "${base}" | grep -qi "${kw}"; then
                INPUT_FILES+=("${f}")
                break
            fi
        done
    done
    if [ ${#INPUT_FILES[@]} -eq 0 ]; then
        echo "No input files matched scenario(s): ${SCENARIOS[*]}"
        echo "Run './run_tests_nonprod.sh --list' to see available scenarios."
        exit 1
    fi
else
    INPUT_FILES=("${INPUT_DIR}"/*.xml)
fi

if [ ${#INPUT_FILES[@]} -eq 0 ] || [ ! -f "${INPUT_FILES[0]}" ]; then
    echo "No input XML files found in ${INPUT_DIR}/"
    exit 1
fi

TOTAL=0
PASSED=0
FAILED=0
FAILED_CASES=()

TOTAL_INPUT_TOKENS=0
TOTAL_OUTPUT_TOKENS=0
TOTAL_CACHE_READ_TOKENS=0
TOTAL_COST_USD=0

extract_tokens() {
    local log_file="$1"
    python3 - "$log_file" << 'PYEOF'
import sys, json

log_path = sys.argv[1]
model_usage = {}

with open(log_path, encoding='utf-8', errors='replace') as f:
    for line in f:
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            obj = json.loads(line)
            if 'modelUsage' in obj:
                model_usage = obj['modelUsage']
        except json.JSONDecodeError:
            pass

input_tok  = 0
output_tok = 0
cache_read = 0
cost_usd   = 0.0

for model, stats in model_usage.items():
    input_tok  += stats.get('inputTokens', 0)
    output_tok += stats.get('outputTokens', 0)
    cache_read += stats.get('cacheReadInputTokens', 0)
    cost_usd   += stats.get('costUSD', 0.0)

print(f"input={input_tok} output={output_tok} cache_read={cache_read} cost={cost_usd:.6f}")
PYEOF
}

log "========================================"
log "  controlm2airflow — nonprod test suite"
log "  $(date '+%Y-%m-%d %H:%M:%S')"
log "  company=${COMPANY} project=${PROJECT} app_code=${APP_CODE} env=${ENV}"
log "  inputs: ${#INPUT_FILES[@]} file(s)"
log "========================================"

IDX=0
for input_path in "${INPUT_FILES[@]}"; do
    IDX=$((IDX + 1))
    input_file="$(basename "${input_path}")"
    base="${input_file%.xml}"
    CASE_LOG="${LOG_DIR}/${base}_${TIMESTAMP}.log"
    STREAM_LOG="${LOG_DIR}/${base}_${TIMESTAMP}_stream.jsonl"

    TOTAL=$((TOTAL + 1))

    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    info "Case ${IDX}: ${base}"
    info "Input  : ${input_file}"
    info "Output : output/test_nonprod/"
    info "Params : company=${COMPANY} project=${PROJECT} app_code=${APP_CODE} env=${ENV}"
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    if [ ! -f "${input_path}" ]; then
        fail "Case ${IDX} — input file not found: ${input_path}"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${IDX}: ${base} — input file missing")
        continue
    fi

    info "Running claude skill..."
    {
        printf 'Follow the skill defined in skills/controlm2airflow/skill.md to convert Control-M jobs to Airflow DAGs.\n\n'
        printf 'Input XML: %s\n' "${input_path}"
        printf 'Output directory: %s/\n\n' "${OUTPUT_DIR}"
        printf 'Parameters:\n'
        printf 'company  = %s\n' "${COMPANY}"
        printf 'app_id   = %s\n' "${PROJECT}"
        printf 'app_code = %s\n' "${APP_CODE}"
        printf 'env      = %s\n' "${ENV}"
        printf 'mode     = inline\n\n'
        printf 'Split DAGs by TIMEFROM — one DAG per schedule group, appending _<HHMM> to the folder name.\n'
        printf 'Jobs with no TIMEFROM go into the _none group.\n\n'
        printf 'Connection IDs, remote users, and secrets for this environment are defined in:\n'
        printf '  skills/controlm2airflow/raws/connection_id_nonprod.md\n'
        printf 'Use the exact ssh_conn_id, user, and secret name from that file for each job — do not derive or guess them.\n\n'
        printf 'Verify every generated DAG with: python <dag>.py && pyflakes <dag>.py (both must exit 0).\n'
    } | claude --print --output-format stream-json --verbose --allowedTools "Read,Write,Bash" \
        2>&1 | tee "${STREAM_LOG}" \
        | python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line.startswith('{'):
        print(line, flush=True)
        continue
    try:
        obj = json.loads(line)
        if obj.get('type') == 'assistant' and 'message' in obj:
            for block in obj['message'].get('content', []):
                if block.get('type') == 'text':
                    print(block['text'], end='', flush=True)
    except json.JSONDecodeError:
        print(line, flush=True)
" 2>&1 | tee "${CASE_LOG}" || true

    # extract token usage
    token_info="$(extract_tokens "${STREAM_LOG}" 2>/dev/null || echo "input=0 output=0 cache_read=0 cost=0.000000")"
    case_input=$(echo "${token_info}"  | sed -nE 's/.*input=([0-9]+).*/\1/p')
    case_output=$(echo "${token_info}" | sed -nE 's/.*output=([0-9]+).*/\1/p')
    case_cache=$(echo "${token_info}"  | sed -nE 's/.*cache_read=([0-9]+).*/\1/p')
    case_cost=$(echo "${token_info}"   | sed -nE 's/.*cost=([0-9.]+).*/\1/p')

    TOTAL_INPUT_TOKENS=$((TOTAL_INPUT_TOKENS     + ${case_input:-0}))
    TOTAL_OUTPUT_TOKENS=$((TOTAL_OUTPUT_TOKENS   + ${case_output:-0}))
    TOTAL_CACHE_READ_TOKENS=$((TOTAL_CACHE_READ_TOKENS + ${case_cache:-0}))
    TOTAL_COST_USD=$(python3 -c "print(f'{float(\"${TOTAL_COST_USD}\") + float(\"${case_cost:-0}\"):.6f}')" 2>/dev/null || echo "${TOTAL_COST_USD}")

    info "Tokens : input=${case_input:-0} output=${case_output:-0} cache_read=${case_cache:-0} cost=\$${case_cost:-0.000000}"

    # verify all DAGs in the output dir that were touched by this run
    # (output dir is shared — check files modified in the last 5 minutes)
    dag_files=()
    while IFS= read -r -d '' f; do
        dag_files+=("$f")
    done < <(find "${OUTPUT_DIR}" -name "*.py" -newer "${input_path}" -print0 2>/dev/null)

    if [ ${#dag_files[@]} -eq 0 ]; then
        fail "Case ${IDX} — no .py output generated in ${OUTPUT_DIR}/"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${IDX}: ${base} — no DAG output")
        continue
    fi

    info "Generated/updated ${#dag_files[@]} DAG file(s):"
    for dag in "${dag_files[@]}"; do
        info "  $(basename "${dag}") ($(wc -l < "${dag}") lines)"
    done

    syntax_ok=true
    for dag in "${dag_files[@]}"; do
        info "Verifying: $(basename "${dag}")"
        if .venv/bin/python "${dag}" >> "${CASE_LOG}" 2>&1; then
            pass "Syntax OK : $(basename "${dag}")"
        else
            fail "Syntax FAIL: $(basename "${dag}")"
            syntax_ok=false
        fi
        if .venv/bin/pyflakes "${dag}" >> "${CASE_LOG}" 2>&1; then
            pass "Lint OK   : $(basename "${dag}")"
        else
            fail "Lint FAIL : $(basename "${dag}")"
            syntax_ok=false
        fi
    done

    if $syntax_ok; then
        pass "Case ${IDX} PASSED — ${base}"
        PASSED=$((PASSED + 1))
    else
        fail "Case ${IDX} FAILED — ${base} (see ${CASE_LOG})"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${IDX}: ${base} — DAG syntax/lint error")
    fi
done

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "  RESULTS: ${PASSED}/${TOTAL} passed"
if [ ${#FAILED_CASES[@]} -gt 0 ]; then
    log ""
    fail "Failed cases:"
    for fc in "${FAILED_CASES[@]}"; do
        log "  • ${fc}"
    done
fi
log ""
log "  Token usage (all cases):"
log "    Input tokens      : ${TOTAL_INPUT_TOKENS}"
log "    Output tokens     : ${TOTAL_OUTPUT_TOKENS}"
log "    Cache read tokens : ${TOTAL_CACHE_READ_TOKENS}"
log "    Total cost (USD)  : \$${TOTAL_COST_USD}"
log ""
log "  Output : ${OUTPUT_DIR}/"
log "  Log    : ${SUMMARY_LOG}"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

[ "${FAILED}" -eq 0 ]
