#!/usr/bin/env bash
# run_tests.sh — run controlm2airflow skill on all input/*.xml files
# Usage:
#   ./run_tests.sh                        # run all input/*.xml
#   ./run_tests.sh input/test_case1.xml   # run specific file(s)
#
# Output: output/<basename_without_ext>/  per input file (preserved across runs)
# Logs:   logs/run_tests_<timestamp>.log  + logs/<basename>_<timestamp>.log per case

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/input"
OUTPUT_BASE="${SCRIPT_DIR}/output"
LOG_DIR="${SCRIPT_DIR}/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SUMMARY_LOG="${LOG_DIR}/run_tests_${TIMESTAMP}.log"

mkdir -p "${OUTPUT_BASE}" "${LOG_DIR}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "$*" | tee -a "${SUMMARY_LOG}"; }
pass() { log "${GREEN}[PASS]${NC} $*"; }
fail() { log "${RED}[FAIL]${NC} $*"; }
info() { log "${YELLOW}[INFO]${NC} $*"; }

# build list of input files to process
if [ $# -gt 0 ]; then
    INPUT_FILES=("$@")
else
    INPUT_FILES=("${INPUT_DIR}"/*.xml)
fi

if [ ${#INPUT_FILES[@]} -eq 0 ] || [ ! -f "${INPUT_FILES[0]}" ]; then
    echo "No input XML files found in ${INPUT_DIR}/"
    exit 1
fi

# default conversion params — override per file by naming convention if needed
COMPANY="scb"
APP_CODE="app"
ENV="dev"

# per-file param overrides keyed by filename prefix
declare -A APP_IDS=(
    ["test_case1"]="AP1001"
    ["test_case2"]="AP1002"
    ["test_case3"]="AP1003"
    ["test_case4"]="AP1004"
    ["test_case5"]="AP1005"
    ["test_case6"]="AP1006"
)
declare -A APP_CODES=(
    ["test_case1"]="erp"
    ["test_case2"]="expinv"
    ["test_case3"]="als"
    ["test_case4"]="edw"
    ["test_case5"]="clr"
    ["test_case6"]="nss"
)

TOTAL=0
PASSED=0
FAILED=0
FAILED_CASES=()

log "========================================"
log "  controlm2airflow — test suite"
log "  $(date '+%Y-%m-%d %H:%M:%S')"
log "  inputs: ${#INPUT_FILES[@]} file(s)"
log "========================================"

IDX=0
for input_path in "${INPUT_FILES[@]}"; do
    IDX=$((IDX + 1))
    input_file="$(basename "${input_path}")"
    base="${input_file%.xml}"
    CASE_LOG="${LOG_DIR}/${base}_${TIMESTAMP}.log"
    CASE_OUTPUT="${OUTPUT_BASE}/${base}"

    # resolve per-file params — match longest prefix key
    app_id="AP$(printf '%04d' "${IDX}")"
    app_code="${APP_CODE}"
    for key in "${!APP_IDS[@]}"; do
        if [[ "${base}" == "${key}"* ]]; then
            app_id="${APP_IDS[${key}]}"
            app_code="${APP_CODES[${key}]}"
            break
        fi
    done

    TOTAL=$((TOTAL + 1))

    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    info "Case ${IDX}: ${base}"
    info "Input  : ${input_file}"
    info "Output : output/${base}/"
    info "Params : company=${COMPANY} app_id=${app_id} app_code=${app_code} env=${ENV}"
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    if [ ! -f "${input_path}" ]; then
        fail "Case ${IDX} — input file not found: ${input_path}"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${IDX}: ${base} — input file missing")
        continue
    fi

    # each case gets its own output subdirectory (preserved — not wiped)
    mkdir -p "${CASE_OUTPUT}"

    # run skill via claude
    info "Running claude skill..."
    claude --print --allowedTools "Read,Write,Bash" 2>&1 | tee "${CASE_LOG}" << EOF
Follow the skill defined in skills/controlm2airflow/skill.md to convert Control-M jobs to Airflow DAGs.

Input XML: ${input_path}
Output directory: ${CASE_OUTPUT}/

Parameters:
company  = ${COMPANY}
app_id   = ${app_id}
app_code = ${app_code}
env      = ${ENV}
EOF

    # check output was generated
    dag_files=("${CASE_OUTPUT}"/*.py)
    if [ ${#dag_files[@]} -eq 0 ] || [ ! -f "${dag_files[0]}" ]; then
        fail "Case ${IDX} — no .py output generated in ${CASE_OUTPUT}/"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${IDX}: ${base} — no DAG output")
        continue
    fi

    info "Generated ${#dag_files[@]} DAG file(s):"
    for dag in "${dag_files[@]}"; do
        info "  $(basename "${dag}") ($(wc -l < "${dag}") lines)"
    done

    # syntax-check each generated DAG
    syntax_ok=true
    for dag in "${dag_files[@]}"; do
        info "Verifying: $(basename "${dag}")"
        if .venv/bin/python "${dag}" >> "${CASE_LOG}" 2>&1; then
            pass "Syntax OK : $(basename "${dag}")"
        else
            fail "Syntax FAIL: $(basename "${dag}")"
            syntax_ok=false
        fi
    done

    if $syntax_ok; then
        pass "Case ${IDX} PASSED — ${base}"
        PASSED=$((PASSED + 1))
    else
        fail "Case ${IDX} FAILED — ${base} (see ${CASE_LOG})"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${IDX}: ${base} — DAG syntax error")
    fi
done

# summary
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
log "  Output : ${OUTPUT_BASE}/<case-name>/"
log "  Log    : ${SUMMARY_LOG}"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

[ "${FAILED}" -eq 0 ]
