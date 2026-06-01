#!/usr/bin/env bash
# run_tests.sh — run all 6 test cases through the controlm2airflow skill
# Usage:
#   ./run_tests.sh              # run all cases
#   ./run_tests.sh 1            # run only case 1
#   ./run_tests.sh 1 3 5        # run specific cases

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/output"
LOG_DIR="${SCRIPT_DIR}/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SUMMARY_LOG="${LOG_DIR}/run_tests_${TIMESTAMP}.log"

mkdir -p "${OUTPUT_DIR}" "${LOG_DIR}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "$*" | tee -a "${SUMMARY_LOG}"; }
pass() { log "${GREEN}[PASS]${NC} $*"; }
fail() { log "${RED}[FAIL]${NC} $*"; }
info() { log "${YELLOW}[INFO]${NC} $*"; }

# id | label | input_file | company | app_id | app_code | env
declare -a CASES=(
    "1|FILE_TRANS pre+post command          |test_case1_filetrans_prepost.xml   |scb|AP1001|erp   |dev"
    "2|FILE_TRANS wildcard paths            |test_case2_filetrans_wildcard.xml  |scb|AP1002|expinv|dev"
    "3|FILE_TRANS FTP-UPLOAD=3 (watch mode)|test_case3_filetrans_filewatch.xml |scb|AP1003|als   |dev"
    "4|FileWatch jobs (PsrpOperator)        |test_case4_filewatch.xml           |scb|AP1004|edw   |dev"
    "5|OS jobs (SSHOperator)                |test_case5_os_jobs.xml             |scb|AP1005|clr   |dev"
    "6|AWS Step Function                    |test_case6_aws_stepfunction.xml    |scb|AP1006|nss   |dev"
)

# determine which cases to run
if [ $# -gt 0 ]; then
    RUN_IDS=("$@")
else
    RUN_IDS=(1 2 3 4 5 6)
fi

TOTAL=0
PASSED=0
FAILED=0
FAILED_CASES=()

log "========================================"
log "  controlm2airflow — test suite"
log "  $(date '+%Y-%m-%d %H:%M:%S')"
log "========================================"

for case_def in "${CASES[@]}"; do
    IFS='|' read -r id label input_file company app_id app_code env <<< "${case_def}"
    # trim whitespace
    label="$(echo "${label}" | xargs)"
    input_file="$(echo "${input_file}" | xargs)"
    app_code="$(echo "${app_code}" | xargs)"

    # skip if not requested
    skip=true
    for req in "${RUN_IDS[@]}"; do
        [ "${req}" = "${id}" ] && skip=false && break
    done
    $skip && continue

    TOTAL=$((TOTAL + 1))
    CASE_LOG="${LOG_DIR}/case${id}_${TIMESTAMP}.log"
    input_path="${SCRIPT_DIR}/input/${input_file}"

    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    info "Case ${id}: ${label}"
    info "Input  : ${input_file}"
    info "Params : company=${company} app_id=${app_id} app_code=${app_code} env=${env}"
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    if [ ! -f "${input_path}" ]; then
        fail "Case ${id} — input file not found: ${input_path}"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${id}: ${label} — input file missing")
        continue
    fi

    # clean previous output
    rm -f "${OUTPUT_DIR}"/*.py

    # run skill via claude
    info "Running claude skill..."
    claude --print --allowedTools "Read,Write,Bash" 2>&1 | tee "${CASE_LOG}" << EOF
Follow the skill defined in skills/controlm2airflow/skill.md to convert Control-M jobs to Airflow DAGs.

Input XML: ${input_path}
Output directory: ${OUTPUT_DIR}/

Parameters:
company  = ${company}
app_id   = ${app_id}
app_code = ${app_code}
env      = ${env}
EOF

    # check output was generated
    dag_files=("${OUTPUT_DIR}"/*.py)
    if [ ${#dag_files[@]} -eq 0 ] || [ ! -f "${dag_files[0]}" ]; then
        fail "Case ${id} — no .py output generated"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${id}: ${label} — no DAG output")
        continue
    fi

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
        pass "Case ${id} PASSED — ${label}"
        PASSED=$((PASSED + 1))
    else
        fail "Case ${id} FAILED — ${label} (DAG syntax error, see ${CASE_LOG})"
        FAILED=$((FAILED + 1))
        FAILED_CASES+=("${id}: ${label} — DAG syntax error")
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
log "  Summary log: ${SUMMARY_LOG}"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

[ "${FAILED}" -eq 0 ]
