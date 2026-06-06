#!/usr/bin/env bash
# run_tests.sh — run controlm2airflow skill on input XML files
#
# Usage:
#   ./run_tests.sh                          # run all input/test_case*.xml
#   ./run_tests.sh input/test_case1.xml     # run specific file(s) by path
#   ./run_tests.sh --scenario wildcard      # run files matching keyword(s)
#   ./run_tests.sh --scenario filewatch aws # run files matching any keyword
#   ./run_tests.sh --scenario reusable      # run reusable DAG test case(s)
#   ./run_tests.sh --list                   # list available scenarios
#
# Scenario keywords match against the input filename (case-insensitive substring).
# Examples: prepost, wildcard, filewatch, os, aws, reusable
#
# Output: output/<basename_without_ext>/  per input file (preserved across runs)
# Logs:   logs/run_tests_<timestamp>.log  + logs/<basename>_<timestamp>.log per case
#
# Test cases:
#   1  test_case1_filetrans_prepost.xml         FILE_TRANS with pre-command + post-command
#   2  test_case2_filetrans_wildcard.xml        FILE_TRANS with wildcard file paths
#   3  test_case3_filetrans_filewatch.xml       FILE_TRANS with FTP-UPLOAD=3 (file watch)
#   4  test_case4_filewatch.xml                 FileWatch jobs → PsrpOperator polling
#   5  test_case5_os_jobs.xml                   OS jobs → SSHOperator
#   6  test_case6_aws_stepfunction.xml          AWS Step Function + S3 upload
#   7  test_case7_filetrans_ftpssl.xml          FILE_TRANS FTP-SSL with %%D day variable
#   8  test_case8_cyclic.xml                    CYCLIC=1 INTERVAL=15M parallel chains
#   9  test_case9_multi_schedule.xml            Folder with 2 FILE_TRANS at different TIMEFROM → 2 DAGs
#  10  test_case10_reusable_dag.xml             Mode B: reusable unix DAG — TriggerDagRunOperator + ExternalTaskSensor

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

usage() {
    # print header comment block (lines 2-30, strip leading '# ')
    sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
    echo ""
    echo "Available scenarios (input/test_case*.xml):"
    for f in "${INPUT_DIR}"/test_case*.xml; do
        [ -f "${f}" ] && echo "  $(basename "${f}" .xml)"
    done
}

# parse arguments
SCENARIOS=()
INPUT_FILES=()

while [ $# -gt 0 ]; do
    case "$1" in
        --list|-l)
            usage; exit 0 ;;
        --scenario|-s)
            shift
            while [ $# -gt 0 ] && [[ "$1" != --* ]]; do
                SCENARIOS+=("$1"); shift
            done ;;
        --help|-h)
            usage; exit 0 ;;
        *.xml)
            INPUT_FILES+=("$1"); shift ;;
        *)
            # treat bare word as a scenario keyword
            SCENARIOS+=("$1"); shift ;;
    esac
done

# resolve final file list
if [ ${#INPUT_FILES[@]} -gt 0 ]; then
    : # explicit paths — use as-is
elif [ ${#SCENARIOS[@]} -gt 0 ]; then
    # filter test_case*.xml by scenario keyword(s)
    for f in "${INPUT_DIR}"/test_case*.xml; do
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
        echo "Run './run_tests.sh --list' to see available scenarios."
        exit 1
    fi
else
    INPUT_FILES=("${INPUT_DIR}"/test_case*.xml)
fi

if [ ${#INPUT_FILES[@]} -eq 0 ] || [ ! -f "${INPUT_FILES[0]}" ]; then
    echo "No input XML files found in ${INPUT_DIR}/"
    exit 1
fi

# default conversion params — override per file by naming convention if needed
COMPANY="scb"
APP_CODE="app"
ENV="dev"

# per-file param lookup (prefix → app_id:app_code)
get_params() {
    local base="$1"
    case "${base}" in
        test_case1*) echo "AP1001:erp"    ;;
        test_case2*) echo "AP1002:expinv" ;;
        test_case3*) echo "AP1003:als"    ;;
        test_case4*) echo "AP1004:edw"    ;;
        test_case5*) echo "AP1005:clr"    ;;
        test_case6*) echo "AP1006:nss"    ;;
        test_case7*)  echo "AP1007:rbf"    ;;
        test_case8*)  echo "AP1008:nss"    ;;
        test_case9*)  echo "AP1009:expinv" ;;
        test_case10*) echo "AP1010:expinv" ;;
        *)            echo "AP9999:app"    ;;
    esac
}

TOTAL=0
PASSED=0
FAILED=0
FAILED_CASES=()

# token usage accumulators
TOTAL_INPUT_TOKENS=0
TOTAL_OUTPUT_TOKENS=0
TOTAL_CACHE_READ_TOKENS=0
TOTAL_COST_USD=0

# parse token usage from a claude stream-json log file
# prints: "input=N output=N cache_read=N cost=$N"
extract_tokens() {
    local log_file="$1"
    # last line of stream-json output is the result JSON containing modelUsage
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

input_tok   = 0
output_tok  = 0
cache_read  = 0
cost_usd    = 0.0

for model, stats in model_usage.items():
    input_tok  += stats.get('inputTokens', 0)
    output_tok += stats.get('outputTokens', 0)
    cache_read += stats.get('cacheReadInputTokens', 0)
    cost_usd   += stats.get('costUSD', 0.0)

print(f"input={input_tok} output={output_tok} cache_read={cache_read} cost={cost_usd:.6f}")
PYEOF
}

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

    # resolve per-file params
    params="$(get_params "${base}")"
    app_id="${params%%:*}"
    app_code="${params##*:}"

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

    # determine output mode: reusable_dag for cases whose filename contains "reusable"
    mode="inline"
    if echo "${base}" | grep -qi "reusable"; then
        mode="reusable_dag"
    fi

    # run skill via claude (stream-json for token tracking)
    STREAM_LOG="${CASE_LOG%.log}_stream.jsonl"
    info "Running claude skill (mode=${mode})..."
    {
        printf 'Follow the skill defined in skills/controlm2airflow/skill.md to convert Control-M jobs to Airflow DAGs.\n\n'
        printf 'Input XML: %s\n' "${input_path}"
        printf 'Output directory: %s/\n\n' "${CASE_OUTPUT}"
        printf 'Parameters:\n'
        printf 'company  = %s\n' "${COMPANY}"
        printf 'app_id   = %s\n' "${app_id}"
        printf 'app_code = %s\n' "${app_code}"
        printf 'env      = %s\n' "${ENV}"
        printf 'mode     = %s\n' "${mode}"
        if [ "${mode}" = "reusable_dag" ]; then
            printf '\nOutput mode is [B] Reusable transfer DAG.\n'
            printf 'Generate the reusable unix transfer DAG (cloned from file-transfer-reusable-unix-dynamic template)\n'
            printf 'AND the caller business DAG. FILE_TRANS jobs must become TriggerDagRunOperator + ExternalTaskSensor pairs.\n'
            printf 'All 11 conf params must be passed explicitly. Dependency chains wire to wait_* sensor tasks.\n'
            printf 'Verify both output files with python + pyflakes.\n'
        fi
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
        # print assistant text chunks to human-readable log
        if obj.get('type') == 'assistant' and 'message' in obj:
            for block in obj['message'].get('content', []):
                if block.get('type') == 'text':
                    print(block['text'], end='', flush=True)
    except json.JSONDecodeError:
        print(line, flush=True)
" 2>&1 | tee "${CASE_LOG}" || true

    # extract token usage from stream log
    token_info="$(extract_tokens "${STREAM_LOG}" 2>/dev/null || echo "input=0 output=0 cache_read=0 cost=0.000000")"
    case_input=$(echo "${token_info}"  | sed -nE 's/.*input=([0-9]+).*/\1/p')
    case_output=$(echo "${token_info}" | sed -nE 's/.*output=([0-9]+).*/\1/p')
    case_cache=$(echo "${token_info}"  | sed -nE 's/.*cache_read=([0-9]+).*/\1/p')
    case_cost=$(echo "${token_info}"   | sed -nE 's/.*cost=([0-9.]+).*/\1/p')

    TOTAL_INPUT_TOKENS=$((TOTAL_INPUT_TOKENS   + ${case_input:-0}))
    TOTAL_OUTPUT_TOKENS=$((TOTAL_OUTPUT_TOKENS + ${case_output:-0}))
    TOTAL_CACHE_READ_TOKENS=$((TOTAL_CACHE_READ_TOKENS + ${case_cache:-0}))
    TOTAL_COST_USD=$(python3 -c "print(f'{float(\"${TOTAL_COST_USD}\") + float(\"${case_cost:-0}\"):.6f}')" 2>/dev/null || echo "${TOTAL_COST_USD}")

    info "Tokens : input=${case_input:-0} output=${case_output:-0} cache_read=${case_cache:-0} cost=\$${case_cost:-0.000000}"

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
log "  Token usage (all cases):"
log "    Input tokens      : ${TOTAL_INPUT_TOKENS}"
log "    Output tokens     : ${TOTAL_OUTPUT_TOKENS}"
log "    Cache read tokens : ${TOTAL_CACHE_READ_TOKENS}"
log "    Total cost (USD)  : \$${TOTAL_COST_USD}"
log ""
log "  Output : ${OUTPUT_BASE}/<case-name>/"
log "  Log    : ${SUMMARY_LOG}"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

[ "${FAILED}" -eq 0 ]
