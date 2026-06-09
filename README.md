# AI Agent Skill Library

Reusable skills for AI Agents, developed by Preedee@Digital.Dev.  
Each skill is a self-contained unit of agent capability that can be composed into larger workflows.

## Available Skills

| Skill | Description |
|-------|-------------|
| [controlm2airflow](skills/controlm2airflow/skill.md) | Convert Control-M job XML exports to Apache Airflow 3.x DAG Python files |

---

## How to Run a Skill

Skills are invoked interactively inside a **Claude Code** session. The general prompt pattern is:

```
convert control-m job to airflow dag
- input: folder: <path-to-xml>
- output: folder: <output-dir>/*

company name = <company>
app_id = <app_id>
app_code = <app_code>
env = <env>
```

---

## controlm2airflow

Convert a Control-M XML export to Airflow DAG Python files.

### Prerequisites

```bash
python -m venv .venv
source .venv/bin/activate
pip install apache-airflow \
    apache-airflow-providers-ssh \
    apache-airflow-providers-microsoft-psrp \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-amazon \
    pendulum
```

### Example prompt

```
convert control-m job to airflow dag
- input: folder: input/input1.xml
- output: folder: output/*

company name = scb
app_id = APP1234
app_code = TESTAPP
env = dev
```

### Test script

`run_tests.sh` runs one or more test cases end-to-end: invokes the skill via Claude, then syntax- and lint-checks every generated DAG with `python` + `pyflakes`. Token usage and cost are reported per case and totalled in the summary.

```bash
# Run all 10 test cases
./run_tests.sh

# Run a specific file
./run_tests.sh input/test_case1.xml

# Run by keyword (matches against the filename, case-insensitive)
./run_tests.sh --scenario prepost
./run_tests.sh --scenario wildcard
./run_tests.sh --scenario filewatch
./run_tests.sh --scenario os
./run_tests.sh --scenario aws
./run_tests.sh --scenario ftpssl
./run_tests.sh --scenario cyclic
./run_tests.sh --scenario multi
./run_tests.sh --scenario reusable

# Run multiple keywords at once (any match)
./run_tests.sh --scenario wildcard aws

# List available test cases
./run_tests.sh --list
```

#### Test cases

| # | File | Keyword | Feature tested |
|---|------|---------|----------------|
| 1 | `test_case1_filetrans_prepost.xml` | `prepost` | FILE_TRANS with pre-command + post-command |
| 2 | `test_case2_filetrans_wildcard.xml` | `wildcard` | FILE_TRANS with wildcard file paths (`*.DAT`, `*.*`) |
| 3 | `test_case3_filetrans_filewatch.xml` | `filewatch` | FILE_TRANS with `FTP-UPLOAD=3` (file watch mode) |
| 4 | `test_case4_filewatch.xml` | `filewatch` | `FileWatch` jobs → `PsrpOperator` polling (Windows) |
| 5 | `test_case5_os_jobs.xml` | `os` | OS jobs → `SSHOperator` |
| 6 | `test_case6_aws_stepfunction.xml` | `aws` | AWS Step Function + S3 upload |
| 7 | `test_case7_filetrans_ftpssl.xml` | `ftpssl` | FILE_TRANS FTP-SSL, wildcard, PRECOMM mkdir, `%%D` variable |
| 8 | `test_case8_cyclic.xml` | `cyclic` | `CYCLIC=1 INTERVAL=15M`, two parallel OS→FILE_TRANS→OS chains |
| 9 | `test_case9_multi_schedule.xml` | `multi` | 2 FILE_TRANS jobs at different `TIMEFROM` → must produce 2 DAGs |
| 10 | `test_case10_reusable_dag.xml` | `reusable` | Mode B: reusable unix DAG + caller DAG (`TriggerDagRunOperator` + `ExternalTaskSensor`) |

Output DAGs land in `output/<case-name>/`. Logs are written to `logs/`.  
A case passes only when both `python <dag>.py` and `pyflakes <dag>.py` exit 0.

### Output

One `.py` file per Control-M folder:

```
<company>-<app_id>-<app_code>-<folder_name>-<env>.py
```

Example: `scb-app1234-testapp-edw_bi_watcher-dev.py`

### Supported job types

| Control-M `APPL_TYPE` | Airflow Operator / Sensor |
|-----------------------|--------------------------|
| `OS` (Unix) | `SSHOperator` |
| `OS` (Windows) | `PsrpOperator` |
| `FILE_TRANS` (Unix→any) | `SSHOperator` + `lftp` / `azcopy` / `aws s3 cp` |
| `FILE_TRANS` (Windows→any) | `PsrpOperator` + `WinSCP` / `azcopy` |
| `FileWatch` | `FileSensor` (reschedule mode) |
| `AWS` (Step Function) | `StepFunctionStartExecutionOperator` + `StepFunctionExecutionSensor` |
| *(unsupported)* | `# TODO:` comment emitted |

### What the skill handles automatically

- Naming conventions for DAG file, DAG ID, and task IDs
- Control-M `%%ODATE`, `%%PREV`, `%%SUBSTR`, `%%$YEAR.` → Airflow Jinja templates
- Schedule derivation from `TIMEFROM`, `DAYS`, `FOLDER_ORDER_METHOD`, and folder name suffix
- Task dependencies from `INCOND`/`OUTCOND` (same-folder wiring + `ExternalTaskSensor` for cross-folder)
- `AND_OR="O"` → `trigger_rule=TriggerRule.ONE_SUCCESS`
- `email_on_failure=False`, `email_on_retry=False` on all DAGs
- `success_callback` / `failure_callback` with email notification hooks
- Control-M job configuration preserved as inline comments on each task
- DAG syntax verified via `python <dag>.py` after generation

Full specification: [`skills/controlm2airflow/skill.md`](skills/controlm2airflow/skill.md)

---

## Repository Structure

```
skill/
├── README.md
├── CLAUDE.md
├── LICENSE
├── .gitignore
└── skills/
    └── controlm2airflow/
        ├── skill.md              # full conversion specification
        ├── controlm-schema.xsd   # Control-M XML schema reference
        └── templates/            # company Airflow DAG templates (gitignored)
```

## License

MIT — Preedee@Digital.Dev 2026
