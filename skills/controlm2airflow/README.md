# controlm2airflow

Convert Control-M job XML exports to Apache Airflow 3.x DAG Python files.

## Prerequisites

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

## Usage

Invoke the skill via Claude Code with the following prompt:

```
convert control-m job to airflow dag
- input: folder: <path-to-xml>
- output: folder: <output-dir>/*

company name = <company>
app_id = <app_id>
app_code = <app_code>
env = <env>
```

### Example

```
convert control-m job to airflow dag
- input: folder: input/input1.xml
- output: folder: output/*

company name = scb
app_id = APP1234
app_code = TESTAPP
env = dev
```

### Output

One `.py` file per Control-M folder, named:

```
<company>-<app_id>-<app_code>-<folder_name>-<env>.py
```

Example: `scb-app1234-testapp-edw_bi_watcher-dev.py`

## Test

Use `test_run.sh` to clean the output folder, run the conversion, and verify DAG syntax in one step:

```bash
./test_run.sh
```

Edit the variables at the top of `test_run.sh` to change the input file, company, app_id, app_code, and env.

## Supported Job Types

| Control-M `APPL_TYPE` | Airflow Operator / Sensor |
|-----------------------|--------------------------|
| `OS` (Unix) | `SSHOperator` |
| `OS` (Windows) | `PsrpOperator` |
| `FILE_TRANS` (Unix→any) | `SSHOperator` + `lftp` / `azcopy` / `aws s3 cp` |
| `FILE_TRANS` (Windows→any) | `PsrpOperator` + `WinSCP` / `azcopy` |
| `FileWatch` | `FileSensor` (reschedule mode) |
| `AWS` (Step Function) | `StepFunctionStartExecutionOperator` + `StepFunctionExecutionSensor` |
| *(unsupported)* | `# TODO:` comment emitted |

## What the Skill Handles Automatically

- Naming conventions for DAG file, DAG ID, and task IDs
- Control-M `%%ODATE`, `%%PREV`, `%%SUBSTR`, `%%$YEAR.` → Airflow Jinja templates
- Schedule derivation from `TIMEFROM`, `DAYS`, `FOLDER_ORDER_METHOD`, and folder name suffix
- Task dependencies from `INCOND`/`OUTCOND` (same-folder wiring + `ExternalTaskSensor` for cross-folder)
- `AND_OR="O"` → `trigger_rule=TriggerRule.ONE_SUCCESS`
- `email_on_failure=False`, `email_on_retry=False` on all DAGs
- `success_callback` / `failure_callback` with email notification hooks
- Control-M job configuration preserved as inline comments on each task
- DAG syntax verified via `python <dag>.py` after generation

## Skill Definition

See [`skill.md`](skill.md) for the full conversion specification, including variable substitution rules, operator mapping, dependency resolution algorithm, and coding style guidelines.
