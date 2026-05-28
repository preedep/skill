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

`test_run.sh` cleans the output folder, runs the conversion, and verifies DAG syntax in one step:

```bash
./test_run.sh
```

Edit variables at the top of `test_run.sh` to change input file, company, app_id, app_code, and env.

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
