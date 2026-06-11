# CLAUDE.md — AI Agent Skill Library

## Project Purpose

This repository consolidates reusable skills developed by Preedee@Digital.Dev for AI Agents. Each skill is a self-contained unit of agent capability that can be composed into larger agent workflows.

## Repository Structure

```
skill/
├── CLAUDE.md
├── LICENSE               (MIT)
├── .gitignore
├── run_tests.sh          # run all skill test cases (see Testing section)
├── create_reusable_dag.sh # generate reusable file-transfer DAGs from parameters (see Reusable DAG section)
├── input/                # test input XML files (gitignored — no real data)
├── output/               # generated DAG files (gitignored)
├── logs/                 # test run logs (gitignored)
└── skills/               # individual skill implementations
    ├── controlm2airflow/ # Convert Control-M jobs to Apache Airflow DAGs
    │   ├── skill.md
    │   ├── raws/         # reference data (gitignored — no real data)
    │   │   ├── controlm-schema.xsd
    │   │   ├── neutron_export_xml_260522.xml
    │   │   └── node_id.md
    │   └── templates/    # DAG/config/groovy templates per operator type (gitignored except reusable-*-dynamic *.py)
    │       ├── file-transfer-reusable-unix-dynamic/dags/project_file_transfer_unix_dynamic.py  (tracked)
    │       └── file-transfer-reusable-win-dynamic/dags/project_file_transfer_win_dynamic.py   (tracked)
    └── <skill-name>/     # (future skills follow the same pattern)
        ├── skill.md      # skill definition: purpose, inputs, outputs, examples
        └── ...           # supporting assets or code
```

> Add a new entry here whenever a skill directory is created.

## Development Guidelines

### Adding a New Skill

1. Create a directory under `skills/<skill-name>/`.
2. Write a `skill.md` that defines: **purpose**, **inputs**, **outputs**, and **example usage**.
3. Keep each skill focused on a single capability — compose skills for complex tasks.
4. Document any dependencies (external APIs, tools, models) clearly.

### Language / Tooling

- Primary language: **Rust** (per `.gitignore` — adjust if the project expands to other languages).
- Follow standard `cargo` conventions: `cargo build`, `cargo test`, `cargo fmt`, `cargo clippy`.
- No `#[allow(warnings)]` or `#[allow(clippy::...)]` without a comment explaining why.

### Code Style

- No unnecessary comments — name things clearly instead.
- No dead code; remove rather than comment out.
- Prefer explicit error handling (`Result`/`Option`) over panics in library code.

### Testing

- Unit tests live in the same file (`#[cfg(test)]`).
- Integration tests go in `tests/`.
- Run `cargo test` before committing.

#### controlm2airflow skill tests

Test inputs live in `input/test_case*.xml` (gitignored). Run all cases:

```bash
./run_tests.sh                        # all cases
./run_tests.sh --scenario reusable    # by keyword
./run_tests.sh input/test_case1.xml   # by file
./run_tests.sh --list                 # list available scenarios
```

Token usage (input/output/cache/cost) is reported per case and totalled in the summary.
Stream logs saved to `logs/<case>_stream.jsonl` alongside the human-readable log.

| Case | File | Feature tested |
|------|------|----------------|
| 1 | `test_case1_filetrans_prepost.xml` | FILE_TRANS with pre-command + post-command |
| 2 | `test_case2_filetrans_wildcard.xml` | FILE_TRANS with wildcard file paths (`*.DAT`, `*.CTL`, `*.*`) |
| 3 | `test_case3_filetrans_filewatch.xml` | FILE_TRANS with `FTP-UPLOAD=3` (file watch mode) |
| 4 | `test_case4_filewatch.xml` | `FileWatch` jobs → `PsrpOperator` polling (Windows remote) |
| 5 | `test_case5_os_jobs.xml` | OS jobs → `SSHOperator` |
| 6 | `test_case6_aws_stepfunction.xml` | AWS Step Function + S3 upload |
| 7 | `test_case7_filetrans_ftpssl.xml` | FILE_TRANS FTP-SSL: wildcard upload, mixed download/upload, PRECOMM mkdir, SRCOPT delete-source, %%D day-of-week variable |
| 8 | `test_case8_cyclic.xml` | CYCLIC=1 INTERVAL=00015M: schedule=timedelta(minutes=15), two parallel chains OS→FILE_TRANS→OS, wildcard LPATH |
| 9 | `test_case9_multi_schedule.xml` | Folder with 2 independent FILE_TRANS jobs at different TIMEFROM (2100 and 0100) — must split into 2 DAGs, no cross-dependencies |
| 10 | `test_case10_reusable_dag.xml` | Mode B reusable DAG: EXPINV_DAILY_TABLE — 2 OS + 3 FILE_TRANS (Unix, 2 source hosts, 2 dest hosts, fork dependency) → reusable unix DAG + caller DAG with `TriggerDagRunOperator` + `ExternalTaskSensor` pairs |

Each case converts the XML via `claude --print` using `skill.md`, then syntax-verifies every generated DAG with `python <dag>.py` + `pyflakes` (both exit code 0 required).

#### Reusable DAG generation and verification

`create_reusable_dag.sh` generates reusable file-transfer DAGs from parameters (no XML input):

```bash
./create_reusable_dag.sh --both company=nix project=apxxxx app_code=poc env=nonprod dag_name=transfer-daily
./create_reusable_dag.sh --unix ...    # Unix DAG only
./create_reusable_dag.sh --win  ...    # Windows DAG only
./create_reusable_dag.sh --scan        # scan input/test_nonprod/*.xml → JSON catalogue
```

Each generated DAG is verified with three checks (all must pass):

| Check | Tool | What it catches |
|-------|------|-----------------|
| Python syntax | `python <dag>.py` | Import errors, indentation, SyntaxError |
| Python lint | `pyflakes <dag>.py` | Unused imports, undefined names |
| Shell lint | `shellcheck` (per bash heredoc) | Real bash bugs: bad `-exec`, unused vars, quoting issues |

`shellcheck` requires `brew install shellcheck`. Intentionally suppressed codes: SC2086 (unquoted glob vars), SC2012 (`ls` for display), SC2010 (`ls|grep` for display), SC2154 (Jinja vars look unset).

## Constraints 
- Not save or use real company data to git.

## Owner

Preedee@Digital.Dev — MIT License 2026
