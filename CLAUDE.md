# CLAUDE.md — AI Agent Skill Library

## Project Purpose

This repository consolidates reusable skills developed by Preedee@Digital.Dev for AI Agents. Each skill is a self-contained unit of agent capability that can be composed into larger agent workflows.

## Repository Structure

```
skill/
├── CLAUDE.md
├── LICENSE           (MIT)
├── .gitignore        (Rust defaults)
└── skills/           # individual skill implementations
    ├── controlm2airflow/   # Convert Control-M jobs to Apache Airflow DAGs
    │   ├── skill.md
    │   └── controlm-schema.xsd
    └── <skill-name>/       # (future skills follow the same pattern)
        ├── skill.md        # skill definition: purpose, inputs, outputs, examples
        └── ...             # supporting assets or code
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

## Constraints 
- Not save or use real company data to git.

## Owner

Preedee@Digital.Dev — MIT License 2026
