# Contributing Guide

[中文](CONTRIBUTING.md) **·** [English]

Thanks for your interest in contributing to Binomic! This document explains
how to get involved.

## AI-assisted Code Policy

**AI-assisted code is welcome in this project.** However:

- You (the contributor) must **carefully review** every line you submit,
  whether you wrote it yourself or with AI assistance;
- AI-assisted code carries **the same responsibility** as code you wrote
  yourself — correctness, security, and license compliance are the
  contributor's responsibility;
- Submitting a change means you fully understand and endorse it; "the AI wrote
  it" is never an excuse for defects.

## Development Environment

This project uses [uv](https://docs.astral.sh/uv/) for dependency management
and requires Python ≥ 3.12.

```bash
git clone git@github.com:Ahsen17/binomic.git
cd binomic
make install    # create the virtual environment and install dependencies
make check      # lint + type check + full test suite
```

Common commands:

| Command | Purpose |
|-|-|
| `make fix` | ruff auto-fix and formatting |
| `make lint` | pre-commit + mypy (strict) |
| `make test` | unit tests |
| `make test-integration` | integration tests (requires a local Redis) |
| `make coverage` | coverage report (`fail_under = 80`) |

## Contribution Workflow

1. Claim or raise an issue in
   [Issues](https://github.com/Ahsen17/binomic/issues);
2. Fork the repository and create a feature branch from `main`, e.g.
   `feat/broker-metrics`;
3. Develop and add tests (see "Testing Requirements" below);
4. Make sure `make check` passes locally;
5. Open a Pull Request to `main`; CI (`pytest.yml`) must be fully green.

## Commit Convention

Commit messages follow
[Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>?): <description>

<body?>

<footer?>
```

Common types: `feat`, `fix`, `refactor`, `doc`, `perf`, `style`, `test`,
`build`, `chore`. CHANGELOG.md is generated automatically from the commit
history by [git-cliff](https://git-cliff.org) and **must not be edited by
hand in principle**.

## Testing Requirements

- The test tree mirrors the source tree:
  `src/binomic/base/schemas.py` → `tests/base/test_schemas.py`;
- Shared fixtures live in `tests/conftest.py`; submodule fixtures live in that
  submodule's `conftest.py`;
- Test cases are organized as `class TestXX`;
- External dependency strategy: unit tests use fakexx packages (e.g.
  fakeredis); dependencies without a fakexx package use async mocks;
  integration tests (`test_integration/`) run against real services;
- New features must come with tests, and coverage may not drop below 80%.

## Code of Conduct

Stay respectful and professional. Critique code, not people, and make
newcomers feel welcome to ask questions.
