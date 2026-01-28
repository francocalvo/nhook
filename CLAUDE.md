# Project Configuration

Notion Webhook server project.

# Python Guidelines

## Package Management (uv)

Always that you need to add packages, use it like this:

```bash
uv sync              # Install dependencies
uv add <package>     # Add dependency
uv lock              # Update lockfile
uv run <command>     # Run in venv
```

Never edit the `pyproject.toml` for adding dependencies.

## Code Quality (ruff)

```bash
ruff check .         # Lint
ruff format .        # Format
ruff check --fix .   # Auto-fix
```

- Config in `pyproject.toml` under `[tool.ruff]`

## Testing (pytest)

```bash
uv run pytest                    # Run all
uv run pytest -k "test_name"     # Run specific
uv run pytest --cov=src          # With coverage
```

- Place tests in `tests/` mirroring `src/` structure
- Use fixtures for shared setup

## Type Hints

- All functions must have type annotations
- Use `from __future__ import annotations`
- Run `mypy` or `pyright` for type checking

# Development Conventions

For commiting, use the default '/commit' Command, or indications in
.claude/commands/commit.md

Create a commit using commitizen after each feature. Each commit should be
granular:

- added feature.
- fixed tests.
- formatted files.
- created tests.

You get the gist.

## Branch Naming

- `feature/<ticket>-<description>`
- `fix/<ticket>-<description>`
- `chore/<description>`

## Commit Messages

Format: `<type>(<scope>): <description>`

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

```
feat(etl): add daily sales aggregation pipeline
fix(redshift): correct timezone handling in date columns
```

Never add a tool like Claude Code, Codex, etc as co-author.

# Nix Guidelines

If you need extra CLI tools, you can use the Nix dev-shell provided in the
flake.

Use 'nix develop' to enter it.
