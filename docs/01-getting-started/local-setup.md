# Local Setup

This guide covers development environment setup for ASAP.

## Pre-commit Hooks

Pre-commit hooks ensure code quality before committing.

### Installation

```bash
pip3 install pre-commit

# Install hooks (run from repo root)
cd /path/to/ASAPQuery
pre-commit install
```

### What the Hooks Do

The pre-commit configuration (`.pre-commit-config.yaml`) runs:

**Rust:**
- `rustfmt` - Format Rust code
- `cargo check` - Check Rust code compiles
- `cargo clippy` - Lint Rust code (all warnings as errors)

**Python:**
- `black` - Format Python code
- `isort` - Sort Python imports
- `flake8` - Lint Python code
- `mypy` - Type check Python code (in components that have it configured)

**General:**
- Remove trailing whitespace
- Ensure files end with newline
- Validate YAML files
- Check for large files

### Running Hooks

**Automatic** (on git commit):
```bash
git add myfile.rs
git commit -m "Fix bug"
# Pre-commit hooks run automatically
```

**Manual** (run on all files):
```bash
pre-commit run --all-files
```

**Manual** (run on staged files):
```bash
pre-commit run
```

### Common Hook Failures

**Issue**: `black` or `rustfmt` fails

**Solution**: The hook auto-fixes formatting. Just re-stage and commit:
```bash
git add <file>
git commit
```
**Issue**: `clippy` warnings

**Solution**: Fix warnings

**Issue**: `cargo fmt` checks failed

**Solution**: Run `cargo fmt --`
