# How to Adapt an Open PR After the Repo Reorganization

In [issue #129](https://github.com/ProjectASAP/ASAPQuery/issues/129), all top-level directories were renamed to use lowercase, hyphenated `asap-` prefixes. If you opened a PR before this reorganization was merged, your branch will have conflicts and stale path references that need to be updated.

This guide walks you through rebasing onto the new structure.

---

## Directory Renames

| Old name | New name |
|---|---|
| `QueryEngineRust/` | `asap-query-engine/` |
| `CommonDependencies/` | `asap-common/` |
| `ArroyoSketch/` | `asap-summary-ingest/` |
| `Controller/` | `asap-planner/` |
| `Utilities/` | `asap-tools/` |
| `PrometheusExporters/` | `asap-tools/data-sources/prometheus-exporters/` |
| `PrometheusClient/` | `asap-tools/queriers/prometheus-client/` |
| `ExecutionUtilities/` | `asap-tools/execution-utilities/` |
| `quickstart/` | `asap-quickstart/` |
| `sketch-core/` | `asap-common/sketch-core/` |

---

## Step 1: Rebase onto main

```bash
git fetch origin
git rebase origin/main
```

Git will likely report conflicts because files it knew as (e.g.) `QueryEngineRust/src/main.rs` no longer exist at that path — they are now at `asap-query-engine/src/main.rs`.

---

## Step 2: Resolve file-move conflicts

When Git cannot automatically apply your commits because the files moved, you need to tell it where the files went.

For each conflicted file, check where it now lives:

```bash
# See which files Git is confused about
git status

# Find where a file ended up after the rename
git log --diff-filter=R --summary origin/main | grep "rename"
```

Then manually apply your changes to the new path:

```bash
# Example: your change was to QueryEngineRust/src/foo.rs
# Apply it to the new location
git checkout HEAD -- asap-query-engine/src/foo.rs   # get current version
# Then re-apply your edits manually, or use:
git show ORIG_HEAD:QueryEngineRust/src/foo.rs        # see your old version
```

---

## Step 3: Update path references in your changed files

After resolving file moves, search your branch's changes for any remaining hardcoded old paths.

```bash
# Check your diff for old names
git diff origin/main | grep -E "QueryEngineRust|CommonDependencies|ArroyoSketch|Controller/|Utilities/|PrometheusExporters|PrometheusClient|ExecutionUtilities|sketch-core"
```

Apply the substitutions from the table above to any hits. Common places to check:

- **Cargo.toml** `path =` dependencies
- **Python** `os.path.join(...)` calls
- **Shell scripts** `cd` commands and path variables
- **Docker** `build.context`, volume mounts, `COPY` directives
- **GitHub Actions** `working-directory` and `paths` triggers
- **`.pre-commit-config.yaml`** manifest paths and file regex patterns

---

## Step 4: Check sub-directory renames within asap-common

`sketch-core` moved from being a top-level directory to living inside `asap-common/`:

```toml
# Old
path = "../../sketch-core"

# New
path = "../../asap-common/sketch-core"
```

`CommonDependencies/dependencies/` is now `asap-common/dependencies/`:

```toml
# Old (Cargo.toml)
path = "../CommonDependencies/dependencies/rs/promql_utilities"

# New
path = "../asap-common/dependencies/rs/promql_utilities"
```

```python
# Old (pyproject.toml / Python imports)
mypy_path = "../../CommonDependencies/py"

# New
mypy_path = "../../asap-common/py"
```

---

## Step 5: Verify CI passes

Push your rebased branch and check that all CI jobs pass:

```bash
git push --force-with-lease origin your-branch-name
```

Key CI jobs to watch:
- **Rust** — Cargo build and tests for `asap-query-engine` and `asap-common`
- **Python** — Linting/type-checking for `asap-tools/queriers/prometheus-client`
- **Docker** — Image builds for `asap-query-engine`, `asap-planner-rs`, `asap-summary-ingest`

---

## Quick reference: sed commands for bulk renaming

If your PR touches many files, these one-liners can handle the mechanical substitutions:

```bash
# Run from repo root on files in your PR
# Adjust the file glob to match what your PR touches

# Rename directory references in Rust/TOML files
sed -i 's|QueryEngineRust|asap-query-engine|g' your_file.toml
sed -i 's|CommonDependencies|asap-common|g' your_file.toml

# Rename in Python files
sed -i 's|"Utilities"|"asap-tools"|g' your_file.py
sed -i 's|"Controller"|"asap-planner"|g' your_file.py
sed -i 's|"ArroyoSketch"|"asap-summary-ingest"|g' your_file.py
sed -i 's|"PrometheusClient"|"asap-tools/queriers/prometheus-client"|g' your_file.py
sed -i 's|"PrometheusExporters"|"asap-tools/data-sources/prometheus-exporters"|g' your_file.py

# Rename in shell scripts
sed -i 's|/Utilities/|/asap-tools/|g' your_script.sh
sed -i 's|/Controller/|/asap-planner/|g' your_script.sh
```

Always review the diff after running `sed` to make sure it didn't touch comment prose you intended to keep.
