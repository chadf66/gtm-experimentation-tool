# GTM Experimentation Tool (gxt)

GTM Experimentation Tool (gxt) is a small command-line utility for managing and running A/B experiments stored in a simple project layout. It provides tools to compile experiment manifests, qualify data source references, build deterministic assignment SQL, and optionally execute assignment upserts into a data warehouse. A BigQuery adapter is included to run assignment MERGE operations.

## Contents

- [Features](#features)
- [Quick start](#quick-start)
- [Project layout](#project-layout)
- [CLI reference and examples](#cli-reference-and-examples)
- [BigQuery adapter details and requirements](#bigquery-adapter-details-and-requirements)
- [Developer notes and testing](#developer-notes-and-testing)
- [License](#license)

## Features

- Compile experiment manifests and rewrite `source(...)` references to fully qualified table identifiers.
- Generate deterministic assignment SQL using hashing (BigQuery: `FARM_FINGERPRINT` + `MOD`).
- Dry-run mode prints compiled SQL and prevents writes.
- Optional execution against BigQuery with `--no-dry-run`; supports an opt-in `--create-assignments-table` helper.
- Pluggable adapter model — `BigQueryAdapter` provided as a concrete example.

## Quick start

### Prerequisites

- Python 3.10+.
- Recommended: create and activate a virtual environment.

### Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you will run against Google BigQuery, install the BigQuery client as well:

```bash
pip install google-cloud-bigquery
```

Set Google credentials (if using BigQuery):

- Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON with BigQuery permissions, or use Application Default Credentials.
- Ensure billing is enabled for the GCP project if you will run DML operations (MERGE/INSERT).

## Project layout

Repository expects projects in `projects/` with this structure:

- `gxt_project.yml` — project configuration
- `profiles.yml` — profiles mapping (project/dataset/credentials)
- `experiments/<experiment_name>/` — each experiment contains:
  - `audience.sql` — SQL that yields the set of units to assign
  - `config.yml` — experiment config (variants, randomization_unit, assignments table, etc.)
- `target/` — compiled manifests are written here by `compile`

## CLI reference

Run the CLI via module invocation or a packaged entry point:

```bash
python -m src.gxt.main <command> [args]
# or if installed as `gxt`:
gxt <command> [args]
```

### Commands

- `init` — scaffold a project from templates
- `new-experiment <name>` — create a new experiment scaffold
- `compile [EXPERIMENT] --project-path <path>` — compile manifests for the project or a single experiment
- `list --project-path <path>` — list experiments
- `validate --project-path <path>` — validate experiment configs and SQL
- `run [EXPERIMENT] --project-path <path> [--no-dry-run] [--adapter ADAPTER_NAME] [--create-assignments-table]` — build and optionally execute assignment upserts for a single experiment
- `run --group <GROUP_NAME> --project-path <path> [--no-dry-run] [--adapter ADAPTER_NAME] [--create-assignments-table]` — build and optionally execute assignment upserts for all experiments in a group

### Run command notes

- Default behavior is a dry-run (prints SQL). Use `--no-dry-run` to execute.
- `--create-assignments-table` instructs the BigQuery adapter to create the assignments table if it is missing. This is opt-in and requires BigQuery client + permissions.
- Use `--group/-g <GROUP_NAME>` to run all experiments that belong to a specific group. Experiments can specify group membership using either a `group` string or `groups` list in their `config.yml`.
- When running a group, experiments are processed in alphabetical order for deterministic results.

### Examples

Dry-run compile + run for `demo_exp`:

```bash
python -m src.gxt.main compile demo_exp --project-path projects/demo
python -m src.gxt.main run demo_exp --project-path projects/demo
```

Run all experiments in a group (dry-run):

```bash
python -m src.gxt.main run --group marketing-tests --project-path projects/demo
```

Real run against BigQuery (ensure credentials and billing enabled):

```bash
python -m src.gxt.main run dummy_experiment2 --project-path projects/test_project2 --no-dry-run --create-assignments-table
```

Real run for a group against BigQuery:

```bash
python -m src.gxt.main run --group marketing-tests --project-path projects/demo --no-dry-run --create-assignments-table
```

## BigQuery adapter details

### Behavior

- The included `BigQueryAdapter` attempts to construct a `google.cloud.bigquery.Client` when `google-cloud-bigquery` is installed and the profile supplies a project (and credentials via env var or ADC).
- If the client is not available, adapter methods fall back to printing SQL and returning empty results — safe for dry-run.

### Provided methods

- `execute(sql)` — runs a query and returns rows (or prints SQL when client unavailable)
- `insert_rows(table, rows)` — inserts rows using `insert_rows_json` when available
- `ensure_table_exists(table, schema, location)` — create table with default schema if missing
- `upsert_from_select(target_table, src_select_sql, key_columns)` — MERGE FROM (SELECT ...) that inserts rows which don't yet exist (insert-only upsert)
- `hash_bucket_sql(column, salt, precision)` — returns an expression using `FARM_FINGERPRINT` and `MOD` to compute a deterministic bucket in [0,1)

### Important notes

- Confirm billing on your GCP project before running non-dry-run: DML operations require billing.
- The default assignments schema used by the adapter is: `experiment_id STRING, unit STRING, variant STRING, assigned_at TIMESTAMP`. You can override by calling `ensure_table_exists` with an explicit schema.

## Configuration and profiles

Example `projects/<project>/profiles.yml`:

```yaml
default:
  project: my-gcp-project
  dataset: my_dataset
```

The tool resolves adapter/profile configuration using `gxt_project.yml` in each project folder when not provided via CLI flags.

## Developer notes

- CLI implementation: `src/gxt/main.py` (Typer)
- Commands: `src/gxt/commands/` (compile, run, init, list, validate, new_experiment)
- Parser and manifest compilation: `src/gxt/parser/manifest.py`
- Adapters: `src/gxt/adapters/`
- Templates: `src/gxt/templates/`

## Testing

- There are no unit tests in this snapshot. Recommended tests:
  - Unit tests for `BigQueryAdapter.upsert_from_select` (mock client)
  - Tests for `manifest.compile_manifest` and `_qualify_sources_in_sql`

## Maintenance and next improvements

- Replace transient state `adapter._last_insert_columns` by passing insert columns explicitly to `upsert_from_select`.
- Add unit tests and CI pipeline.
- Add more adapters (Redshift, Snowflake) as needed.

## License

See the `LICENSE` file in the repository.

