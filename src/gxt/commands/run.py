"""gxt run command - build assignment SQL and optionally execute it in the warehouse."""
from pathlib import Path
import typer
from typing import Optional

from ..adapters.bigquery import BigQueryAdapter
from ..parser.manifest import compile_manifest
from ..utils.profiles import load_profile
import yaml


app = typer.Typer()


def build_assignment_sql(audience_sql: str, hash_sql_expr: str, variants: list, randomization_unit: str) -> str:
    """Construct a simple assignment SQL statement using a precomputed hash bucket expression.

    This function returns SQL that selects the randomization unit and assigns a variant
    based on cumulative exposures. It's intentionally simple and should be adapted per
    warehouse SQL dialect and performance needs.
    """
    # Build CASE expression for variant assignment using cumulative exposure
    cumulative = 0.0
    cases = []
    for v in variants:
        name = v.get("name")
        exposure = float(v.get("exposure", 0))
        low = cumulative
        cumulative += exposure
        high = cumulative
        cases.append((name, low, high))

    case_clauses = []
    for name, low, high in cases:
        case_clauses.append(f"WHEN hash_bucket >= {low} AND hash_bucket < {high} THEN '{name}'")

    case_sql = "\n        ".join(case_clauses)

    # Ensure we deduplicate the randomization unit values so each unit gets a
    # single assignment row even if the audience SQL returns duplicates.
    # Build SQL with a hashed CTE so we can reference the alias `hash_bucket` in the CASE expression
    sql = (
        f"WITH audience AS (\n{audience_sql}\n),\n"
        f"unique_audience AS (\n  SELECT DISTINCT {randomization_unit} AS {randomization_unit} FROM audience\n),\n"
        f"hashed AS (\n  SELECT {randomization_unit} AS {randomization_unit},\n    {hash_sql_expr} AS hash_bucket\n  FROM unique_audience\n)\n"
        f"SELECT\n  {randomization_unit} AS {randomization_unit},\n  hash_bucket,\n  CASE\n        {case_sql}\n    END AS variant\nFROM hashed"
    )

    return sql


@app.callback(invoke_without_command=True)
def run(
    experiment: str = typer.Argument(..., help="Experiment name (directory under experiments/)."),
    project_path: Optional[str] = typer.Option(None, "--project-path", "-p", help="Project root path where the experiments/ folder lives"),
    adapter: Optional[str] = typer.Option(None, help="Adapter to use for execution, e.g. 'bigquery'"),
    dry_run: bool = typer.Option(True, help="If set, prints SQL and does not execute."),
    create_assignments_table: bool = typer.Option(False, "--create-assignments-table", help="If set, create the assignments table in the warehouse if it does not exist."),
):
    """Run assignments for a given experiment: compile assignment SQL and optionally execute it."""
    # choose project root: provided project_path or current working dir
    root = Path(project_path).resolve() if project_path else Path.cwd()
    exp_dir = root / "experiments" / experiment
    if not exp_dir.exists():
        typer.echo(f"Experiment not found: {experiment}")
        raise typer.Exit(code=1)

    cfg_file = exp_dir / "config.yml"
    audience_file = exp_dir / "audience.sql"
    if not cfg_file.exists() or not audience_file.exists():
        typer.echo(f"Experiment {experiment} missing config.yml or audience.sql")
        raise typer.Exit(code=2)

    import yaml
    cfg = yaml.safe_load(cfg_file.read_text())
    variants = cfg.get("variants") or []
    randomization_unit = cfg.get("randomization_unit") or "user_id"

    # Determine adapter
    adapter_obj = None
    if adapter:
        adapter = adapter.lower()
        if adapter == "bigquery":
            adapter_obj = BigQueryAdapter()
        else:
            typer.echo(f"Unknown adapter: {adapter}. Proceeding with dry-run only.")
    else:
        # attempt to load profile from project gxt_project.yml -> profiles.yml
        # (use the resolved project root from --path, do NOT reset to cwd)
        gxt_yml = root / "gxt_project.yml"
        profile_output = None
        if gxt_yml.exists():
            try:
                proj = yaml.safe_load(gxt_yml.read_text()) or {}
                profile_name = proj.get("profile", "gxt_profile")
                profile_output = load_profile(root, profile_name)
                if profile_output and profile_output.get("type") == "bigquery":
                    adapter_obj = BigQueryAdapter.from_profile(profile_output)
            except Exception:
                pass

    # Get audience SQL content
    # Prefer compiled audience SQL (so any {{ source(...) }} markers are qualified).
    # Compile the project manifest and require a compiled audience_sql entry.
    try:
        manifest = compile_manifest(root, adapter=adapter_obj)
    except Exception as e:
        typer.echo(f"Manifest compilation failed: {e}")
        typer.echo("Fix compile errors before running. Aborting.")
        raise typer.Exit(code=2)

    exp_entry = manifest.get("experiments", {}).get(experiment)
    if not exp_entry or not exp_entry.get("audience_sql"):
        typer.echo(f"Experiment '{experiment}' not found in compiled manifest or missing audience_sql. Aborting.")
        raise typer.Exit(code=2)

    audience_sql = exp_entry["audience_sql"]

    # Require an adapter that can provide a hash expression. We don't allow a
    # vendor-specific fallback anymore even for dry-runs: the preview must
    # match the configured adapter/dialect to avoid misleading outputs.
    if adapter_obj is None or not hasattr(adapter_obj, "hash_bucket_sql"):
        typer.echo("A configured adapter that implements `hash_bucket_sql` is required to build assignment SQL.")
        typer.echo("Provide --adapter (e.g. --adapter bigquery) or configure profiles.yml and gxt_project.yml.")
        raise typer.Exit(code=3)

    # Build hash SQL using the adapter and salt by experiment name.
    hash_sql_expr = adapter_obj.hash_bucket_sql(randomization_unit, salt=experiment)

    assignment_sql = build_assignment_sql(audience_sql, hash_sql_expr, variants, randomization_unit)

    if dry_run:
        typer.echo("--- Assignment SQL (dry-run) ---")
        typer.echo(assignment_sql)
        return

    # For non-dry-run, also require execution support
    if not hasattr(adapter_obj, "execute"):
        typer.echo("The configured adapter does not support execution. Aborting.")
        raise typer.Exit(code=4)

    # Determine target assignments table from experiment config or project-level gxt_project.yml
    # Priority: config.yml assignments_table -> gxt_project.yml assignments_table -> gxt_project.yml dataset + gxt_assignments
    assignments_table = None
    try:
        cfg = yaml.safe_load((exp_dir / "config.yml").read_text()) or {}
        assignments_table = cfg.get("assignments_table")
    except Exception:
        assignments_table = None

    if not assignments_table:
        # try project-level gxt_project.yml
        gxt_yml = root / "gxt_project.yml"
        if gxt_yml.exists():
            try:
                proj = yaml.safe_load(gxt_yml.read_text()) or {}
                assignments_table = proj.get("assignments_table")
                # If assignments_table is not fully qualified, check for dataset in gxt_project.yml
                if not assignments_table:
                    project_dataset = proj.get("dataset")
                    if project_dataset:
                        assignments_table = f"{project_dataset}.gxt_assignments"
            except Exception:
                assignments_table = None

    if not assignments_table:
        typer.echo("Could not determine target assignments_table for upsert (set assignments_table or dataset in gxt_project.yml). Aborting.")
        raise typer.Exit(code=5)

    # For non-dry-run runs we will upsert into the assignments table. Build a SELECT that
    # matches the assignments schema. We assume assignments table columns: experiment_id, {randomization_unit}, variant, assigned_at
    # assigned_at will be set to CURRENT_TIMESTAMP() in the inserted rows.
    src_select = (
        f"SELECT\n  '{experiment}' AS experiment_id,\n  CAST({randomization_unit} AS STRING) AS {randomization_unit},\n  variant AS variant,\n  CURRENT_TIMESTAMP() AS assigned_at\nFROM (\n{assignment_sql}\n)"
    )

    # Use adapter upsert API which will execute a MERGE inserting only new rows
    if not hasattr(adapter_obj, "upsert_from_select"):
        typer.echo("The configured adapter does not support upsert_from_select. Aborting.")
        raise typer.Exit(code=6)

    # If requested, ensure assignments table exists (adapter will create only if client available)
    if create_assignments_table:
        # Default schema: experiment_id STRING, <randomization_unit> STRING, variant STRING, assigned_at TIMESTAMP
        default_schema = [
            {"name": "experiment_id", "type": "STRING"},
            {"name": randomization_unit, "type": "STRING"},
            {"name": "variant", "type": "STRING"},
            {"name": "assigned_at", "type": "TIMESTAMP"},
        ]
        try:
            adapter_obj.ensure_table_exists(assignments_table, schema=default_schema, location=getattr(adapter_obj, 'location', None))
        except Exception as e:
            typer.echo(f"Could not ensure assignments table exists: {e}")
            raise typer.Exit(code=8)

    # If adapter profile did not include explicit credentials, inform the user we're
    # attempting Application Default Credentials (ADC). This is helpful when running
    # on GCP VMs or Cloud Functions where ADC is provided automatically.
    try:
        profile_missing_creds = False
        if hasattr(adapter_obj, 'client') and adapter_obj.client is None:
            # We couldn't build a client earlier; profile may be missing credentials
            profile_missing_creds = True
    except Exception:
        profile_missing_creds = False

    if profile_missing_creds:
        typer.echo("Note: no BigQuery client could be created from the configured profile.\n" \
                   "If you're running on GCP, Application Default Credentials (ADC) may be used automatically.\n" \
                   "Locally you can run: `gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS.")

    typer.echo("Performing upsert into assignments table...")
    # Use key columns: experiment_id and the randomization unit
    try:
        # Provide explicit insert columns so the adapter does not rely on transient
        # internal state. This ensures the randomization unit column name is used.
        insert_cols = ["experiment_id", randomization_unit, "variant", "assigned_at"]
        result = adapter_obj.upsert_from_select(assignments_table, src_select, ["experiment_id", randomization_unit], insert_columns=insert_cols)
        typer.echo("Upsert returned:")
        typer.echo(str(result))
    except Exception as e:
        typer.echo(f"Upsert failed: {e}")
        raise typer.Exit(code=7)
