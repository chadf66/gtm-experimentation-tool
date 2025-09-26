"""gxt validate command - validate experiments YAML and SQL."""
from pathlib import Path
import typer
import yaml
from typing import Optional

from ..parser.manifest import compile_manifest

app = typer.Typer()


@app.callback(invoke_without_command=True)
def validate(
    project_path: Optional[str] = typer.Option(None, "--project-path", "-p", help="Project root path where the experiments/ folder lives"),
    strict: bool = typer.Option(False, "--strict", help="Run additional strict checks, including attempting to compile the manifest."),
):
    """Validate experiments under an experiments/ directory.

    By default validates `config.yml` presence and that variant exposures sum to 1.
    Use `--project-path` to point at a project folder instead of running from cwd.
    Use `--strict` to run extra checks (audience.sql presence and manifest compilation).
    """
    root = Path(project_path).resolve() if project_path else Path.cwd()
    experiments = root / "experiments"
    if not experiments.exists():
        typer.echo(f"No experiments/ directory found at {root}.")
        raise typer.Exit(code=1)

    errors = []
    warnings = []
    for exp in sorted(experiments.iterdir()):
        if not exp.is_dir():
            continue
        cfg_file = exp / "config.yml"
        if not cfg_file.exists():
            errors.append(f"{exp.name}: missing config.yml")
            continue

        # Read and parse config.yml with better error reporting
        try:
            raw = cfg_file.read_text()
            cfg = yaml.safe_load(raw) or {}
        except Exception as e:
            errors.append(f"{exp.name}: error parsing config.yml: {e}")
            continue

        # Validate variants structure and exposures
        variants = cfg.get("variants") or []
        if variants:
            if not isinstance(variants, list):
                errors.append(f"{exp.name}: 'variants' must be a list")
            else:
                total = 0.0
                seen_names = set()
                for i, v in enumerate(variants):
                    if not isinstance(v, dict):
                        errors.append(f"{exp.name}: variant at index {i} must be a mapping")
                        continue
                    name = v.get("name")
                    if not name:
                        errors.append(f"{exp.name}: variant at index {i} missing 'name'")
                    else:
                        if name in seen_names:
                            errors.append(f"{exp.name}: duplicate variant name '{name}'")
                        seen_names.add(name)
                    exp_val = v.get("exposure")
                    try:
                        exp_f = float(exp_val)
                        if exp_f < 0:
                            errors.append(f"{exp.name}: variant '{name}' has negative exposure {exp_f}")
                        total += exp_f
                    except Exception:
                        errors.append(f"{exp.name}: variant '{name}' has non-numeric exposure: {exp_val}")

                # Validate sum to 1 (tolerance)
                if abs(total - 1.0) > 1e-6:
                    errors.append(f"{exp.name}: variant exposures do not sum to 1 (got {total})")

        # If requested, verify audience.sql exists and is non-empty
        aud_file = exp / "audience.sql"
        if not aud_file.exists():
            warnings.append(f"{exp.name}: missing audience.sql")
        else:
            try:
                aud_text = aud_file.read_text().strip()
                if not aud_text:
                    warnings.append(f"{exp.name}: audience.sql is empty")
            except Exception as e:
                errors.append(f"{exp.name}: could not read audience.sql: {e}")

    # In strict mode, attempt to compile manifest to catch source qualification and adapter-related issues
    if strict:
        try:
            # compile_manifest writes target/manifest.json and will surface parsing issues
            _ = compile_manifest(root)
        except Exception as e:
            errors.append(f"manifest compilation failed: {e}")

    # Validate profiles.yml and gxt_project.yml structure
    try:
        proj_yml = root / "gxt_project.yml"
        if proj_yml.exists():
            try:
                proj = yaml.safe_load(proj_yml.read_text()) or {}
            except Exception as e:
                errors.append(f"gxt_project.yml: parse error: {e}")
                proj = {}
        else:
            proj = {}

        profiles_yml = root / "profiles.yml"
        profiles = None
        if profiles_yml.exists():
            try:
                raw = profiles_yml.read_text()
                # simple env_var rendering used elsewhere in codebase
                from ..utils.profiles import load_profile as _lp

                # We won't re-implement rendering here; attempt to load the active output
                profile_name = proj.get("profile", "gxt_profile")
                profile_output = _lp(root, profile_name)
                if profile_output is None:
                    errors.append(f"profiles.yml: could not locate profile '{profile_name}' or its target output")
                else:
                    # basic checks for BigQuery profile
                    ptype = profile_output.get("type")
                    if ptype == "bigquery":
                        if not profile_output.get("project"):
                            errors.append("profiles.yml: BigQuery profile missing 'project'")
                        if not profile_output.get("dataset"):
                            errors.append("profiles.yml: BigQuery profile missing 'dataset'")
            except Exception as e:
                errors.append(f"profiles.yml: parse error: {e}")
        else:
            warnings.append("profiles.yml not found")
    except Exception:
        # do not let profile validation crash the whole validate command
        pass

    # If strict and profile indicates BigQuery, attempt a lightweight connection test
    if strict:
        try:
            # reuse profile output from above if available
            profile_name = proj.get("profile", "gxt_profile")
            from ..utils.profiles import load_profile as _lp

            profile_output = _lp(root, profile_name)
            if profile_output and profile_output.get("type") == "bigquery":
                try:
                    # lazy import of bigquery client; if not installed we'll warn
                    from google.cloud import bigquery as gbq  # type: ignore

                    client = gbq.Client(project=profile_output.get("project") or None)
                    # run a lightweight query
                    _ = list(client.query("SELECT 1 AS ok").result())
                except Exception as e:
                    errors.append(f"BigQuery connectivity test failed: {e}")
        except Exception:
            # swallow errors and report via errors list above
            pass

    # Report results
    if errors:
        typer.echo("Validation FAILED:")
        for e in errors:
            typer.echo(f" - {e}")
        if warnings:
            typer.echo("Warnings:")
            for w in warnings:
                typer.echo(f" - {w}")
        raise typer.Exit(code=2)

    if warnings:
        typer.echo("Validation OK with warnings:")
        for w in warnings:
            typer.echo(f" - {w}")
        raise typer.Exit(code=0)

    typer.echo("Validation OK")
