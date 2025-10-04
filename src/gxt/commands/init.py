"""gxt init command - scaffold a new gxt project."""
from pathlib import Path
import typer
import pkgutil
from typing import Optional
import yaml

app = typer.Typer()


@app.callback(invoke_without_command=True)
def init(
    project_path: Optional[str] = typer.Option(None, "--project-path", "-p", help="Directory to initialize the project in (default: current directory)"),
    name: str = typer.Option("gxt_project", "--name", help="Project name to write into gxt_project.yml"),
    # Note: adapter removed — warehouse/profile info lives in profiles.yml
    dataset: str = typer.Option("gxt_experiments", "--dataset", help="Default dataset/schema to write assignments to"),
    assignments_table: str = typer.Option("gxt_assignments", "--assignments-table", help="Default assignments table name"),
    version: str = typer.Option("0.1.0", "--version", help="Project version"),
):
    """Create a new gxt project scaffold.

    This command bootstraps a local gxt project by creating the following files and
    directories under the target path (default: current working directory):

        - `gxt_project.yml` (project-level configuration)
        - `experiments/` (folder for experiment subfolders)
        - `target/` (compiled artifacts)

    Options:
        --path / -p           Directory to initialize the project in (default: current dir)
        --name                Project name to write into `gxt_project.yml` (default: gxt_project)
        --adapter             Default warehouse adapter (e.g. bigquery) written to config
        --dataset             Optional default dataset/schema for assignments
        --assignments-table   Default assignments table name (default: gxt_assignments)
        --version             Project version written to `gxt_project.yml`

    Behavior:
        - If `gxt_project.yml` already exists it will be merged with the provided
            values (existing keys are preserved unless explicitly overwritten).
        - `randomization_unit` is intentionally not set at the project level; each
            individual experiment's `config.yml` should declare its own randomization unit.

    Examples:
        gxt init
        gxt init --path projects/demo --name demo --adapter bigquery --dataset demo_ds
    """

    root = Path(project_path).expanduser().resolve() if project_path else Path.cwd()
    typer.echo(f"Initializing gxt project at {root}")

    # create parent directories when path points to a nested location
    root.mkdir(parents=True, exist_ok=True)

    gxt_yml = root / "gxt_project.yml"
    experiments_dir = root / "experiments"
    target_dir = root / "target"

    experiments_dir.mkdir(exist_ok=True)
    target_dir.mkdir(exist_ok=True)

    # If gxt_project.yml already exists, merge values; otherwise create with defaults
    # Use a profile-based approach (profiles.yml) similar to dbt. We write profile: gxt_profile
    data = {
        "project_name": name,
        "version": version,
        "profile": "gxt_profile",
        "dataset": dataset,
        "assignments_table": assignments_table,
    }

    if gxt_yml.exists():
        try:
            existing = yaml.safe_load(gxt_yml.read_text()) or {}
            # remove any legacy 'adapter' key so warehouse config comes from profiles.yml
            if "adapter" in existing:
                existing.pop("adapter")
            # merge without overwriting existing keys unless user provided non-defaults
            merged = {**data, **existing}
            gxt_yml.write_text(yaml.dump(merged, sort_keys=False))
            typer.echo(f"Updated {gxt_yml} (merged with existing)")
        except Exception:
            typer.echo(f"{gxt_yml} exists but could not be parsed; leaving unchanged")
    else:
        gxt_yml.write_text(yaml.dump(data, sort_keys=False))
        typer.echo(f"Created {gxt_yml}")

    # Render profiles.yml from template into the project root
    try:
        from jinja2 import Environment, FileSystemLoader

        tmpl_dir = Path(__file__).resolve().parents[1] / "templates"
        env = Environment(loader=FileSystemLoader(str(tmpl_dir)))
        profiles_tmpl = env.get_template("profiles.yml.jinja")

        # Render profiles.yml without passing adapter (template defaults to bigquery)
        profiles_content = profiles_tmpl.render(
            env_var=lambda k, d='': "{{ env_var('%s') }}" % k
            if d == ''
            else "{{ env_var('%s','%s') }}" % (k, d)
        )
        profiles_path = root / "profiles.yml"
        profiles_path.write_text(profiles_content)
        typer.echo(f"Created {profiles_path}")
    except Exception as e:
        typer.echo(f"Could not create profiles.yml: {e}")
    typer.echo(f"Created directories: {experiments_dir}, {target_dir}")
