"""gxt new-experiment command."""
from pathlib import Path
from typing import Optional
import typer
from jinja2 import Environment, FileSystemLoader

app = typer.Typer()

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"


@app.callback(invoke_without_command=True)
def new_experiment(
    name: str = typer.Option(..., "--name", "-n", help="Experiment name (directory under experiments/)"),
    project_path: Optional[str] = typer.Option(
        None, "--project-path", "-p", help="Project root path where the experiments/ folder lives"
    ),
    description: str = "",
    start_date: str = "",
    end_date: str = "",
    owner: str = "",
    tags: str = "",
    randomization_unit: str = typer.Option(
        ..., "--randomization-unit", help="Randomization unit for this experiment (required)"
    ),
):
    """Create a new experiment scaffold under experiments/<name>/

    The new optional fields are:
    - description: free-text experiment description
    - start_date / end_date: schedule window (ISO date strings encouraged)
    - owner: person or team responsible
    - tags: comma-separated labels (e.g. "email,sales")
    """
    # choose project root: provided project_path or current working dir
    root = Path(project_path).resolve() if project_path else Path.cwd()
    exp_dir = root / "experiments" / name
    exp_dir.mkdir(parents=True, exist_ok=True)

    # Copy the audience.sql template verbatim so Jinja-style markers like
    # {{ source('dataset','table') }} are preserved for the compiler to replace later.
    audience_path = TEMPLATE_DIR / "audience.sql"
    audience_sql = audience_path.read_text()
    (exp_dir / "audience.sql").write_text(audience_sql)

    # Prepare Jinja environment for rendering config.yml (we don't render audience.sql)
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    # config.yml
    cfg_tmpl = env.get_template("config.yml.jinja")
    # convert comma-separated tags into a list for the template
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
    # attempt to read default assignments_table from gxt_project.yml
    assignments_table = ""
    gxt_yml = root / "gxt_project.yml"
    if gxt_yml.exists():
        try:
            import yaml

            proj = yaml.safe_load(gxt_yml.read_text()) or {}
            if proj.get("assignments_table"):
                assignments_table = proj.get("assignments_table")
            elif proj.get("dataset"):
                # Use project dataset with default table name if no explicit assignments_table
                assignments_table = f"{proj.get('dataset')}.gxt_assignments"
        except Exception:
            assignments_table = ""
    else:
        typer.echo(f"Warning: no gxt_project.yml found at {root}; assignments_table will be empty in config.yml")

    cfg = cfg_tmpl.render(
        experiment_id=name,
        description=description,
        start_date=start_date,
        end_date=end_date,
        owner=owner,
        tags=tag_list,
        randomization_unit=randomization_unit,
        assignments_table=assignments_table,
    )
    (exp_dir / "config.yml").write_text(cfg)

    typer.echo(f"Created experiment scaffold at {exp_dir}")
