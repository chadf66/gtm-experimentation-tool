"""gxt list command - list experiments and metadata."""
from pathlib import Path
import typer
import yaml
from typing import Optional

app = typer.Typer()


@app.callback(invoke_without_command=True)
def list_cmd(
    status: Optional[str] = typer.Option(None, help="Filter by status (active|inactive|archived)"),
    project_path: Optional[str] = typer.Option(None, "--project-path", "-p", help="Project root path where the experiments/ folder lives")
):
    """List experiments in the `experiments/` directory with brief metadata."""
    # choose project root: provided project_path or current working dir
    root = Path(project_path).resolve() if project_path else Path.cwd()
    experiments_dir = root / "experiments"

    if not experiments_dir.exists():
        typer.echo("No experiments/ directory found.")
        raise typer.Exit(code=1)

    rows = []
    for exp in sorted(experiments_dir.iterdir()):
        if not exp.is_dir():
            continue
        cfg_file = exp / "config.yml"
        meta = {
            "name": exp.name,
            "status": "<missing>",
            "randomization_unit": "-",
            "variants": [],
            "groups": [],
        }
        if cfg_file.exists():
            try:
                cfg = yaml.safe_load(cfg_file.read_text()) or {}
                meta["status"] = cfg.get("status", meta["status"]) or meta["status"]
                meta["randomization_unit"] = cfg.get("randomization_unit", meta["randomization_unit"]) or meta["randomization_unit"]
                meta["variants"] = cfg.get("variants", []) or []
                meta["groups"] = cfg.get("groups", []) or []
            except Exception as e:
                meta["status"] = f"error: {e}"

        if status and meta["status"] != status:
            continue

        # format variant summary
        if meta["variants"]:
            var_summary = ", ".join([f"{v.get('name')}({v.get('exposure')})" for v in meta["variants"]])
        else:
            var_summary = "-"

        rows.append((meta["name"], meta["status"], meta["randomization_unit"], var_summary, len(meta["groups"]) if meta["groups"] else 0))

    if not rows:
        typer.echo("No experiments found.")
        return

    # print simple table
    header = ("Experiment", "Status", "Unit", "Variants (exposure)", "#groups")
    typer.echo("\t".join(header))
    for r in rows:
        typer.echo("\t".join([str(x) for x in r]))
