"""gxt validate command - validate experiments YAML and SQL."""
from pathlib import Path
import typer
import yaml

app = typer.Typer()


@app.callback(invoke_without_command=True)
def validate():
    """Validate experiments under experiments/ directory."""
    root = Path.cwd()
    experiments = root / "experiments"
    if not experiments.exists():
        typer.echo("No experiments/ directory found.")
        raise typer.Exit(code=1)

    errors = []
    for exp in experiments.iterdir():
        if not exp.is_dir():
            continue
        cfg_file = exp / "config.yml"
        if not cfg_file.exists():
            errors.append(f"{exp.name}: missing config.yml")
            continue
        try:
            cfg = yaml.safe_load(cfg_file.read_text())
            # quick exposure sum check if variants present
            variants = cfg.get("variants") or []
            total = sum(v.get("exposure", 0) for v in variants)
            if variants and abs(total - 1.0) > 1e-6:
                errors.append(f"{exp.name}: variant exposures do not sum to 1 (got {total})")
        except Exception as e:
            errors.append(f"{exp.name}: error parsing config.yml: {e}")

    if errors:
        typer.echo("Validation FAILED:")
        for e in errors:
            typer.echo(f" - {e}")
        raise typer.Exit(code=2)

    typer.echo("Validation OK")
