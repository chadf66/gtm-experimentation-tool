"""gxt CLI entrypoint."""
import json
from pathlib import Path
import typer
from typing import Optional

app = typer.Typer(help="GTM Experimentation Tool (gxt) - CLI")

# All command modules as Typer sub-apps for consistent structure
from .commands import compile as compile_cmd
from .commands import validate as validate_cmd
from .commands import init as init_cmd
from .commands import new_experiment as new_experiment_cmd
from .commands import list as list_cmd
from .commands import run as run_cmd

# Mount each command module's Typer app

# gxt init
app.add_typer(init_cmd.app, name="init")

# gxt new-experiment
app.add_typer(new_experiment_cmd.app, name="new-experiment")

# gxt compile
app.add_typer(compile_cmd.app, name="compile")

# gxt validate
app.add_typer(validate_cmd.app, name="validate")

# gxt list
app.add_typer(list_cmd.app, name="list")

# gxt run
from .commands.run import run as run_command

# Register run as a direct command so positional arguments parse reliably when
# invoking via `python -m src.gxt.main run ...`.
app.command(name="run")(run_command)


@app.command()
def version():
    """Show gxt version info."""
    typer.echo("gxt 0.1.0")


if __name__ == "__main__":
    app()
