"""gxt compile command - compile experiments into target/manifest.json"""
from pathlib import Path
import typer
from typing import Optional

from ..parser.manifest import compile_manifest
from ..adapters.bigquery import BigQueryAdapter
from ..utils.profiles import load_profile
import yaml

app = typer.Typer()


@app.callback(invoke_without_command=True)
def compile_cmd(
    project_path: Optional[str] = typer.Option(None, "--project-path", "-p", help="Project root path to compile (defaults to current working dir)"),
    adapter: Optional[str] = typer.Option(None, help="Adapter to use for SQL snippets, e.g. 'bigquery'"),
):
    """Compile experiments into `target/manifest.json` under the given project path."""
    root = Path(project_path).resolve() if project_path else Path.cwd()
    adapter_obj = None
    if adapter:
        adapter = adapter.lower()
        if adapter == "bigquery":
            adapter_obj = BigQueryAdapter()
        else:
            typer.echo(f"Unknown adapter: {adapter}. Proceeding without adapter.")
    else:
        # Attempt to load adapter from project gxt_project.yml -> profiles.yml
        gxt_yml = root / "gxt_project.yml"
        if gxt_yml.exists():
            try:
                proj = yaml.safe_load(gxt_yml.read_text()) or {}
                profile_name = proj.get("profile", "gxt_profile")
                profile_output = load_profile(root, profile_name)
                if profile_output and profile_output.get("type") == "bigquery":
                    adapter_obj = BigQueryAdapter.from_profile(profile_output)
            except Exception:
                pass

    manifest = compile_manifest(root, adapter=adapter_obj)
    typer.echo(f"Compiled {len(manifest.get('experiments', {}))} experiments into {root / 'target' / 'manifest.json'}")
