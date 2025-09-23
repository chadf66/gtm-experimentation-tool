"""Manifest compilation utilities."""
from pathlib import Path
import yaml
import json
from typing import Optional
import re

# Avoid importing adapters here to keep compile_manifest adapter-agnostic.


def compile_manifest(root: Path = None, adapter: Optional[object] = None) -> dict:
    """Scan experiments/ and build a simple manifest JSON structure."""
    root = root or Path.cwd()
    experiments_dir = root / "experiments"
    target_dir = root / "target"
    manifest = {"experiments": {}}

    if not experiments_dir.exists():
        return manifest

    for exp in experiments_dir.iterdir():
        if not exp.is_dir():
            continue
        cfg_file = exp / "config.yml"
        manifest["experiments"][exp.name] = {"path": str(exp)}
        if cfg_file.exists():
            try:
                cfg = yaml.safe_load(cfg_file.read_text())
                manifest["experiments"][exp.name]["config"] = cfg
                # also read audience.sql and optionally qualify sources
                aud_file = exp / "audience.sql"
                if aud_file.exists():
                    aud_sql = aud_file.read_text()
                    if adapter is not None and hasattr(adapter, "qualify_table"):
                        aud_sql = _qualify_sources_in_sql(aud_sql, adapter)
                    manifest["experiments"][exp.name]["audience_sql"] = aud_sql
                # If an adapter was provided, include an example hash SQL snippet
                try:
                    if adapter is not None and hasattr(adapter, "hash_bucket_sql"):
                        # Use randomization_unit from config if present, else default to 'user_id'
                        ru = cfg.get("randomization_unit") if isinstance(cfg, dict) else None
                        ru = ru or "user_id"
                        manifest["experiments"][exp.name]["hash_sql_example"] = adapter.hash_bucket_sql(ru)
                except Exception:
                    # ignore adapter failures while compiling manifest
                    manifest["experiments"][exp.name]["hash_sql_example"] = None
            except Exception:
                manifest["experiments"][exp.name]["config"] = None

    target_dir.mkdir(exist_ok=True)
    out = target_dir / "manifest.json"
    out.write_text(json.dumps(manifest, indent=2))
    return manifest


def _qualify_sources_in_sql(sql: str, adapter) -> str:
    r"""Replace dbt-style source('dataset','table') calls with adapter-qualified identifiers.

    This uses a simple regex to find source\(\s*'dataset'\s*,\s*'table'\s*\) patterns.
    """
    # matches either literal source('dataset','table') or Jinja-style {{ source('dataset','table') }}
    # Accepts single or double quotes and flexible whitespace inside.
    literal_pattern = re.compile(r"(?i)\bsource\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)")

    jinja_pattern = re.compile(
        r"(?i)\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
    )

    def _replace_match(m):
        dataset = m.group(1)
        table = m.group(2)
        try:
            return adapter.qualify_table(dataset, table)
        except Exception:
            return f"{dataset}.{table}"

    # First replace Jinja-style occurrences, then literal occurrences to cover both cases.
    sql = jinja_pattern.sub(_replace_match, sql)
    sql = literal_pattern.sub(_replace_match, sql)
    return sql
