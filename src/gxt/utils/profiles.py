"""Profiles loader for gxt.

This provides a tiny helper to read a dbt-style `profiles.yml` and return the
active output configuration for a given profile name.
"""
from pathlib import Path
import yaml
from typing import Optional, Dict
import os
import re


def load_profile(root: Path, profile_name: str = "gxt_profile") -> Optional[Dict]:
    """Load profiles.yml from the given project root and return the active output dict.

    Returns the output config (e.g. profiles[profile]['outputs'][target]) or None if not found.
    """
    profiles_path = root / "profiles.yml"
    if not profiles_path.exists():
        return None
    try:
        raw = profiles_path.read_text()
        # simple rendering for {{ env_var('FOO') }} and {{ env_var('FOO','default') }}
        def _replace_env_var(match):
            key = match.group(1)
            default = match.group(2) if match.group(2) is not None else ""
            return os.environ.get(key, default)

        rendered = re.sub(r"\{\{\s*env_var\(\s*'([^']+)'\s*(?:,\s*'([^']*)')?\s*\)\s*\}\}", _replace_env_var, raw)
        data = yaml.safe_load(rendered) or {}
        profile = data.get(profile_name)
        if not profile:
            return None
        target = profile.get("target")
        outputs = profile.get("outputs", {})
        output = outputs.get(target)
        if not isinstance(output, dict):
            return None

        # Normalize keys: Snowflake uses 'schema', BigQuery uses 'dataset'
        if "schema" in output and "dataset" not in output:
            output["dataset"] = output.get("schema")

        return output
    except Exception:
        return None
