"""Profiles loader for gxt.

This provides a tiny helper to read a dbt-style `profiles.yml` and return the
active output configuration for a given profile name.
"""
from pathlib import Path
import yaml
from typing import Optional, Dict
import os
import re
from dotenv import load_dotenv 

def load_profile(root: Path, profile_name: str = "gxt_profile") -> Optional[Dict]:
    """Load profiles.yml from the given project root and return the active output dict.

    Returns the output config (e.g. profiles[profile]['outputs'][target]) or None if not found.
    """
    profiles_path = root / "profiles.yml"
    if not profiles_path.exists():
        return None
    try:
        # If a .env file exists at the project root, load its values into the
        # environment for the purpose of rendering profiles.yml. We only set
        # variables that are not already present in os.environ so we don't
        # override user environment.
        # Require python-dotenv for parsing .env files. This simplifies parsing
        # and makes behavior consistent. If python-dotenv is not installed,
        # raise a clear error instructing users to install it.

        env_path = str(root / ".env")
        # load_dotenv will not override existing env vars by default
        load_dotenv(env_path)

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
