import os
from pathlib import Path
import subprocess

import pytest


HOOK = Path(__file__).resolve().parents[1] / ".githooks" / "pre-commit"


def run_hook(tmp_path, filename, content):
    env = os.environ.copy()
    for name in list(env):
        if name.startswith("GIT_"):
            env.pop(name)
    def git(*args):
        subprocess.run(["git", *args], cwd=tmp_path, env=env, check=True, capture_output=True)
    git("init", "-q")
    path = tmp_path / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    git("add", filename)
    return subprocess.run(["bash", str(HOOK)], cwd=tmp_path, env=env, capture_output=True, text=True)


def test_hook_allows_auth_identifiers_and_synthetic_fixtures(tmp_path):
    setting = "api" + "_key"
    content = (
        f"def verify_{setting}(credential): pass\n"
        f"{setting.upper()}_AUTH_ERROR = 'Authentication failed'\n"
        f"EXAMPLE_KEY = 'cmk_replaced_key'\n"
        f"headers = {{'X-API-Key': EXAMPLE_KEY}}\n"
        f"{setting} = os.getenv('CATMAPR_API_KEY')\n"
    )
    result = run_hook(tmp_path, "auth.py", content)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("style", ["assignment", "header", "unlabeled_token"])
def test_hook_blocks_literal_api_credentials(tmp_path, style):
    setting = "api" + "_key"
    token = "cmk_" + "a" * 43
    header_name = "-".join(["X", "API", "Key"])
    content = {
        "assignment": f"{setting} = 'short-but-sensitive'\n",
        "header": f"headers = {{'{header_name}': '{token}'}}\n",
        "unlabeled_token": f"credential = '{token}'\n",
    }[style]
    result = run_hook(tmp_path, "credential.py", content)
    assert result.returncode != 0
    assert "potential secrets" in result.stderr


@pytest.mark.parametrize("parts", [
    ("CATMAPPER", "AUTH", "SECRET"), ("SECRET", "KEY"),
    ("AWS", "SECRET", "ACCESS", "KEY"), ("AWS", "ACCESS", "KEY", "ID"),
    ("MAIL", "PASSWORD"), ("NEO4J", "PASSWORD"),
])
def test_hook_keeps_other_secret_checks(tmp_path, parts):
    result = run_hook(tmp_path, "config.py", "_".join(parts) + " = 'sensitive'\n")
    assert result.returncode != 0
    assert "potential secrets" in result.stderr


@pytest.mark.parametrize("filename", [".env", "keys/private.pem", "id_rsa"])
def test_hook_keeps_sensitive_path_checks(tmp_path, filename):
    result = run_hook(tmp_path, filename, "sensitive\n")
    assert result.returncode != 0
    assert "secret-bearing patterns" in result.stderr
