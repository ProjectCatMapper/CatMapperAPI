"""Exercise the actual nightly client without network calls or mail delivery."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize("marker,mail,expected", [
    ("CATMAPPER_ROUTINES_STATUS=ok", "Email sent successfully", 0),
    ("CATMAPPER_ROUTINES_STATUS=failed", "Email sent successfully", 1),
    ("", "Email sent successfully", 1),
    ("CATMAPPER_ROUTINES_STATUS=ok", "Error sending email", 1),
])
def test_wrapper_requires_successful_maintenance_even_when_mail_succeeds(tmp_path, marker, mail, expected):
    api = tmp_path / "app" / "CatMapperAPI"
    api.mkdir(parents=True)
    script = api / "routines.sh"
    shutil.copy2(Path(__file__).resolve().parents[1] / "routines.sh", script)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, content in {
        "curl": '#!/bin/sh\nprintf "%s\\n" "$TEST_STREAM"\n',
        "sendmail": '#!/bin/sh\ncat > /dev/null\n',
    }.items():
        path = bin_dir / name
        path.write_text(content)
        path.chmod(0o755)
    result = subprocess.run([str(script)], env={
        "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
        "TEST_STREAM": marker + f"\n<br><h2>Mail sent with status: {mail}</h2>\n",
        "CATMAPPER_ROUTINES_HEARTBEAT_SEC": "0.05",
    }, capture_output=True, text=True, timeout=10)
    assert result.returncode == expected, result.stdout + result.stderr
    if expected:
        assert "completed successfully" not in result.stdout
