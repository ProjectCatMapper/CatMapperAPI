"""Run operational shell scripts with fake Docker/archive/upload commands."""
import fcntl
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def executable(path, text):
    path.write_text(text)
    path.chmod(0o755)


@pytest.fixture
def maintenance(tmp_path):
    app = tmp_path / "app"
    api = app / "CatMapperAPI"
    (api / "log").mkdir(parents=True)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    events = tmp_path / "events"
    env = {"CATMAPPER_APP_ROOT": str(app),
           "CATMAPPER_RUNTIME_ENV": str(tmp_path / "no-runtime-file"),
           "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
           "EVENTS": str(events)}
    shutil.copy2(ROOT / "dbBackup.sh", api / "dbBackup.sh")
    executable(bin_dir / "docker", r"""#!/usr/bin/env python3
import os,sys
from pathlib import Path
args=sys.argv[1:]
with open(os.environ['EVENTS'],'a') as f:f.write('docker '+ ' '.join(args[:2])+'\n')
if args[0]=='stop' and os.environ.get('STOP_FAIL')==args[1]:sys.exit(4)
if args[0]=='exec' and os.environ.get('READINESS_FAIL'):sys.exit(5)
""")
    executable(bin_dir / "tar", r"""#!/usr/bin/env python3
import os,sys
from pathlib import Path
a=sys.argv[1:];out=Path(a[a.index('-cf')+1]);db=Path(a[a.index('-C')+1]).name
with open(os.environ['EVENTS'],'a') as f:f.write('archive '+db+'\n')
out.write_text('new archive')
if os.environ.get('TAR_FAIL')==db:sys.exit(9)
""")
    executable(bin_dir / "id", "#!/bin/sh\necho rjbischo\n")
    executable(bin_dir / "sleep", "#!/bin/sh\nexit 0\n")
    executable(api / "syncBackups.sh", '#!/bin/sh\necho upload >> "$EVENTS"\nexit "${SYNC_EXIT:-0}"\n')
    executable(api / "routines.sh", '#!/bin/sh\necho routines >> "$EVENTS"\nexit "${ROUTINES_EXIT:-0}"\n')

    def run(**changes):
        return subprocess.run([str(ROOT / "nightlyMaintenance.sh")], env={**env, **changes},
                              capture_output=True, text=True, timeout=10)
    return app, events, env, run


def test_backups_and_upload_finish_before_readiness_and_routines(maintenance):
    app, events, _, run = maintenance
    result = run()
    assert result.returncode == 0, result.stderr
    assert events.read_text().splitlines() == [
        event for db in ["sociomap1", "archamap1", "gisdb", "userdb"]
        for event in [f"docker stop {db}", f"archive {db}", f"docker start {db}"]
    ] + ["upload", "docker exec api", "routines"]
    assert (app / "CatMapperAPI/log/dbBackup.out").exists()
    assert (app / "CatMapperAPI/log/cronRoutines.out").exists()


def test_archive_failure_restarts_database_and_preserves_previous_backup(maintenance):
    app, events, _, run = maintenance
    backups = app / "db/sociomap1/backups"
    backups.mkdir(parents=True)
    previous = backups / "neo4j-backup.tar.zst"
    previous.write_text("previous successful backup")
    result = run(TAR_FAIL="sociomap1")
    assert result.returncode != 0 and "routines were not started" in result.stderr
    assert previous.read_text() == "previous successful backup"
    assert not list(backups.glob("*.partial.*"))
    assert events.read_text().splitlines() == ["docker stop sociomap1", "archive sociomap1", "docker start sociomap1"]


def test_failed_aws_upload_prevents_readiness_checks_and_routines(maintenance):
    _, events, _, run = maintenance
    result = run(SYNC_EXIT="42")
    assert result.returncode != 0
    assert events.read_text().splitlines()[-1] == "upload"
    assert "routines" not in events.read_text()


def test_database_stop_failure_prevents_archiving_and_routines(maintenance):
    _, events, _, run = maintenance
    result = run(STOP_FAIL="sociomap1")
    assert result.returncode != 0
    assert events.read_text().splitlines() == ["docker stop sociomap1", "docker start sociomap1"]


def test_database_readiness_failure_prevents_routines(maintenance):
    _, events, _, run = maintenance
    result = run(CATMAPPER_DATABASE_READY_TIMEOUT="0")
    assert result.returncode != 0 and "readiness checks failed" in result.stderr
    assert events.read_text().splitlines()[-1] == "upload"


def test_routine_failure_is_reported_after_a_successful_backup(maintenance):
    _, events, _, run = maintenance
    result = run(ROUTINES_EXIT="1")
    assert result.returncode != 0 and "Routines failed" in result.stderr
    assert events.read_text().splitlines()[-3:] == ["upload", "docker exec api", "routines"]


def test_overlapping_nightly_runs_are_rejected_without_running_a_backup(maintenance):
    app, events, _, run = maintenance
    with (app / "CatMapperAPI/log/nightlyMaintenance.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = run()
    assert result.returncode == 75
    assert not events.exists()


@pytest.mark.parametrize("selection,container", [("SocioMap", "sociomap1"), ("ArchaMap", "archamap1"), ("gisdb", "gisdb"), ("userdb", "userdb")])
def test_selected_database_backup_does_not_stop_unselected_databases(maintenance, selection, container):
    _, events, env, _ = maintenance
    result = subprocess.run([str(ROOT / "dbBackup.sh"), selection], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert events.read_text().splitlines() == [f"docker stop {container}", f"archive {container}", f"docker start {container}", "upload"]
