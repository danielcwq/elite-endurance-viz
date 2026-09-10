#!/usr/bin/env python3
"""Portable developer entry points; never rebuild or overwrite snapshot data."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV_PYTHON = ROOT / '.venv' / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')


def run(command, **kwargs):
    subprocess.run([str(part) for part in command], cwd=ROOT, check=True, **kwargs)


def verify_python():
    if not VENV_PYTHON.is_file():
        raise RuntimeError('Local .venv is missing. Run make setup first.')
    run([VENV_PYTHON, '-c',
         'import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else "This project requires Python 3.12; the existing .venv was left intact.")'])


def setup():
    uv = shutil.which('uv')
    if not VENV_PYTHON.is_file():
        if (ROOT / '.venv').exists():
            raise RuntimeError('An incomplete .venv already exists. Inspect it before replacing it; setup did not modify it.')
        if uv:
            run([uv, 'venv', '--python', '3.12', ROOT / '.venv'])
        else:
            python = sys.executable if sys.version_info[:2] == (3, 12) else shutil.which('python3.12')
            if not python:
                raise RuntimeError('Install uv or Python 3.12, then rerun make setup.')
            run([python, '-m', 'venv', ROOT / '.venv'])
    verify_python()
    if uv:
        run([uv, 'pip', 'install', '--python', VENV_PYTHON, '-r', 'requirements-analysis.txt'])
    else:
        run([VENV_PYTHON, '-m', 'pip', 'install', '-r', 'requirements-analysis.txt'])
    print('Setup complete. make test runs tests; make check also validates the packaged snapshot.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('setup', 'test', 'check', 'run', 'seed', 'demo'))
    args = parser.parse_args()
    try:
        if args.command == 'setup':
            setup()
        else:
            verify_python()
            if args.command in ('test', 'check'):
                run([VENV_PYTHON, '-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test_*.py'])
                if args.command == 'check':
                    run([VENV_PYTHON, 'scripts/check_serving_2024.py'])
            elif args.command == 'seed':
                run([VENV_PYTHON, 'scripts/build_demo_2024.py'])
            elif args.command == 'demo':
                database = ROOT / 'data/derived/2024/demo/synthetic.duckdb'
                if not os.path.lexists(database):
                    run([VENV_PYTHON, 'scripts/build_demo_2024.py'])
                # Require the explicit synthetic marker before opening demo mode.
                run([VENV_PYTHON, '-c',
                     'import duckdb,sys; c=duckdb.connect(sys.argv[1],read_only=True); '
                     'v=c.execute("SELECT specification_version FROM dataset_builds").fetchall(); '
                     'sys.exit(0 if v==[("SYNTHETIC-DEMO-v1",)] else "Not a synthetic demo database")', database])
                environment = os.environ.copy()
                environment['ENDURANCEVIZ_DB_PATH'] = str(database)
                run([VENV_PYTHON, '-m', 'uvicorn', 'main:app', '--host', '127.0.0.1',
                     '--port', '8001', '--no-access-log'], env=environment)
            else:
                run([VENV_PYTHON, '-m', 'uvicorn', 'main:app', '--host', '127.0.0.1', '--port', '8000', '--no-access-log'])
    except (RuntimeError, OSError, subprocess.CalledProcessError) as exc:
        print(f'error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
