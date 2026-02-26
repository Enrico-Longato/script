"""
Launcher script: execute every Python file in this folder in alphabetical order.

Intended to be run from within the `script` directory or via the project root.
It discovers all `*.py` files in the same directory, sorts them by name, and
invokes each one as a separate Python process.  The launcher itself is ignored.

Usage:
    python run_all_scripts.py [--exclude name1,name2,...]

Options:
    --exclude: comma-separated list of basenames (without .py) to skip.

This provides a simple way to replay the pipeline by name order.
"""

import sys
import subprocess
from pathlib import Path


def main():
    base = Path(__file__).resolve().parent
    py_files = sorted(p for p in base.glob("*.py") if p.name != Path(__file__).name)

    exclusions = set()
    if "--exclude" in sys.argv:
        idx = sys.argv.index("--exclude")
        if idx + 1 < len(sys.argv):
            exclusions = set(name.strip() for name in sys.argv[idx + 1].split(",") if name.strip())

    print(f"Launcher directory: {base}")
    print(f"Found {len(py_files)} python files")

    for script in py_files:
        stem = script.stem
        if stem in exclusions:
            print(f"Skipping excluded script: {script.name}")
            continue
        print("\n" + "=" * 60)
        print(f"Running {script.name}")
        try:
            result = subprocess.run([sys.executable, str(script)], check=True)
            print(f"{script.name} finished with return code {result.returncode}")
        except subprocess.CalledProcessError as e:
            print(f"Error executing {script.name}: return code {e.returncode}")
            sys.exit(e.returncode)


if __name__ == "__main__":
    main()
