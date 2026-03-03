"""
Launcher script: execute Python files in this folder in alphabetical order.

It discovers all `*.py` files in the same directory (excluding itself),
sorts them by name, and allows the user to:

- Run all scripts
- Manually select which scripts to run

Usage:
    python run_all_scripts.py [--exclude name1,name2,...]

Options:
    --exclude: comma-separated list of basenames (without .py) to skip.
"""

import sys
import subprocess
from pathlib import Path


def main():
    base = Path(__file__).resolve().parent
    py_files = sorted(
        p for p in base.glob("*.py")
        if p.name != Path(__file__).name
    )

    # Handle exclusions from CLI
    exclusions = set()
    if "--exclude" in sys.argv:
        idx = sys.argv.index("--exclude")
        if idx + 1 < len(sys.argv):
            exclusions = {
                name.strip()
                for name in sys.argv[idx + 1].split(",")
                if name.strip()
            }

    # Filter excluded scripts
    py_files = [p for p in py_files if p.stem not in exclusions]

    print(f"\nLauncher directory: {base}")
    print(f"Found {len(py_files)} Python files:\n")

    if not py_files:
        print("No scripts available to run.")
        sys.exit(0)

    # Show numbered list
    for i, script in enumerate(py_files, start=1):
        print(f"{i}. {script.name}")

    # Interactive selection
    choice = input("\nDo you want to run ALL scripts? (y/n): ").strip().lower()

    selected_scripts = py_files

    if choice == "n":
        numbers = input(
            "Enter the numbers of the scripts to run (comma-separated, e.g. 1,3,5): "
        ).strip()

        try:
            indexes = {int(n.strip()) for n in numbers.split(",")}
            selected_scripts = [
                script for i, script in enumerate(py_files, start=1)
                if i in indexes
            ]

            if not selected_scripts:
                print("No valid scripts selected. Exiting.")
                sys.exit(1)

        except ValueError:
            print("Invalid input. Exiting.")
            sys.exit(1)

    print("\nStarting execution...")

    # Execute selected scripts
    for script in selected_scripts:
        print("\n" + "=" * 60)
        print(f"Running {script.name}")

        try:
            result = subprocess.run(
                [sys.executable, str(script)],
                check=True
            )
            print(f"{script.name} finished with return code {result.returncode}")

        except subprocess.CalledProcessError as e:
            print(f"Error executing {script.name}: return code {e.returncode}")
            sys.exit(e.returncode)


if __name__ == "__main__":
    main()