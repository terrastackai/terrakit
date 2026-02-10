# © Copyright IBM Corporation 2025-2026
# SPDX-License-Identifier: Apache-2.0


import argparse
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path


def get_current_year():
    """Get the current year."""
    return datetime.now().year


def extract_copyright_year(content: str):
    """Extract year(s) from existing copyright header."""
    match = re.search(r"© Copyright IBM Corporation (\d{4}(?:-\d{4})?)", content)
    return match.group(1) if match else None


def is_new_file(file_path: Path):
    """Check if file is new (not in git history)."""
    try:
        result = subprocess.run(
            ["git", "log", "--oneline", str(file_path)],
            capture_output=True,
            text=True,
            cwd=file_path.parent,
        )
        return len(result.stdout.strip()) == 0
    except Exception:
        return True  # Assume new if git check fails


def get_file_creation_year(file_path: Path):
    """Get the year when file was first committed to git."""
    try:
        result = subprocess.run(
            [
                "git",
                "log",
                "--follow",
                "--format=%ad",
                "--date=format:%Y",
                "--",
                str(file_path),
            ],
            capture_output=True,
            text=True,
            cwd=file_path.parent,
        )
        years = result.stdout.strip().split("\n")
        if years and years[-1]:  # Get oldest commit year
            return int(years[-1])
        return None
    except Exception:
        return None


def generate_copyright_year(file_path: Path, existing_year: str | None = None):
    """Generate appropriate copyright year string."""
    current_year = get_current_year()

    # If no existing year in file, check git history
    if existing_year is None:
        creation_year = get_file_creation_year(file_path)
        if creation_year is None:
            # File not in git yet - use current year
            return str(current_year)
        elif creation_year == current_year:
            return str(current_year)
        else:
            return f"{creation_year}-{current_year}"

    # Parse existing year(s) from file
    if "-" in existing_year:
        start_year, end_year = existing_year.split("-")
        if int(end_year) == current_year:
            return existing_year  # Already up to date
        return f"{start_year}-{current_year}"
    else:
        if int(existing_year) == current_year:
            return existing_year
        return f"{existing_year}-{current_year}"


def check_header(file_path: Path):
    """
    Reads a Python file and updates/adds copyright header with appropriate year.

    Args:
        file_path: A Path object representing the Python file.
    """
    try:
        # Read the entire file
        original_content = file_path.read_text(encoding="utf-8")

        # Extract existing copyright year if present
        existing_year = extract_copyright_year(original_content)

        # Generate appropriate year string
        copyright_year = generate_copyright_year(file_path, existing_year)

        # Create header with appropriate year
        header = f"# © Copyright IBM Corporation {copyright_year}\n# SPDX-License-Identifier: Apache-2.0\n\n\n"

        # Check if header needs updating
        if original_content.startswith(header):
            return

        # Remove old header if present
        if existing_year:
            # Remove old copyright lines
            lines = original_content.split("\n")
            new_lines = []
            skip_count = 0
            for line in lines:
                if skip_count < 4 and (
                    line.startswith("# ©")
                    or line.startswith("# SPDX")
                    or line.strip() == "#"
                    or line.strip() == ""
                ):
                    skip_count += 1
                    continue
                new_lines.append(line)
            original_content = "\n".join(new_lines)

        # Add new header
        print(f"Updating: header in '{file_path}' with year {copyright_year}")
        new_content = header + original_content

        # Write back to file
        file_path.write_text(new_content, encoding="utf-8")

    except IOError as e:
        print(f"Error: Could not read or write to file '{file_path}'. Reason: {e}")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while processing '{file_path}': {e}")
        exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Prepend/update copyright header with appropriate year.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-d",
        "--directory",
        nargs="?",
        default=os.getcwd(),
        help="The target directory to scan for .py files.\nDefaults to the current working directory if not provided.",
    )
    group.add_argument(
        "-f",
        "--files",
        nargs="*",
        help="A list of python files to scan",
    )
    args = parser.parse_args()

    # User specified a directory, search and apply headers
    if args.files is None:
        target_directory = Path(args.directory)

        if not target_directory.is_dir():
            print(f"Error: The path '{target_directory}' is not a valid directory.")
            exit(1)

        files = [
            path for path in target_directory.rglob("*.py") if ".venv" not in str(path)
        ]

        if not files:
            print("No Python files (.py) found in the specified directory.")
            exit(1)

        for file_path in files:
            check_header(file_path)

    # User specified a list of files, loop over these and apply headers
    else:
        for file_arg in args.files:
            file_path = Path(file_arg)
            if not file_path.is_file():
                print(f"Error: unable to find the file '{file_path}'.")
                exit(1)

            if str(file_arg).endswith(".py"):
                check_header(file_path)


if __name__ == "__main__":
    main()
