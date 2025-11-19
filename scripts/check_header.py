# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


import argparse
import os
from pathlib import Path


def check_header(file_path: Path, header: str):
    """
    Reads a Python file and prepends the HEADER_STRING if it's not present.

    Args:
        file_path: A Path object representing the Python file.
    """
    try:
        # Read the entire file, specify encoding for compatibility
        original_content = file_path.read_text(encoding="utf-8")

        # Check if the file already starts with the header
        if original_content.startswith(header):
            return

        # If the header is not present, create
        print(f"Updating: header in '{file_path}'")
        new_content = header + original_content

        # Write the new content back to the file
        file_path.write_text(new_content, encoding="utf-8")

    except IOError as e:
        print(f"Error: Could not read or write to file '{file_path}'. Reason: {e}")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while processing '{file_path}': {e}")
        exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Prepend a header if it doesn't exist.",
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

    header = "# © Copyright IBM Corporation 2025\n# SPDX-License-Identifier: Apache-2.0\n\n\n"

    # user specified a directory, search and apply headers
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
            check_header(file_path, header)

    # user specified a list of files, loop over these and apply headers
    else:
        for file_arg in args.files:
            file_path = Path(file_arg)
            if not file_path.is_file():
                print(f"Error: unable to find the file '{file_path}'.")
                exit(1)

            if str(file_arg).endswith(".py"):
                check_header(file_path, header)


if __name__ == "__main__":
    main()
