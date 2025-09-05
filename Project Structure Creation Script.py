# Project Structure Creation Script
# Run this to create the initial directory structure

import os


def create_directory_structure():
    """Create the project directory structure"""

    # Main directories
    directories = [
        "src",
        "src/core",
        "src/kiwoom",
        "src/gui",
        "src/utils",
        "config",
        "data",
        "logs",
        "tests",
        "tests/unit",
        "tests/integration"
    ]

    # Create directories
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

    # Create __init__.py files for Python packages
    init_files = [
        "src/__init__.py",
        "src/core/__init__.py",
        "src/kiwoom/__init__.py",
        "src/gui/__init__.py",
        "src/utils/__init__.py",
        "tests/__init__.py",
        "tests/unit/__init__.py",
        "tests/integration/__init__.py"
    ]

    for init_file in init_files:
        with open(init_file, 'w') as f:
            f.write('# -*- coding: utf-8 -*-\n')
        print(f"Created: {init_file}")


if __name__ == "__main__":
    create_directory_structure()
    print("Project structure created successfully!")