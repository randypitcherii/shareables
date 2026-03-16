#!/usr/bin/env python3
"""
Create test fixtures for dependency_processor.py validation.

Generates various zip file formats to test all processing strategies:
1. requirements_only.zip - Just a requirements.txt
2. pure_python.zip - Python files without packaging metadata
3. packaged.zip - Proper Python package with pyproject.toml
4. existing_wheels.zip - Pre-built wheel files
5. mixed.zip - requirements.txt + custom code
6. nested.zip - Dependencies in subdirectory
"""
import os
import zipfile
import tempfile
import shutil
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "test_artifacts"


def create_requirements_only():
    """Create zip with just requirements.txt."""
    output = FIXTURES_DIR / "requirements_only.zip"
    with zipfile.ZipFile(output, 'w') as zf:
        # Simple requirements file
        requirements = """# Test requirements
requests>=2.28.0
python-dateutil>=2.8.0
"""
        zf.writestr("requirements.txt", requirements)
    print(f"Created: {output}")


def create_pure_python():
    """Create zip with pure Python modules (no packaging)."""
    output = FIXTURES_DIR / "pure_python.zip"
    with zipfile.ZipFile(output, 'w') as zf:
        # Simple module
        zf.writestr("myutils/__init__.py", "from .helpers import greet\n")
        zf.writestr("myutils/helpers.py", '''"""Helper functions."""

def greet(name: str) -> str:
    """Return a greeting."""
    return f"Hello, {name}!"

def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b
''')
        # Standalone script
        zf.writestr("calculator.py", '''"""Simple calculator module."""

def multiply(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b

def divide(a: float, b: float) -> float:
    """Divide two numbers."""
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b
''')
    print(f"Created: {output}")


def create_packaged():
    """Create zip with proper Python package structure."""
    output = FIXTURES_DIR / "packaged.zip"
    with zipfile.ZipFile(output, 'w') as zf:
        # pyproject.toml
        zf.writestr("pyproject.toml", '''[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "testpkg"
version = "1.0.0"
description = "A test package"
requires-python = ">=3.9"

[tool.setuptools.packages.find]
where = ["src"]
''')
        # Package source
        zf.writestr("src/testpkg/__init__.py", '''"""Test package."""
__version__ = "1.0.0"
from .core import process_data
''')
        zf.writestr("src/testpkg/core.py", '''"""Core functionality."""

def process_data(data: list) -> dict:
    """Process a list of data."""
    return {
        "count": len(data),
        "sum": sum(data) if data and isinstance(data[0], (int, float)) else None,
        "items": data
    }
''')
    print(f"Created: {output}")


def create_existing_wheels():
    """
    Create zip with pre-built wheel files.
    For testing, we'll create a minimal valid wheel structure.
    """
    output = FIXTURES_DIR / "existing_wheels.zip"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create a minimal wheel (valid wheel format)
        wheel_name = "simplepkg-0.1.0-py3-none-any.whl"
        wheel_path = tmpdir / wheel_name

        with zipfile.ZipFile(wheel_path, 'w') as whl:
            # Package content
            whl.writestr("simplepkg/__init__.py", '''"""Simple package."""
__version__ = "0.1.0"

def hello():
    return "Hello from simplepkg!"
''')
            # METADATA (required)
            whl.writestr("simplepkg-0.1.0.dist-info/METADATA", '''Metadata-Version: 2.1
Name: simplepkg
Version: 0.1.0
Summary: A simple test package
''')
            # WHEEL (required)
            whl.writestr("simplepkg-0.1.0.dist-info/WHEEL", '''Wheel-Version: 1.0
Generator: test
Root-Is-Purelib: true
Tag: py3-none-any
''')
            # RECORD (required, can be minimal)
            whl.writestr("simplepkg-0.1.0.dist-info/RECORD", '''simplepkg/__init__.py,,
simplepkg-0.1.0.dist-info/METADATA,,
simplepkg-0.1.0.dist-info/WHEEL,,
simplepkg-0.1.0.dist-info/RECORD,,
''')

        # Create output zip containing the wheel
        with zipfile.ZipFile(output, 'w') as zf:
            zf.write(wheel_path, wheel_name)

    print(f"Created: {output}")


def create_mixed():
    """Create zip with requirements.txt AND custom code."""
    output = FIXTURES_DIR / "mixed.zip"
    with zipfile.ZipFile(output, 'w') as zf:
        # Requirements for external deps
        zf.writestr("requirements.txt", """# External dependencies
urllib3>=2.0.0
""")
        # Custom package with pyproject.toml
        zf.writestr("pyproject.toml", '''[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "myapp"
version = "0.2.0"
description = "My application package"
requires-python = ">=3.9"
''')
        zf.writestr("myapp/__init__.py", '''"""My application."""
__version__ = "0.2.0"
''')
        zf.writestr("myapp/api.py", '''"""API module."""
import urllib3

def fetch_status(url: str) -> int:
    """Fetch HTTP status code."""
    http = urllib3.PoolManager()
    response = http.request("HEAD", url)
    return response.status
''')
    print(f"Created: {output}")


def create_nested_structure():
    """Create zip where content is inside a subdirectory (common pattern)."""
    output = FIXTURES_DIR / "nested.zip"
    with zipfile.ZipFile(output, 'w') as zf:
        # Everything under a subdirectory
        prefix = "myproject-v1.0/"
        zf.writestr(f"{prefix}requirements.txt", "certifi>=2023.0.0\n")
        zf.writestr(f"{prefix}myproject/__init__.py", "__version__ = '1.0'\n")
        zf.writestr(f"{prefix}myproject/utils.py", '''def format_name(name):
    return name.title()
''')
    print(f"Created: {output}")


def main():
    """Create all test fixtures."""
    FIXTURES_DIR.mkdir(exist_ok=True)

    print("Creating test fixtures...")
    print("-" * 40)

    create_requirements_only()
    create_pure_python()
    create_packaged()
    create_existing_wheels()
    create_mixed()
    create_nested_structure()

    print("-" * 40)
    print(f"All fixtures created in: {FIXTURES_DIR}")

    # List contents
    print("\nFixture contents:")
    for f in sorted(FIXTURES_DIR.glob("*.zip")):
        with zipfile.ZipFile(f) as zf:
            print(f"\n{f.name}:")
            for name in zf.namelist():
                print(f"  {name}")


if __name__ == "__main__":
    main()
