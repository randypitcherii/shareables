"""Shared test configuration: adds the project root to sys.path."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
