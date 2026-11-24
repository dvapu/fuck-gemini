"""Evaluation test runner for v3 enhancements."""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tests.test_v3_enhancements import run_all_tests

if __name__ == "__main__":
    run_all_tests()
