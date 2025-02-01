#!/bin/bash

rm -rf `find . -name __pycache__`
rm -f `find . -type f -name '*.py[co]'`
rm -f `find . -type f -name '*~'`
rm -f `find . -type f -name '.*~'`
rm -rf .cache
rm -rf .mypy_cache
rm -rf .venv
rm -rf .pytest_cache
rm -rf .ruff_cache
rm -rf .ipynb_checkpoints
rm -rf htmlcov
rm -rf *.egg-info
rm -f .coverage
rm -f .coverage.*