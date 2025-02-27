#!/bin/bash

rm -rf `find . -name __pycache__`
rm -f `find . -type f -name '*.py[co]'`
rm -f `find . -type f -name '*~'`
rm -f `find . -type f -name '.*~'`
rm -f `find . -type f -name '*.log'`
rm -rf `find . -type d -name '.ipynb_checkpoints'`
rm -rf `find . -type d -name '*.egg-info'`
rm -rf .cache
rm -rf dist
rm -rf .mypy_cache
rm -rf .venv
rm -rf .pytest_cache
rm -rf .ruff_cache
rm -rf htmlcov
rm -f .coverage
rm -f .coverage.*