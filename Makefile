.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test # remove all build, test, coverage and Python artifacts

clean-build: # remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: # remove Python file artifacts
	find . -name '*.DS_Store' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '.ipynb_checkpoints' -exec rm -fr {} +

clean-test: # remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: # check style with flake8
	flake8 pelopt tests

test: # run tests quickly with the default Python
	pytest

test-all: # run tests on every Python version with tox
	tox

coverage: # check code coverage quickly with the default Python
	coverage run --source pelopt -m pytest
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

docs: # generate Sphinx HTML documentation, including API docs
	$(sphinx-apidoc) -o docs/pelopt
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/html/index.html

servedocs: docs # compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: dist # package and upload a release
	twine upload dist/*

dist: clean # builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

install: # install the package to the active Python's site-packages
	python setup.py install
	make set-env

install-dev: # install the package to the active Python's site-packages
	pip install -e .
	make set-env

build-reqs: # builds requirements.txt (Observation: requires pipreqs)
	pipreqs ./src/pelopt
	mv  ./src/pelopt/requirements.txt   ./requirements_dev.txt

set-env:
	$ dotenv set PROJECT_DIR $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
	$ dotenv set SRC_DIR $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/src/pelopt
	@echo Environment variables set successfully

clear-env:
	$(PYTHON_INTERPRETER) clear_environment.py

update_requirements:
	poetry export --without-hashes --ansi -o ./requirements.txt
