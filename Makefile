.PHONY: test

configure:  # does any pre-requisite installs
	pip install poetry

build:  # builds
	make configure
	poetry install

update:
	poetry update

test:
	pytest -vv --cov chalicelib tests -m not integratedx

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make build' to install dependencies using poetry.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
	   $(info - Use 'make update' to update dependencies)
