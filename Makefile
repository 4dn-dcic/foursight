.PHONY: test

clean:
	rm -rf *.egg-info

clear-poetry-cache:  # clear poetry/pypi cache. for user to do explicitly, never automatic
	poetry cache clear pypi --all

configure:  # does any pre-requisite installs
	pip install poetry==1.3.2

build:  # builds
	make configure
	poetry install

update:
	poetry update

test:
	pytest -vv --cov chalicelib tests

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make clean' to clean out cached info like eggs.)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make build' to install dependencies using poetry.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
	   $(info - Use 'make update' to update dependencies)

publish:
	# New Python based publish script in dcicutils (2023-04-25).
	poetry run publish-to-pypi --debug

publish-for-ga:
	# New Python based publish script in dcicutils (2023-04-25).
	poetry run publish-to-pypi --noconfirm
