#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#

PYBIN=.venv/bin

pip:clean test build
	sh prod-twine.sh

test_build:
	sh test-twine.sh

test:
	${PYBIN}/python setup.py test

build:
	${PYBIN}/python setup.py sdist
clean:
	rm -rf dist bdist sdist
