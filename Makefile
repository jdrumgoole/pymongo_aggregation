#
#Make pymongo_aggregation
#
# Assumes passwords for pypi have already been configured with keyring.
#

pip:clean test build
	sh prod-twine.sh

test_build:
	sh test-twine.sh

testx:
	python3 setup.py test

build:
	python3 setup.py sdist

clean:
	rm -rf dist bdist sdist

test_data:
	(cd data;sh restore.sh)

init:
	pip install --upgrade pip
	pip install twine
	pip install keyring
	pip install dateutils
	keyring set https://test.pypi.org/legacy/ jdrumgoole
	keyring set https://upload.pypi.org/legacy/ jdrumgoole

