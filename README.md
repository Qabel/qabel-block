# Qabel Block Server

This server handles uploads, downloads and deletes on the storage backend for Qabel Box.

Installation:

	cd src
	virtualenv ../venv --python=python3.5
	source ../venv/bin/activate
	pip install -r ../requirements.txt
	python run.py


The server needs a postgresql (>=9.4) database that needs to me initialized by

	alembic -x "url=postgresql://username:password@localhost/dbname" upgrade head

The tests also need a redis server on localhost, running on port 6379. They start their own postgresql instance given it is installed.
And you need to place aws credentials as `~/.aws`.
