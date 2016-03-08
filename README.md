# Qabel Block Server

This server handles uploads, downloads and deletes on the storage backend for Qabel Box.

Installation:

	cd src
	virtualenv ../venv --python=python3.5
	source ../venv/bin/activate
	pip install -r ../requirements.txt
	python run.py


The server needs a postgresql database that needs to me initialized by

    python manage.py initdb --psql-dsn "postgresql://username:password@localhost/dbname"
