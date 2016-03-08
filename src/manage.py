import argparse
import psycopg2
from blockserver.backend.database import PostgresUserDatabase


def _connect(dsn):
    connection = psycopg2.connect(dsn)
    db = PostgresUserDatabase(connection)
    return db


def initdb(dsn):
    db = _connect(dsn)
    db.init_db()


def dropdb(dsn):
    db = _connect(dsn)
    db.drop_db()


def parse_args(arguments):
    parser = argparse.ArgumentParser(description='Manage qabel-block')
    parser.add_argument('action', choices=('initdb', 'dropdb'))
    parser.add_argument('--psql-dsn', metavar='DSN', required=True)
    return parser.parse_args(arguments)


def main(arguments=None):
    args = parse_args(arguments)
    if args.action == 'initdb':
        initdb(args.psql_dsn)
    elif args.action == 'dropdb':
        dropdb(args.psql_dsn)

if __name__ == '__main__':
    main()



