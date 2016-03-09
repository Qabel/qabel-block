import manage


def test_args(mocker):
    dsn = "foobar"
    initdb = mocker.patch('manage.initdb')
    dropdb = mocker.patch('manage.dropdb')
    arguments = ['initdb', '--psql-dsn', dsn]
    manage.main(arguments)
    initdb.assert_called_with(dsn)

    arguments = ['dropdb', '--psql-dsn', dsn]
    manage.main(arguments)
    dropdb.assert_called_with(dsn)


def test_initdb(mocker):
    db_init = mocker.patch('blockserver.backend.database.PostgresUserDatabase.init_db')
    db = mocker.patch('blockserver.backend.database.PostgresUserDatabase.__init__')
    db.return_value = None
    connect = mocker.patch('psycopg2.connect')
    dsn = 'foobar'
    manage.initdb(dsn)
    connect.assert_called_with(dsn)
    db.assert_called_with(connect.return_value)
    db_init.assert_called_with()


def test_drobdb(mocker):
    db_drop = mocker.patch('blockserver.backend.database.PostgresUserDatabase.drop_db')
    db = mocker.patch('blockserver.backend.database.PostgresUserDatabase.__init__')
    db.return_value = None
    connect = mocker.patch('psycopg2.connect')
    dsn = 'foobar'
    manage.dropdb(dsn)
    connect.assert_called_with(dsn)
    db.assert_called_with(connect.return_value)
    db_drop.assert_called_with()
