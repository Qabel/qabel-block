import environs


env = environs.Env()

with env.prefixed('BLOCK_'):
    logging_config = env('LOGGING_CONFIG')
    local_storage = env('LOCAL_STORAGE', '')
    debug = env.bool('DEBUG')
    accounting_host = env('ACCOUNTING_HOST')
    port = env.int('PORT')
    apisecret = env('API_SECRET')
    max_body_size = env.int('MAX_BODY_SIZE')
    prometheus_port = env('PROMETHOUS_PORT', None)
    dummy = env.bool('DUMMY', False)
    transfers = env.int('TRANSFERS', 10)

psql_dsn = env('DATABASE_URL')
redis_host = env('REDIS_HOST', 'redis')
redis_port = env.int('REDIS_PORT', 6379)
