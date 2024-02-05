from dbessentials import DBMigration

db_migrations = DBMigration()

postgresql_connection_string = {
    'user': 'postgres',
    'password': 12,
    'database': 'postgres',
    'host': 'localhost',
    'port': 5432
}

mongodb_connection_string = "mongodb://localhost:27017/"
mongodb_database = "test"

mongodb_config = db_migrations.set_mongo_config(
    connection_string=mongodb_connection_string, 
    database=mongodb_database
)

postgresql_config = db_migrations.set_postgresql_config(
    connection_string=postgresql_connection_string
)

db_migrations.migrate(
    source=postgresql_config,
    traget=mongodb_config
)