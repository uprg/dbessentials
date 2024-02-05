from typing import Dict, Union, Any, List
import psycopg2
from pymongo import MongoClient
from pymongo.database import Database
from dbessentials.types import DatabaseConfig
from dbessentials.exceptions import MongoException, PostgresqlException

class DBMigration:

    def __init__(
            self
    ) -> None:
        pass


    def set_mongo_config(
            self, 
            connection_string: str = None, 
            database: str = None
    ) -> DatabaseConfig:
        
        if not connection_string:
            raise MongoException("Please provide a connection string.")
        
        if not database:
            raise MongoException("Please provide a database.")
        
        client = MongoClient(connection_string)
        database = client[database]
        return {'service': 'mongodb', 'config': database}
    

    def set_postgresql_config(
            self,
            connection_string: Union[str, Dict] = None
    ) -> DatabaseConfig:
        
        if not connection_string:
            raise PostgresqlException("Please provide a connection string.")
        
        connection = psycopg2.connect(**connection_string)
        cursor = connection.cursor()
        return {'service': 'postgresql', 'config': cursor}
    

    def get_postgresql_tables(
            self,
            cursor: Any
    ) -> List[str]:
        
        cursor.execute(
            query="select * from information_schema.tables where table_schema='public';"
        )

        records = cursor.fetchall()
        tables = list(map(lambda x: x[2], records))
        return tables
    

    def mongodb_insert_records(
            self,
            database: Database = None,
            tables: List[str] = None,
            cursor: Any = None
    ) -> None:
        for table in tables:
            documents = []
            cursor.execute(query=f"select * from {table};")
            columns = [name.name for name in cursor.description]
            records = cursor.fetchall()

            database.create_collection(name=table)

            for record in records:
                document = {}
                for index, value in enumerate(columns):
                    document[value] = record[index]
                documents.append(document)

            collection = database.get_collection(name=table)
            collection.insert_many(documents=documents, ordered=True)

        
        




    def migrate(
            self,
            source: DatabaseConfig = None,
            traget: DatabaseConfig = None
    ) -> None:
        source_config = source["config"]
        traget_config = traget["config"]

        if source["service"] == "postgresql":
            tables = self.get_postgresql_tables(
                cursor=source_config
            )

        if traget["service"] == "mongodb":
            self.mongodb_insert_records(
                database=traget_config,
                tables=tables,
                cursor=source_config
            )