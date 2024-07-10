import psycopg2
from psycopg2 import sql

class DatabaseHandler:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.connection.autocommit = True

    def execute_query(self, query, values):
        with self.connection.cursor() as cursor:
            cursor.execute(query, values)
