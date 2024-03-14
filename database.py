import psycopg2
from psycopg2 import pool
import os


class Database:
    _connection_pool = None

    @staticmethod
    def initialize_pool():
        Database._connection_pool = psycopg2.pool.SimpleConnectionPool(
            1,  # Min number of connections
            10,  # Max number of connections
            dsn=os.environ.get("DB_CONNECTION_STRING")
        )

    @staticmethod
    def get_connection():
        return Database._connection_pool.getconn()

    @staticmethod
    def return_connection(conn):
        Database._connection_pool.putconn(conn)

    @staticmethod
    def close_all_connections():
        Database._connection_pool.closeall()


# Initialize connection pool
Database.initialize_pool()


# Usage
def pg_connect():
    conn = Database.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET search_path TO mainnet")
    finally:
        Database.return_connection(conn)
