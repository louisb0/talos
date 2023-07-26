import psycopg2
import psycopg2.extensions

class Database:
    CONFIG = {
        "database": "talos",
        "host": "localhost",
        "user": "talos",
        "password": "talos",
        "port": 5432,
    }

    def __init__(self):
        self.connection: psycopg2.extensions.connection = psycopg2.connect(**self.CONFIG)
        self.cursor: psycopg2.extensions.cursor = self.connection.cursor()