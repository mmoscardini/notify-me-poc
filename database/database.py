from database.config import config
import psycopg2

db_instance = None


def get_db_instance():
    if db_instance is not None:
        return db_instance
    return Database()


class Database(object):
    def __init__(self):
        self.connection = None

    def connect(self):
        params = config()
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)
        self.connection = connection
        return self.connection

    def close_connection(self):
        self.connection.close()
        self.connection = None

    def is_connected(self):
        return self.connection is not None

    def exec_multiple_commands(self, commands: list):
        if not self.is_connected():
            self.connect()

        cur = self.connection.cursor()

        for command in commands:
            cur.execute(command)

        cur.close()

        self.connection.commit()

    def exec_commit_query(self, query: str):
        if not self.is_connected():
            self.connect()

        cur = self.connection.cursor()

        cur.execute(query)

        cur.close()

        self.connection.commit()

    def fetch_data(self, query: str):
        if not self.is_connected():
            self.connect()

        cur = self.connection.cursor()
        cur.execute(query)
        db_result = cur.fetchall()
        result = []
        for row in db_result:
            result.append(_Reg(cur, row))
        cur.close()

        return result


class _Reg(object):
    def __init__(self, cursor, row):
        for (attr, val) in zip((d[0] for d in cursor.description), row):
            setattr(self, attr, val)
