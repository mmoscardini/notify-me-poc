def exec_commands(connection, commands):
    # create a cursor
    cur = connection.cursor()

    for command in commands:
        cur.execute(command)

        # close the communication with the PostgreSQL
    cur.close()

    # commit this changes
    connection.commit()


class Reg(object):
    def __init__(self, cursor, row):
        for (attr, val) in zip((d[0] for d in cursor.description), row):
            setattr(self, attr, val)


def fetch_all_data(connection, query):
    cur = connection.cursor()
    cur.execute(query)
    db_result = cur.fetchall()
    result = []
    for row in db_result:
        result.append(Reg(cur, row))
    cur.close()

    return result
