import psycopg2
from database.service import exec_commands


def connect():
    from database.config import config

    params = config()
    print('Connecting to the PostgreSQL database...')
    return psycopg2.connect(**params)


def setup(connection):
    _drop_tables(connection)
    _create_tables(connection)
    _insert_all_conditions(connection)


def _drop_tables(connection):
    commands = (
        """
        DROP TABLE IF EXISTS alerts;
        """,
        """
        DROP TABLE IF EXISTS integerConditions;
        """,
        """
        DROP TABLE IF EXISTS rangeConditions;
        """,
        """
        DROP TABLE IF EXISTS booleanConditions;
        """,
        """
        DROP TABLE IF EXISTS booleanConditionAlertEdges;
        """,
        """
        DROP TABLE IF EXISTS integerConditionAlertEdges;
        """,
        """
        DROP TABLE IF EXISTS rangeConditionAlertEdges;
        """,
        """
        DROP TABLE IF EXISTS bloomFiltersConditions;
        """
    )

    exec_commands(connection, commands)
    print("DROP TABLES")


def _create_tables(connection):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS alerts (id SERIAL PRIMARY KEY, active BOOL);
        """,
        """
        CREATE TABLE IF NOT EXISTS integerConditions (
            id SERIAL PRIMARY KEY,
            name varchar(50),
            value int8
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS rangeConditions (
            id SERIAL PRIMARY KEY, 
            name varchar(50), 
            min_val float8, 
            max_val float8
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS booleanConditions (
            id SERIAL PRIMARY KEY, 
            name varchar(50), 
            value bool
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS booleanConditionAlertEdges (
            id SERIAL PRIMARY KEY, 
            alert_id float8,
            condition_id float8
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS integerConditionAlertEdges (
            id SERIAL PRIMARY KEY, 
            alert_id float8,
            condition_id float8
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS rangeConditionAlertEdges (
            id SERIAL PRIMARY KEY, 
            alert_id float8,
            condition_id float8
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS bloomFiltersConditions (
            alert_id SERIAL PRIMARY KEY ,
            filter BIT VARYING(256)
        );
        """,
    )

    exec_commands(connection, commands)
    print("CREATE TABLES")


def _insert_all_conditions(connection):
    _insert_boolean_conditions(connection)
    _insert_integer_conditions(connection)
    _insert_range_conditions(connection)
    print("INSERT ALL CONDITIONS")


def _insert_boolean_conditions(connection):

    commands = (
        """
        INSERT INTO booleanConditions VALUES (1, 'accept_pet', false);
        """,
        """
        INSERT INTO booleanConditions VALUES (2, 'accept_pet', true);
        """,
        """
        INSERT INTO booleanConditions VALUES (3, 'has_pool', false);
        """,
        """
        INSERT INTO booleanConditions VALUES (4, 'has_pool', true);
        """,
    )
    exec_commands(connection, commands)
    print("INSERT INTO BOOLEAN CONDITIONS")


def _insert_integer_conditions(connection):
    commands = (
        """
        INSERT INTO integerConditions VALUES (1, 'bedrooms', 0);
        """,
        """
        INSERT INTO integerConditions VALUES (2, 'bedrooms', 1);
        """,
        """
        INSERT INTO integerConditions VALUES (3, 'bedrooms', 2);
        """,
        """
        INSERT INTO integerConditions VALUES (4, 'bedrooms', 3);
        """,
        """
        INSERT INTO integerConditions VALUES (5, 'bedrooms', 4);
        """
    )

    exec_commands(connection, commands)
    print("INSERT INTO INTEGER CONDITIONS")


def _insert_range_conditions(connection):
    commands = (
        """
        INSERT INTO rangeConditions VALUES (1, 'price', 500, 3000);
        """,
        """
        INSERT INTO rangeConditions VALUES (2, 'price', 1000, 3500);
        """,
        """
        INSERT INTO rangeConditions VALUES (3, 'price', 1500, 3000);
        """,
        """
        INSERT INTO rangeConditions VALUES (4, 'price', 2000, 5000);
        """,
        """
        INSERT INTO rangeConditions VALUES (5, 'price', 2500, 6000);
        """
    )

    exec_commands(connection, commands)
    print("INSERT INTO RANGE CONDITIONS")


def create_alerts(connection):
    commands = (
        """
        INSERT INTO alerts VALUES (1, True);
        """,
        """
        INSERT INTO alerts VALUES (2, True);
        """,
        """
        INSERT INTO alerts VALUES (3, True);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (1, 1, 1);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (2, 1, 3);
        """,
        """
        INSERT INTO integerConditionAlertEdges VALUES (1, 1, 2);
        """,
        """
        INSERT INTO rangeConditionAlertEdges VALUES (1, 1, 2);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (3, 2, 2);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (4, 2, 3);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (5, 2, 4);
        """,
        """
        INSERT INTO integerConditionAlertEdges VALUES (2, 2, 3);
        """,
        """
        INSERT INTO integerConditionAlertEdges VALUES (3, 2, 4);
        """,
        """
        INSERT INTO rangeConditionAlertEdges VALUES (2, 2, 3);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (6, 3, 1);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (7, 3, 2);
        """,
        """
        INSERT INTO booleanConditionAlertEdges VALUES (8, 3, 3);
        """,
        """
        INSERT INTO integerConditionAlertEdges VALUES (4, 3, 5);
        """,
        """
        INSERT INTO rangeConditionAlertEdges VALUES (3, 3, 5);
        """,
    )

    exec_commands(connection, commands)
    print("CREATED ALL ALERTS")
