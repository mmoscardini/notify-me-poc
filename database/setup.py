from database import get_db_instance
from faker import Faker
from database import generate_conditions


def setup(num_of_alerts, conditions):
    """
    This function will create the database snapshot for this POC
    """
    db = get_db_instance()

    _drop_tables(db)
    _create_tables(db)
    _insert_alerts_and_conditions(db, num_of_alerts, conditions)


def _drop_tables(db):
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

    db.exec_multiple_commands(commands)
    print("DROP TABLES")


def _create_tables(db):
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

    db.exec_multiple_commands(commands)
    print("CREATE TABLES")


def _insert_alerts_and_conditions(db, num_of_alerts, conditions):
    num_of_conditions = len(conditions[0])

    bool_conditions, int_conditions, range_conditions = conditions

    _insert_boolean_conditions(db, bool_conditions)
    _insert_integer_conditions(db, int_conditions)
    _insert_range_conditions(db, range_conditions)

    _create_alerts(db, num_of_alerts, num_of_conditions)
    print("INSERT ALERTS AND CONDITIONS")


def _insert_boolean_conditions(db, conditions):
    commands = [
        f"""
        INSERT INTO booleanConditions(name, value) VALUES ('{condition}', 'true');
        INSERT INTO booleanConditions(name, value) VALUES ('{condition}', 'false');
        """ for condition in conditions
    ]

    db.exec_multiple_commands(commands)
    print("INSERT INTO BOOLEAN CONDITIONS")


def _insert_integer_conditions(db, conditions):
    commands = [
        f"""
        INSERT INTO integerConditions(name, value) VALUES ('{condition}', '0');
        INSERT INTO integerConditions(name, value) VALUES ('{condition}', '1');
        INSERT INTO integerConditions(name, value) VALUES ('{condition}', '2');
        INSERT INTO integerConditions(name, value) VALUES ('{condition}', '3');
        """ for condition in conditions
    ]

    db.exec_multiple_commands(commands)
    print("INSERT INTO INTEGER CONDITIONS")


def _insert_range_conditions(db, conditions):
    fake = Faker()

    commands = [
        f"""
        INSERT INTO rangeConditions(name, min_val, max_val) VALUES ('{condition}', '{fake.random.randint(0, 2)}', '{fake.random.randint(2, 5)}');
        """ for condition in conditions
    ]

    db.exec_multiple_commands(commands)
    print("INSERT INTO RANGE CONDITIONS")


def _create_alerts(db, num_of_alerts, num_of_conditions):
    fake = Faker()

    alert_commands = [
        """
        INSERT INTO alerts(active) VALUES (true);
        """ for _ in range(num_of_alerts)
    ]

    boolean_conditions_edge_commands = []

    for alert_id in range(1, num_of_alerts + 1):
        for cond_id in range(1, (num_of_conditions * 2) + 1, 2):
            random_selection = fake.random.random()
            if random_selection < 0.333:
                boolean_conditions_edge_commands.append(
                    f"""
                    INSERT INTO booleanConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id}); 
                    """
                )
            elif random_selection > 0.334 and random_selection < 0.666:
                boolean_conditions_edge_commands.append(
                    f"""
                    INSERT INTO booleanConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id + 1}); 
                    """
                )
            else:
                boolean_conditions_edge_commands.append(
                    f"""
                    INSERT INTO booleanConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id}); 
                    INSERT INTO booleanConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id + 1}); 
                    """
                )

    integer_conditions_edge_commands = []
    for alert_id in range(1, num_of_alerts + 1):
        for cond_id in range(1, (num_of_conditions * 4) + 1, 4):
            random_selection_1 = bool(fake.random.getrandbits(1))
            random_selection_2 = bool(fake.random.getrandbits(1))
            random_selection_3 = bool(fake.random.getrandbits(1))
            random_selection_4 = bool(fake.random.getrandbits(1))

            if random_selection_1:
                integer_conditions_edge_commands.append(
                    f"""
                    INSERT INTO integerConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id}); 
                    """
                )
            if random_selection_2:
                integer_conditions_edge_commands.append(
                    f"""
                    INSERT INTO integerConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id + 1}); 
                    """
                )
            if random_selection_3:
                integer_conditions_edge_commands.append(
                    f"""
                    INSERT INTO integerConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id + 2}); 
                    """
                )
            if random_selection_4:
                integer_conditions_edge_commands.append(
                    f"""
                    INSERT INTO integerConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id + 3}); 
                    """
                )

            if not (random_selection_1 or random_selection_2 or random_selection_3 or random_selection_4):
                integer_conditions_edge_commands.append(
                    f"""
                    INSERT INTO integerConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id}); 
                    """
                )

    range_conditions_edge_commands = []
    for alert_id in range(1, num_of_alerts + 1):
        for cond_id in range(1, num_of_conditions):
            if bool(fake.random.getrandbits(1)):
                range_conditions_edge_commands.append(
                    f"""
                    INSERT INTO rangeConditionAlertEdges(alert_id, condition_id) VALUES 
                    ({alert_id}, {cond_id}); 
                    """
                )

    commands = [
        *alert_commands,
        *boolean_conditions_edge_commands,
        *range_conditions_edge_commands,
        *integer_conditions_edge_commands
    ]

    db.exec_multiple_commands(commands)
    print("CREATED ALL ALERTS")
