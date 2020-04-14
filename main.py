#!/usr/bin/python
import psycopg2
from database import get_db_instance, setup, generate_conditions
from bloomFilter.create_filters import create_alerts_bloom_filters
from event import get_eligible_alerts, generate_listing_events


def main():
    db = get_db_instance()
    try:
        num_of_conditions = 5
        num_of_condition_types = 3
        num_of_alerts = 1000
        number_of_listing_events = 1000

        db.connect()
        conditions = [generate_conditions(i, num_of_conditions) for i in range(1, num_of_condition_types + 1)]
        setup(num_of_alerts, conditions)
        create_alerts_bloom_filters()

        listing_events = generate_listing_events(number_of_listing_events, conditions)
        eligible_alerts = get_eligible_alerts(listing_events, conditions)
        print(f"Eligible alerts found {eligible_alerts}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if db.is_connected():
            db.close_connection()
            print('Database connection closed.')


if __name__ == '__main__':
    main()
