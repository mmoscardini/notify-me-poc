#!/usr/bin/python
import psycopg2
from database import get_db_instance, setup, generate_conditions
from bloomFilter.create_filters import create_alerts_bloom_filters
from event import get_eligible_alerts, generate_listing_events
from datetime import datetime
from data.save_results import save_results


def main():
    db = get_db_instance()
    try:
        should_save = True
        num_of_conditions = 5
        num_of_condition_types = 3
        num_of_alerts = 300000
        number_of_listing_events = 21

        db.connect()
        conditions = [generate_conditions(i, num_of_conditions) for i in range(num_of_condition_types)]
        setup(num_of_alerts, conditions)
        print("CREATING BLOOM FILTERS")
        create_alerts_bloom_filters()

        listing_events = generate_listing_events(number_of_listing_events, conditions)

        t_start = datetime.now()
        print("starting to find matching alerts")
        get_eligible_alerts(listing_events, conditions)
        print("finished finding matching alerts")
        t_end = datetime.now()
        time_elapsed = t_end - t_start

        if should_save:
            row = [num_of_alerts, num_of_conditions, number_of_listing_events, time_elapsed]
            save_results(row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if db.is_connected():
            db.close_connection()
            print('Database connection closed.')


if __name__ == '__main__':
    main()
