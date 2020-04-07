#!/usr/bin/python
import psycopg2
from database import get_db_instance, setup
from bloomFilter.create_filters import create_alerts_bloom_filters
from event.listing_event import listing_events
from event.get_eligible_alerts import get_eligible_alerts


def main():
    db = get_db_instance()
    try:
        db.connect()
        setup()
        create_alerts_bloom_filters()

        eligible_alerts = get_eligible_alerts(listing_events)
        print(f"Eligible alerts found {eligible_alerts}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if db.is_connected():
            db.close_connection()
            print('Database connection closed.')


if __name__ == '__main__':
    main()
