#!/usr/bin/python
import psycopg2
from database.setup import connect, setup, create_alerts
from bloomFilter.create_filters import create_alerts_bloom_filters
from event.listing_event import listing_events
from event.get_valid_alerts import get_valid_alerts


def main():
    connection = None
    try:
        connection = connect()
        setup(connection)
        create_alerts(connection)
        create_alerts_bloom_filters(connection)

        get_valid_alerts(connection, listing_events)


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')


if __name__ == '__main__':
    main()
