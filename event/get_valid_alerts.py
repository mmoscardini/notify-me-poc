from database.service import fetch_all_data
from bloomFilter.bloomfilter import BloomFilter
from bloomFilter.create_filters import create_filter_rule


def get_valid_alerts(connection, listing_events):
    result = []
    for listing_event in listing_events:
        event_bloom_filter = build_listing_event_bloom_filter(listing_event)
        matching_alerts = _get_matching_alerts(connection, event_bloom_filter.bit_array.to01())
        result.append(matching_alerts)

    return result


def build_listing_event_bloom_filter(listing_event, n=41, p=0.05):
    bloom_filter = BloomFilter(n, p)

    for key, value in listing_event.items():
        rule = create_filter_rule(key, value)
        bloom_filter.add(rule)

    return bloom_filter


def _get_matching_alerts(connection, filter):
    query = f"select alert_id from bloomFiltersConditions where filter = '{filter}'"
    return fetch_all_data(connection, query)
