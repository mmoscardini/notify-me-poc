from database import get_db_instance
from bloomFilter.bloomfilter import BloomFilter
from bloomFilter.create_filter_rule import create_filter_rule


def get_eligible_alerts(listing_events: list, conditions: list):
    """
    This function creates the same rules used to store the conditions bloom filters using the listing events,
    and finds all alerts that a given listing event attend.

    :param listing_events: A list of listing event structure.
    :return: A list of tuples containing the ID of the listing event and what alert_ids are eligible to be triggered
    """
    db = get_db_instance()
    result = []

    is_valid_key = is_valid_key_builder(conditions)

    bfs = get_alerts_bloom_filters(db)
    for listing_event in listing_events:
        listing_rules = []
        for key, value in listing_event.items():
            if not is_valid_key(key):
                continue
            rules = _build_rules(key, value)
            for rule in rules:
                listing_rules.append(rule)

        eligible_alerts = _get_alerts_that_apply_to_all_listing_rules(listing_rules, bfs)
        result.append((listing_event["id"], eligible_alerts))

    filtered_results = [item for item in result if len(item[1]) > 0]

    return filtered_results


def get_alerts_bloom_filters(db):
    bfs = []
    alert_bf_filters = _get_all_bf_alerts(db)

    for alert_bf in alert_bf_filters:
        bf = BloomFilter(41, 0.05)
        bf.set_bit_array_from_string(alert_bf.filter)
        bfs.append((alert_bf.alert_id, bf))
    return bfs


def _get_alerts_that_apply_to_all_listing_rules(listing_rules, bfs):
    eligible_alerts = [alert_id for alert_id, bf in bfs]

    for alert_id, bf in bfs:
        for listing_rule in listing_rules:
            if alert_id not in eligible_alerts:
                break
            apply = bf.check(listing_rule)
            if not apply and alert_id in eligible_alerts:
                eligible_alerts.remove(alert_id)

    return eligible_alerts

def is_valid_key_builder(conditions):
    bool_conditions, int_conditions, range_conditions = conditions

    def is_valid_key(key):
        return key in bool_conditions or key in int_conditions

    return is_valid_key

def _build_rules(key, value):
    default_rule = create_filter_rule(key, value)
    return (default_rule,)


def _get_all_bf_alerts(db):
    query = "select * from bloomFiltersConditions"
    return db.fetch_data(query)
