from database import get_db_instance
from bloomFilter.bloomfilter import BloomFilter
from bloomFilter.create_filter_rule import create_filter_rule


def get_eligible_alerts(listing_events):
    """
    This function creates the same rules used to store the conditions bloom filters using the listing events,
    and finds all alerts that a given listing event attend.

    :param listing_events: A list of listing event structure.
    :return: A list of tuples containing the ID of the listing event and what alert_ids are eligible to be triggered
    """
    db = get_db_instance()
    result = []

    bfs = get_alerts_bloom_filters(db)
    for listing_event in listing_events:
        listing_rules = []
        for key, value in listing_event.items():
            condition_type = _get_condition_type_by_key(key)
            if not condition_type:
                continue
            rules = _build_rules(key, value, condition_type)
            for rule in rules:
                listing_rules.append(rule)

        eligible_alerts = _get_alerts_that_apply_to_all_listing_rules(listing_rules, bfs)
        result.append((listing_event["id"], eligible_alerts))

    return result


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
            apply = bf.check(listing_rule)
            if not apply and alert_id in eligible_alerts:
                eligible_alerts.remove(alert_id)


    return eligible_alerts


def _get_condition_type_by_key(key):
    boolean_keys = ["has_pool", "accept_pet"]
    integer_keys = ["bedrooms"]

    if key in boolean_keys:
        return "boolean"
    elif key in integer_keys:
        return "integer"
    else:
        return False


def _build_rules(key, value, condition_type):
    if value is None and condition_type == "boolean":
        t_rule = create_filter_rule(key, True)
        f_rule = create_filter_rule(key, True)
        return (t_rule, f_rule)

    default_rule = create_filter_rule(key, value)
    return (default_rule,)


def _get_all_bf_alerts(db):
    query = "select * from bloomFiltersConditions"
    return db.fetch_data(query)
