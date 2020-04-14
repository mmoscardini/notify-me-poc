from builtins import sorted

from bloomFilter.bloomfilter import BloomFilter
from bloomFilter.add_bloom_filter_rule import add_bloom_filter_rule, save_bloom_filter_rule
from database import get_db_instance


def create_alerts_bloom_filters(n=41, p=0.05):
    """
    This method creates a bloom filter for each alert saved on the database.
    The bloom filters contain the conditions each alert is related to.

    :param n: number of items expected to be stored in filter for Bloom filters
    :param p: False Positive probability in decimal for Bloom Filters
    :return None
    """
    db = get_db_instance()
    condition_types = ["boolean", "integer"]
    alerts = [result for  condition_type in condition_types for result in db.fetch_data(_alert_condition_query_generator(condition_type)) ]

    sorted_alerts = sorted(alerts, key=lambda x: x.alert_id )

    prev_alert_id = None
    bf = BloomFilter(n, p)

    for alert in sorted_alerts:
        add_bloom_filter_rule(bf, alert.name, alert.value)

        if prev_alert_id != alert.alert_id and prev_alert_id is not None:
            save_bloom_filter_rule(alert.alert_id, bf)
            bf = BloomFilter(n, p)

        prev_alert_id = alert.alert_id


def _alert_condition_query_generator(condition_type: str):
    return f"""
    select 
        a.id as alert_id,
        cond.name as name,
        cond.value as value
    from alerts a
    join {condition_type}ConditionAlertEdges edge on edge.alert_id = a.id
    join {condition_type}Conditions cond on cond.id = edge.condition_id
    """
