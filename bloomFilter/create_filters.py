from bloomFilter.bloomfilter import BloomFilter
from bloomFilter.add_bloom_filter_rule import add_bloom_filter_rule
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

    for condition_type in condition_types:
        query = _alert_condition_query_generator(condition_type)
        alerts_and_conditions = db.fetch_data(query)

        prev_alert_id = None
        bf = None
        for row_number in range(len(alerts_and_conditions) - 1):
            row = alerts_and_conditions[row_number]
            if prev_alert_id != row.alert_id:
                bf = BloomFilter(n, p)

            add_bloom_filter_rule(row, bf)

            prev_alert_id = row.alert_id


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
