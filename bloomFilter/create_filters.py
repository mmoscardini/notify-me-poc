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

        def inset_rule_for_condition(prev_alert_id, row_number, bf):
            """
            Recursive function to add all rules base on fetched conditions

            :param prev_alert_id: id of the previous alert handled
            :param row_number: the row number to handle this iteration
            :param bf: bloom filter
            :return: None
            """
            row = alerts_and_conditions[row_number]
            if prev_alert_id != row.alert_id:
                bf = BloomFilter(n, p)

            add_bloom_filter_rule(row, bf)

            if row_number >= len(alerts_and_conditions) - 1:
                print(f"Created all {condition_type} rules")
                return

            return inset_rule_for_condition(row.alert_id, row_number + 1, bf)

        inset_rule_for_condition(None, 0, None)


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
