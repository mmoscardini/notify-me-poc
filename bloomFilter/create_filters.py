from bloomFilter.bloomfilter import BloomFilter
from database.service import exec_commands, fetch_all_data


def create_alerts_bloom_filters(connection, n=41, p=0.05):
    condition_types = ["boolean", "integer", "range"]
    for condition_type in condition_types:

        query = alert_condition_query_generator(condition_type)
        alerts_and_conditions = fetch_all_data(connection, query)

        def save_bloom_filter(alert_id, bf):
            commands = [f"""
                        INSERT INTO  bloomFiltersConditions VALUES ({alert_id}, '{bf.bit_array.to01()}')
                        ON CONFLICT (alert_id) DO UPDATE SET filter = '{bf.bit_array.to01()}'
                        """]
            exec_commands(connection, commands)
        def get_bloom_filter(alert_id):
            q = f"select filter from  bloomFiltersConditions where alert_id={alert_id}"
            return fetch_all_data(connection, q)

        def add_rule_to_filter(prev_alert_id, row_number, bf):
            row = alerts_and_conditions[row_number]
            rule = create_filter_rule(row.name, row.value)
            bf.add(rule)
            if prev_alert_id == 1:
                print(rule)

            if prev_alert_id != row.alert_id:
                if prev_alert_id is not None:
                    save_bloom_filter(prev_alert_id, bloom_filter)

                new_bloom_filter = BloomFilter(n, p)
                next_alert_bloom_filter = get_bloom_filter(row.alert_id)
                if len(next_alert_bloom_filter) > 0:
                    new_bloom_filter.set_bit_array_from_string(next_alert_bloom_filter[0].filter)

                if row_number < len(alerts_and_conditions) - 1:
                    return add_rule_to_filter(row.alert_id, row_number + 1, new_bloom_filter)

            if row_number >= len(alerts_and_conditions) - 1:
                return save_bloom_filter(prev_alert_id, bloom_filter)

            return add_rule_to_filter(row.alert_id, row_number + 1, bloom_filter)

        bloom_filter = BloomFilter(n, p)
        add_rule_to_filter(None, 0, bloom_filter)


def alert_condition_query_generator(condition_type: str):
    if condition_type == "range":
        return f"""
            select 
                a.id as alert_id,
                cond.name as name,
                (cond.min_val || ',' || cond.max_val) as value
            from alerts a
            join {condition_type}ConditionAlertEdges edge on edge.alert_id = a.id
            join {condition_type}Conditions cond on cond.id = edge.condition_id
            """

    return f"""
    select 
        a.id as alert_id,
        cond.name as name,
        cond.value as value
    from alerts a
    join {condition_type}ConditionAlertEdges edge on edge.alert_id = a.id
    join {condition_type}Conditions cond on cond.id = edge.condition_id
    """


def create_filter_rule(key, value):
    return f"{key}:{value}"
