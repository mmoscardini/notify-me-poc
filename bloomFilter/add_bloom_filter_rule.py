from bloomFilter.create_filter_rule import create_filter_rule
from database import get_db_instance


def add_bloom_filter_rule(item, bloom_filter):
    """
    This function creates a new rule based on the item information (name and value), hashes it with the bloom filter
    and store it's result to the database

    :param item: the item of the database containing the required information to create a rule for the bloom filter
    :param bloom_filter: the bloom filter used to store the rule and generate the bit_array
    :return None
    """
    db = get_db_instance()

    rule = create_filter_rule(item.name, item.value)
    bloom_filter = update_bloom_filter_with_existing_filter(db, item.alert_id, bloom_filter)
    bloom_filter.add(rule)
    insert_or_update_bloom_filter(db, item.alert_id, bloom_filter)


def update_bloom_filter_with_existing_filter(db, alert_id, bloom_filter):
    existing_bloom_filter = get_bloom_filter_by_id(db, alert_id)
    if len(existing_bloom_filter) > 0:
        bloom_filter.set_bit_array_from_string(existing_bloom_filter[0].filter)
    return bloom_filter

def get_bloom_filter_by_id(db, alert_id):
    q = f"select filter from  bloomFiltersConditions where alert_id={alert_id}"
    return db.fetch_data(q)


def insert_or_update_bloom_filter(db, alert_id, bloom_filter):
    q = f"""
         INSERT INTO  bloomFiltersConditions VALUES ({alert_id}, '{bloom_filter.bit_array.to01()}')
         ON CONFLICT (alert_id) DO UPDATE SET filter = '{bloom_filter.bit_array.to01()}'
         """
    db.exec_commit_query(q)
