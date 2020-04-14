from bloomFilter.create_filter_rule import create_filter_rule
from database import get_db_instance

db = get_db_instance()


def add_bloom_filter_rule(bf, key, value):
    rule = create_filter_rule(key, value)
    bf.add(rule)
    return bf


def save_bloom_filter_rule(alert_id, bf):
    insert_or_update_bloom_filter(db, alert_id, bf.bit_array.to01())


def insert_or_update_bloom_filter(db, alert_id, filter):
    q = f"""
         INSERT INTO  bloomFiltersConditions VALUES ({alert_id}, '{filter}')
         ON CONFLICT (alert_id) DO UPDATE SET filter = '{filter}'
         """
    db.exec_commit_query(q)
