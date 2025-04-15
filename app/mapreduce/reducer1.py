#!/usr/bin/env python3

import sys
from cassandra.cluster import Cluster

CASSANDRA_HOST = 'cassandra-server'
KEYSPACE = "search_engine_index"

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()

session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
""")

session.set_keyspace(KEYSPACE)

session.execute("""
CREATE TABLE IF NOT EXISTS doc_stats (
    doc_id TEXT PRIMARY KEY,
    doc_title TEXT,
    doc_length INT
);
""")

# Prep for insertion
insert_doc_stat_stmt = session.prepare("""
INSERT INTO doc_stats (doc_id, doc_title, doc_length) VALUES (?, ?, ?)
""")

current_key = None
postings_for_key = []
doc_stat_info = None

for line in sys.stdin:
    line = line.strip()
    try:
        key, marker, value1, value2 = line.split('\t', 3)
    except ValueError:
        try:
            key, marker, value1 = line.split('\t', 2)
            value2 = None
        except ValueError:
            continue

    # Additional check here
    if marker == 'POSTING':
        try:
            value2 = int(value2)
        except (ValueError, TypeError):
            continue

    if current_key != key:
        if current_key:
            if postings_for_key:
                for doc_id, freq in postings_for_key:
                    print(f"{current_key}\t({doc_id},{freq})")
            elif doc_stat_info:
                title, length = doc_stat_info
                try:
                    session.execute(insert_doc_stat_stmt, (current_key, title, int(length)))
                except Exception as e:
                    sys.stderr.write(f"Error inserting doc_stat for {current_key}: {e}\n")

        current_key = key
        postings_for_key = []
        doc_stat_info = None

    if marker == 'POSTING':
        # value1 = doc_id, value2 = frequency
        postings_for_key.append((value1, value2))
    elif marker == 'DOC_STAT':
        # value1 = title, value2 = length
        doc_stat_info = (value1, value2)

if current_key:
    if postings_for_key:
        for doc_id, freq in postings_for_key:
            print(f"{current_key}\t({doc_id},{freq})")
    elif doc_stat_info:
        title, length = doc_stat_info
        try:
            session.execute(insert_doc_stat_stmt, (current_key, title, int(length)))
        except Exception as e:
            sys.stderr.write(f"Error inserting doc_stat for {current_key}: {e}\n")

cluster.shutdown()