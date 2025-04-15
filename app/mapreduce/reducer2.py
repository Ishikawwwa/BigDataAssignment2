#!/usr/bin/env python3

import sys
import ast
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

CASSANDRA_HOST = 'cassandra-server'
KEYSPACE = "search_engine_index"

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()
session.set_keyspace(KEYSPACE)

session.execute("""
CREATE TABLE IF NOT EXISTS term_stats (
    term TEXT PRIMARY KEY,
    doc_frequency INT
);
""")
session.execute("""
CREATE TABLE IF NOT EXISTS term_index (
    term TEXT PRIMARY KEY,
    postings LIST<FROZEN<TUPLE<TEXT, INT>>>
);
""")

insert_term_stat_stmt = session.prepare("""
INSERT INTO term_stats (term, doc_frequency) VALUES (?, ?)
""")
insert_term_index_stmt = session.prepare("""
INSERT INTO term_index (term, postings) VALUES (?, ?)
""")

current_term = None
postings_list = []

for line in sys.stdin:
    line = line.strip()
    try:
        term, posting_str = line.split('\t', 1)
    except ValueError:
        continue

    try:
        posting_tuple = ast.literal_eval(posting_str)
        if not isinstance(posting_tuple, tuple) or len(posting_tuple) != 2:
            raise ValueError("Posting is not a valid tuple of length 2")
        doc_id_str = str(posting_tuple[0])
        tf_int = int(posting_tuple[1])
        posting = (doc_id_str, tf_int)
    except (ValueError, SyntaxError, TypeError) as e:
        sys.stderr.write(f"Skipping invalid posting format '{posting_str}' for term '{term}': {e}\n")
        continue

    if current_term != term:
        if current_term and postings_list:
            doc_frequency = len(postings_list)

            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch.add(insert_term_stat_stmt, (current_term, doc_frequency))
            batch.add(insert_term_index_stmt, (current_term, postings_list))

            try:
                session.execute(batch)
            except Exception as e:
                sys.stderr.write(f"Error inserting data for term {current_term}: {e}\n")

        current_term = term
        postings_list = []

    postings_list.append(posting)

if current_term and postings_list:
    doc_frequency = len(postings_list)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch.add(insert_term_stat_stmt, (current_term, doc_frequency))
    batch.add(insert_term_index_stmt, (current_term, postings_list))
    try:
        session.execute(batch)
    except Exception as e:
        sys.stderr.write(f"Error inserting data for term {current_term}: {e}\n")

cluster.shutdown() 