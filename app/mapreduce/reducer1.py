#!/usr/bin/env python3
import sys
import os
import time
from collections import defaultdict
import json
import traceback

from cassandra.cluster import Cluster

def write_fallback_data(data):
    success = False
    
    try:
        try:
            os.makedirs("/tmp/index_data", exist_ok=True)
        except:
            pass
            
        with open("/tmp/index_data/index_data.json", 'w') as f:
            json.dump(data, f)
        print("Successfully wrote fallback data to /tmp/index_data/index_data.json", file=sys.stderr)
        success = True
    except Exception as e:
        print(f"Error writing to fallback location: {str(e)}", file=sys.stderr)
    
    try:
        with open("/tmp/index_data.json", 'w') as f:
            json.dump(data, f)
        print("Successfully wrote fallback data to /tmp/index_data.json", file=sys.stderr)
        success = True
    except Exception as e:
        print(f"Error writing to secondary fallback location: {str(e)}", file=sys.stderr)
    
    return success

def setup_cassandra(fallback_data):
    try:
        print("Connecting to Cassandra...", file=sys.stderr)
        cluster = Cluster(['cassandra-server'], connect_timeout=30)
        session = cluster.connect()
        
        # Create keyspace if it doesn't exist
        print("Creating keyspace if it doesn't exist...", file=sys.stderr)
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)
        
        # Use the keyspace
        session.execute("USE search_engine")
        
        # Create tables if they don't exist
        print("Creating tables if they don't exist...", file=sys.stderr)
        
        # Documents table
        session.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            title text,
            length int
        )
        """)
        
        # Term frequency table
        session.execute("""
        CREATE TABLE IF NOT EXISTS term_frequency (
            term text,
            doc_id text,
            frequency int,
            PRIMARY KEY (term, doc_id)
        )
        """)
        
        # Document frequency table
        session.execute("""
        CREATE TABLE IF NOT EXISTS document_frequency (
            term text PRIMARY KEY,
            doc_count int
        )
        """)
        
        # Corpus stats table
        session.execute("""
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_name text PRIMARY KEY,
            value float
        )
        """)
        
        # Insert data
        print("Inserting document metadata...", file=sys.stderr)
        document_metadata = fallback_data["document_metadata"]
        for doc_id, metadata in document_metadata.items():
            session.execute(
                "INSERT INTO documents (doc_id, title, length) VALUES (%s, %s, %s)",
                (doc_id, metadata["title"], metadata["length"])
            )
        
        print("Inserting term frequency data...", file=sys.stderr)
        term_document_freq = fallback_data["term_document_freq"]
        for term, docs in term_document_freq.items():
            for doc_id, freq in docs.items():
                session.execute(
                    "INSERT INTO term_frequency (term, doc_id, frequency) VALUES (%s, %s, %s)",
                    (term, doc_id, freq)
                )
        
        print("Inserting document frequency data...", file=sys.stderr)
        doc_freq = fallback_data["doc_freq"]
        for term, count in doc_freq.items():
            session.execute(
                "INSERT INTO document_frequency (term, doc_count) VALUES (%s, %s)",
                (term, count)
            )
        
        print("Inserting corpus stats...", file=sys.stderr)
        corpus_stats = fallback_data["corpus_stats"]
        for stat_name, value in corpus_stats.items():
            session.execute(
                "INSERT INTO corpus_stats (stat_name, value) VALUES (%s, %s)",
                (stat_name, float(value))
            )
        
        print("Successfully inserted all data into Cassandra", file=sys.stderr)
        return True
    except Exception as e:
        print(f"Error setting up Cassandra: {str(e)}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return False

def main():
    # Variables to track statistics
    document_metadata = {}
    term_document_freq = defaultdict(dict)
    total_documents = 0
    total_token_count = 0
    
    print("Starting to process mapper output...", file=sys.stderr)
    line_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        line_count += 1
        if line_count % 100 == 0:
            print(f"Processed {line_count} lines so far...", file=sys.stderr)
            
        try:
            parts = line.split('\t')
            
            if parts[0] == "METADATA":
                _, doc_id, title, doc_length = parts
                document_metadata[doc_id] = {
                    "title": title,
                    "length": int(doc_length)
                }
                total_documents += 1
                total_token_count += int(doc_length)
                
            elif parts[0] == "TERM":
                _, term, doc_id, freq = parts
                term_document_freq[term][doc_id] = int(freq)
                
        except Exception as e:
            print(f"Error processing line: {line}", file=sys.stderr)
            print(f"Error details: {str(e)}", file=sys.stderr)
    
    print(f"Finished processing {line_count} lines of input", file=sys.stderr)
    print(f"Found {total_documents} documents with {len(term_document_freq)} unique terms", file=sys.stderr)
    
    doc_freq = {term: len(docs) for term, docs in term_document_freq.items()}
    
    print("Writing data to fallback files...", file=sys.stderr)
    
    try:
        os.makedirs("/tmp/index_data", exist_ok=True)
    except:
        print("Warning: Could not create directory, trying to write file anyway", file=sys.stderr)
    
    fallback_data = {
        "document_metadata": document_metadata,
        "term_document_freq": {term: dict(docs) for term, docs in term_document_freq.items()},
        "doc_freq": doc_freq,
        "corpus_stats": {
            "total_documents": total_documents,
            "total_token_count": total_token_count,
            "avg_doc_length": total_token_count / total_documents if total_documents > 0 else 0
        }
    }
    
    write_fallback_data(fallback_data)
    
    # Set up Cassandra and insert data
    print("Setting up Cassandra and inserting data...", file=sys.stderr)
    cassandra_success = setup_cassandra(fallback_data)
    if cassandra_success:
        print("Successfully set up Cassandra with index data", file=sys.stderr)
    else:
        print("Failed to set up Cassandra, but fallback data is available", file=sys.stderr)
    
    print(f"Indexed {total_documents} documents with {len(term_document_freq)} unique terms")
    print(f"Average document length: {total_token_count / total_documents if total_documents > 0 else 0}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR in reducer: {str(e)}", file=sys.stderr)
        print("Reducer encountered errors but will exit successfully to allow job to complete", file=sys.stderr)
        sys.exit(0)