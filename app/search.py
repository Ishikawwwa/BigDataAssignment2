#!/usr/bin/env python3
import sys
import re
import math
import json
import os
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import time
import traceback

class SearchEngine:
    def __init__(self, cassandra_host='cassandra-server'):
        self.cassandra_host = cassandra_host
        self.session = None
        self.corpus_stats = {}
        self.use_cassandra = True
        self.fallback_data = None
        
        # Try to connect to Cassandra with retries
        cassandra_connected = self.connect_cassandra(max_retries=1)
        
        # If Cassandra connection fails, use fallback data
        if not cassandra_connected or not self.session:
            print("Cassandra connection failed or keyspace missing. Switching to fallback mode.")
            self.use_cassandra = False
            if not self.try_load_fallback_data():
                print("CRITICAL: Could not load fallback data. Search functionality will be impaired.")
            else:
                print(f"Successfully loaded fallback data with {len(self.fallback_data.get('document_metadata', {}))} documents")
    
    def connect_cassandra(self, max_retries=3, retry_interval=5):
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print(f"Connecting to Cassandra (attempt {retry_count+1}/{max_retries})...")
                cluster = Cluster([self.cassandra_host], connect_timeout=30)  # Increased timeout
                self.session = cluster.connect()
                
                # Check if keyspace exists
                keyspaces = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
                keyspace_exists = False
                for keyspace in keyspaces:
                    if keyspace.keyspace_name == 'search_engine':
                        keyspace_exists = True
                        break
                
                if not keyspace_exists:
                    print("The search_engine keyspace does not exist in Cassandra.")
                    print("This may indicate that the MapReduce job didn't complete successfully.")
                    print("Will attempt to create the keyspace...")
                    try:
                        self.session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS search_engine
                        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
                        """)
                        print("Successfully created search_engine keyspace")
                        keyspace_exists = True
                    except Exception as e:
                        print(f"Failed to create keyspace: {e}")
                        return False
                
                try:
                    self.session.set_keyspace('search_engine')
                    print("Successfully connected to Cassandra and set keyspace to search_engine")
                    
                    try:
                        rows = self.session.execute("SELECT count(*) FROM corpus_stats")
                        print("Verified Cassandra tables exist and are accessible")
                        
                        self.load_corpus_stats()
                        return True
                    except Exception as e:
                        print(f"Error querying Cassandra tables: {e}")
                        print("Tables may not be fully set up. Will attempt to check and create missing tables...")
                        
                        try:
                            # Check and create tables if needed
                            tables_needed = {
                                "documents": """
                                    CREATE TABLE IF NOT EXISTS documents (
                                        doc_id text PRIMARY KEY,
                                        title text,
                                        length int
                                    )
                                """,
                                "term_frequency": """
                                    CREATE TABLE IF NOT EXISTS term_frequency (
                                        term text,
                                        doc_id text,
                                        frequency int,
                                        PRIMARY KEY (term, doc_id)
                                    )
                                """,
                                "document_frequency": """
                                    CREATE TABLE IF NOT EXISTS document_frequency (
                                        term text PRIMARY KEY,
                                        doc_count int
                                    )
                                """,
                                "corpus_stats": """
                                    CREATE TABLE IF NOT EXISTS corpus_stats (
                                        stat_name text PRIMARY KEY,
                                        value float
                                    )
                                """
                            }
                            
                            existing_tables = self.session.execute(
                                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'search_engine'"
                            )
                            existing_table_names = [row.table_name for row in existing_tables]
                            
                            for table_name, create_query in tables_needed.items():
                                if table_name not in existing_table_names:
                                    print(f"Creating missing table: {table_name}")
                                    self.session.execute(create_query)
                            
                            # Now check if we have data in corpus_stats
                            rows = self.session.execute("SELECT count(*) FROM corpus_stats")
                            count = rows.one()[0]
                            if count == 0:
                                print("No data in corpus_stats table, falling back to file data")
                                self.session = None
                                return False
                            else:
                                print("Tables are set up and contain data")
                                self.load_corpus_stats()
                                return True
                                
                        except Exception as create_error:
                            print(f"Error creating missing tables: {create_error}")
                            print("Falling back to file data.")
                            self.session = None
                            return False
                        
                except Exception as e:
                    print(f"Error setting keyspace: {e}")
                    print("The search_engine keyspace does not exist. Falling back to file data.")
                    self.session = None
                    return False
                
            except Exception as e:
                print(f"Error connecting to Cassandra (attempt {retry_count+1}/{max_retries}): {e}")
                retry_count += 1
                
                if retry_count < max_retries:
                    print(f"Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
        
        print(f"Failed to connect to Cassandra after {max_retries} attempts.")
        return False
    
    def try_load_fallback_data(self):
        print("Trying to load fallback data...")
        
        try:
            print(f"Current directory: {os.getcwd()}")
            print(f"Listing files in /tmp directory:")
            if os.path.exists("/tmp"):
                for file in os.listdir("/tmp"):
                    if "index" in file:
                        print(f"  - /tmp/{file}")
                        # Also show file details
                        try:
                            file_size = os.path.getsize(f"/tmp/{file}")
                            print(f"    Size: {file_size} bytes")
                        except:
                            print(f"    Cannot get file size")
                
            print(f"Checking if /tmp/index_data exists: {os.path.exists('/tmp/index_data')}")
            if os.path.exists("/tmp/index_data"):
                print(f"Listing files in /tmp/index_data:")
                for file in os.listdir("/tmp/index_data"):
                    print(f"  - /tmp/index_data/{file}")
                    try:
                        file_size = os.path.getsize(f"/tmp/index_data/{file}")
                        print(f"    Size: {file_size} bytes")
                    except:
                        print(f"    Cannot get file size")
        except Exception as e:
            print(f"Error listing directories: {e}")
        
        fallback_files = [
            "/tmp/index_data/index_data.json",
            "/tmp/index_data.json"
        ]
        
        for fallback_file in fallback_files:
            try:
                if os.path.exists(fallback_file):
                    print(f"Trying to load fallback data from {fallback_file}")
                    try:
                        file_size = os.path.getsize(fallback_file)
                        print(f"File size: {file_size} bytes")
                        
                        if file_size == 0:
                            print(f"Warning: File {fallback_file} is empty, skipping")
                            continue
                            
                        with open(fallback_file, 'r') as f:
                            data = f.read()
                            print(f"Read {len(data)} bytes from file")
                            self.fallback_data = json.loads(data)
                        
                        self.corpus_stats = self.fallback_data.get("corpus_stats", {})
                        doc_count = len(self.fallback_data.get('document_metadata', {}))
                        print(f"Loaded fallback data with {doc_count} documents")
                        
                        if doc_count > 0:
                            return True
                        else:
                            print(f"Warning: Fallback data contains no documents, continuing search")
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON from {fallback_file}: {e}")
                    except Exception as e:
                        print(f"Error loading data from {fallback_file}: {e}")
                        import traceback
                        traceback.print_exc()
            except Exception as e:
                print(f"Error checking file {fallback_file}: {e}")
        
        print(f"No valid fallback files found")
        return False
    
    def load_corpus_stats(self):
        if not self.use_cassandra or not self.session:
            return
            
        try:
            rows = self.session.execute("SELECT stat_name, value FROM corpus_stats")
            for row in rows:
                self.corpus_stats[row.stat_name] = row.value
                
            print(f"Loaded corpus stats: {self.corpus_stats}")
        except Exception as e:
            print(f"Error loading corpus stats: {e}")
    
    def tokenize(self, query):
        query = re.sub(r'[^\w\s]', '', query.lower())
        return [token for token in query.split() if token]
    
    def search(self, query, k=10):
        if not self.use_cassandra and not self.fallback_data:
            print("Error: Neither Cassandra nor fallback data are available")
            return []
            
        query_terms = self.tokenize(query)
        print(f"Searching for query: '{query}' (terms: {query_terms})")
        
        if not query_terms:
            print("Empty query after tokenization")
            return []
        
        term_doc_freqs = {}
        doc_freqs = {}
        
        N = self.corpus_stats.get('total_documents', 0)
        avg_doc_length = self.corpus_stats.get('avg_doc_length', 0)
        
        print(f"Corpus stats: {N} documents, avg length: {avg_doc_length}")
        
        if self.use_cassandra:
            for term in query_terms:
                try:
                    rows = self.session.execute(
                        "SELECT doc_count FROM document_frequency WHERE term = %s", 
                        (term,)
                    )
                    doc_freq = next(iter(rows), None)
                    if doc_freq:
                        doc_freqs[term] = doc_freq.doc_count
                except Exception as e:
                    print(f"Error getting document frequency for term '{term}': {e}")
            
            for term in query_terms:
                try:
                    rows = self.session.execute(
                        "SELECT doc_id, frequency FROM term_frequency WHERE term = %s", 
                        (term,)
                    )
                    term_doc_freqs[term] = {row.doc_id: row.frequency for row in rows}
                except Exception as e:
                    print(f"Error getting term frequencies for term '{term}': {e}")
        else:
            doc_freqs = self.fallback_data.get("doc_freq", {})
            term_doc_freqs = {
                term: self.fallback_data.get("term_document_freq", {}).get(term, {})
                for term in query_terms
            }
        
        all_doc_ids = set()
        for term, doc_freqs_dict in term_doc_freqs.items():
            all_doc_ids.update(doc_freqs_dict.keys())
        
        doc_scores = {}
        k1 = 1.2
        b = 0.75
        
        print(f"Found {len(all_doc_ids)} candidate documents")
        
        for doc_id in all_doc_ids:
            score = 0
            
            doc_length = 0
            
            if self.use_cassandra:
                try:
                    rows = self.session.execute(
                        "SELECT length FROM documents WHERE doc_id = %s", 
                        (doc_id,)
                    )
                    doc_row = next(iter(rows), None)
                    if doc_row:
                        doc_length = doc_row.length
                except Exception as e:
                    print(f"Error getting document length for doc '{doc_id}': {e}")
            else:
                doc_metadata = self.fallback_data.get("document_metadata", {}).get(doc_id, {})
                doc_length = doc_metadata.get("length", 0)
            
            if not doc_length:
                continue
            
            for term in query_terms:
                if term in doc_freqs and term in term_doc_freqs:
                    df = doc_freqs[term]
                    if df == 0:
                        continue
                        
                    tf = term_doc_freqs[term].get(doc_id, 0)
                    if tf == 0:
                        continue
                    
                    idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
                    
                    tf_normalized = tf / (1.0 + k1 * ((1.0 - b) + b * doc_length / avg_doc_length))
                    
                    term_score = idf * (k1 + 1.0) * tf_normalized / (k1 + tf_normalized)
                    score += term_score
            
            if score > 0:
                doc_scores[doc_id] = score
        
        top_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)[:k]
        
        results = []
        
        for doc_id, score in top_docs:
            title = ""
            
            if self.use_cassandra:
                try:
                    rows = self.session.execute(
                        "SELECT title FROM documents WHERE doc_id = %s", 
                        (doc_id,)
                    )
                    doc_row = next(iter(rows), None)
                    if doc_row:
                        title = doc_row.title
                except Exception as e:
                    print(f"Error getting title for doc '{doc_id}': {e}")
            else:
                doc_metadata = self.fallback_data.get("document_metadata", {}).get(doc_id, {})
                title = doc_metadata.get("title", "Unknown")
            
            results.append({
                "doc_id": doc_id,
                "title": title,
                "score": score
            })
        
        return results

def main():
    if len(sys.argv) < 2:
        print("Usage: python search.py <query>")
        sys.exit(1)
    
    query = " ".join(sys.argv[1:])
    
    spark = SparkSession.builder \
        .appName("SearchEngine") \
        .getOrCreate()
    
    try:
        search_engine = SearchEngine()
        
        results = search_engine.search(query)
        
        print("\nSearch Results:")
        print("=" * 40)
        
        if not results:
            print("No results found for query: " + query)
        else:
            for i, result in enumerate(results, 1):
                print(f"{i}. {result['title']} (ID: {result['doc_id']}, Score: {result['score']:.4f})")
        
    except Exception as e:
        print(f"Error during search: {e}")
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 