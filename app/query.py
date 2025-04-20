#!/usr/bin/env python3
import sys
import re
import math
from pyspark.sql import SparkSession
from pyspark import SparkContext, RDD
from cassandra.cluster import Cluster

def tokenize(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    tokens = text.split()
    stopwords = {'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'in', 'to', 'of', 'for', 'with', 'on', 'at', 'this', 'that'}
    tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
    return tokens

def get_cassandra_data():
    try:
        cluster = Cluster(['cassandra-server'])
        session = cluster.connect('search_engine')
        
        corpus_stats = {}
        rows = session.execute("SELECT stat_name, value FROM corpus_stats")
        for row in rows:
            corpus_stats[row.stat_name] = row.value
        
        total_documents = int(corpus_stats.get('total_documents', 0))
        avg_doc_length = float(corpus_stats.get('avg_doc_length', 0))
        
        doc_freq = {}
        rows = session.execute("SELECT term, doc_count FROM document_frequency")
        for row in rows:
            doc_freq[row.term] = row.doc_count
        
        doc_metadata = {}
        rows = session.execute("SELECT doc_id, title, length FROM documents")
        for row in rows:
            doc_metadata[row.doc_id] = {
                'title': row.title,
                'length': row.length
            }
        
        return total_documents, avg_doc_length, doc_freq, doc_metadata, session
    except Exception as e:
        print(f"Error connecting to Cassandra: {str(e)}")
        sys.exit(1)

def calculate_bm25(term_freq, doc_length, df, N, avg_doc_length, k1=1.2, b=0.75):
    if df == 0 or N == 0:
        return 0
    
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
    tf_normalized = term_freq / (k1 * ((1 - b) + b * (doc_length / avg_doc_length)) + term_freq)
    
    return idf * tf_normalized

def main():
    if len(sys.argv) < 2:
        print("Usage: python query.py <query text>")
        sys.exit(1)
    
    query = ' '.join(sys.argv[1:])
    print(f"Searching for: {query}")
    
    spark = SparkSession.builder \
        .appName("BM25 Document Ranking") \
        .getOrCreate()
    sc = spark.sparkContext
    
    N, avg_doc_length, doc_freq, doc_metadata, session = get_cassandra_data()
    
    query_terms = tokenize(query)
    if not query_terms:
        print("No valid search terms found in query")
        sys.exit(0)
    
    docs_scores = []
    
    for term in query_terms:
        rows = session.execute(
            "SELECT doc_id, frequency FROM term_frequency WHERE term = %s",
            (term,)
        )
        
        if not rows:
            continue
        
        term_docs = []
        for row in rows:
            doc_id = row.doc_id
            term_freq = row.frequency
            
            if doc_id in doc_metadata:
                doc_length = doc_metadata[doc_id]['length']
                
                df = doc_freq.get(term, 0)
                score = calculate_bm25(term_freq, doc_length, df, N, avg_doc_length)
                
                term_docs.append((doc_id, score))
        
        docs_scores.extend(term_docs)
    
    doc_scores_rdd = sc.parallelize(docs_scores)
    
    doc_total_scores = doc_scores_rdd.reduceByKey(lambda a, b: a + b)
    
    top_docs = doc_total_scores.sortBy(lambda x: -x[1]).take(10)
    
    print("\nTop 10 relevant documents:")
    print("--------------------------")
    for i, (doc_id, score) in enumerate(top_docs, 1):
        title = doc_metadata.get(doc_id, {}).get('title', 'Unknown Title')
        print(f"{i}. Document ID: {doc_id}, Title: {title}, Score: {score:.4f}")
    
    spark.stop()

if __name__ == "__main__":
    main()