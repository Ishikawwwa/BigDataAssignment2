#!/usr/bin/env python3
import sys
import re
import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

CASSANDRA_HOST = 'cassandra-server'
KEYSPACE = "search_engine_index"
BM25_K1 = 1.5
BM25_B = 0.75
TOP_N = 10

def clean_text(text):
    if not text:
        return []
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text) # Remove punctuation
    return text.split()

def calculate_idf(doc_frequency, total_docs):
    # Checks for edge cases
    if total_docs <= 0 or doc_frequency < 0:
        return 0.0
    safe_total_docs = max(total_docs, doc_frequency)
    numerator = safe_total_docs - doc_frequency + 0.5
    denominator = doc_frequency + 0.5
    if numerator <= 0 or denominator <= 0:
        return 0.0
    return math.log((numerator / denominator) + 1)

if __name__ == "__main__":

    query_string = " ".join(sys.stdin.readlines())
    if not query_string.strip():
        print("Error: No query provided via stdin.", file=sys.stderr)
        sys.exit(1)

    print(f"Received query: \"{query_string.strip()}\"", file=sys.stderr)
    query_terms = clean_text(query_string)
    print(f"Processed query terms: {query_terms}", file=sys.stderr)

    if not query_terms:
        print("Query resulted in no terms after cleaning.", file=sys.stderr)
        sys.exit(0)

    try:
        spark = SparkSession.builder \
            .appName("BM25 Ranking") \
            .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()

        sc = spark.sparkContext
        print("SparkSession created.", file=sys.stderr)

    except Exception as e:
        print(f"Error creating SparkSession: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print("Loading data from Cassandra...", file=sys.stderr)
        doc_stats_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="doc_stats", keyspace=KEYSPACE) \
            .load() \
            .selectExpr("doc_id", "doc_title", "CAST(doc_length AS Double) as doc_length") \
            .persist()

        term_stats_df = spark.read\
            .format("org.apache.spark.sql.cassandra")\
            .options(table="term_stats", keyspace=KEYSPACE)\
            .load()\
            .selectExpr("term", "CAST(doc_frequency AS Double) as doc_frequency")\
            .persist()

        term_index_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="term_index", keyspace=KEYSPACE) \
            .load() \
            .select("term", "postings") \
            .withColumn("posting", F.explode("postings")) \
            .select(
                F.col("term"),
                F.col("posting._1").alias("doc_id"), # Access tuple elements
                F.col("posting._2").cast("double").alias("term_frequency") # TF
            ) \
            .persist()

        print("Cassandra data loaded successfully.", file=sys.stderr)

    except Exception as e:
        print(f"Error loading data from Cassandra: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    total_docs = doc_stats_df.count()
    if total_docs == 0:
        print("No documents found in the index (doc_stats table is empty).", file=sys.stderr)
        spark.stop()
        sys.exit(0)

    avg_doc_length_result = doc_stats_df.agg(F.avg("doc_length")).first()
    avg_doc_length = avg_doc_length_result[0] if avg_doc_length_result and avg_doc_length_result[0] is not None else 0.0

    if avg_doc_length <= 0:
        print("Warning: Average document length is non-positive. Setting to 1.0 to avoid potential division by zero.", file=sys.stderr)
        avg_doc_length = 1.0

    print(f"Total documents (N): {total_docs}", file=sys.stderr)
    print(f"Average document length (avgdl): {avg_doc_length}", file=sys.stderr)

    query_terms_set = set(query_terms)
    filtered_term_stats = term_stats_df.filter(F.col("term").isin(query_terms_set))
    filtered_term_index = term_index_df.filter(F.col("term").isin(query_terms_set))


    if filtered_term_stats.rdd.isEmpty():
        print(f"None of the query terms {query_terms} were found in the index.", file=sys.stderr)
        spark.stop()
        sys.exit(0)

    broadcast_total_docs = sc.broadcast(total_docs)
    idf_udf = F.udf(lambda df: calculate_idf(df, broadcast_total_docs.value), FloatType())

    term_idf_df = filtered_term_stats.withColumn("idf", idf_udf(F.col("doc_frequency"))) \
                                     .select("term", "idf")
    print("IDF calculated for query terms:", file=sys.stderr)
    term_idf_df.show(truncate=False)

    doc_term_data = filtered_term_index.join(doc_stats_df, "doc_id") \
                                       .select("term", "doc_id", "term_frequency", "doc_length", "doc_title")

    bm25_components = doc_term_data.join(term_idf_df, "term") \
                                    .select("doc_id", "doc_title", "term", "term_frequency", "doc_length", "idf")

    k1 = BM25_K1
    b = BM25_B
    broadcast_avg_doc_length = sc.broadcast(avg_doc_length)

    denominator = F.col("term_frequency") + k1 * (1 - b + b * F.col("doc_length") / broadcast_avg_doc_length.value)
    scores_per_term = bm25_components.withColumn(
        "bm25_term_score",
        F.when(denominator == 0, 0.0) \
         .otherwise(F.col("idf") * (((k1 + 1) * F.col("term_frequency")) / denominator))
    )
    print("BM25 scores calculated per term/document.", file=sys.stderr)

    final_scores = scores_per_term.groupBy("doc_id", "doc_title") \
                                  .agg(F.sum("bm25_term_score").alias("bm25_score"))
    print("Scores aggregated per document.", file=sys.stderr)

    top_n_docs = final_scores.orderBy(F.desc("bm25_score")).limit(TOP_N)

    print(f"\n--- Top {TOP_N} Documents ---")
    results = top_n_docs.select("doc_id", "doc_title", "bm25_score").collect()

    if not results:
        print("No relevant documents found for the query.")
    else:
        for i, row in enumerate(results):
            # Formatting it nicely
            print(f"{i+1:>2}. Score: {row['bm25_score']:<10.4f} | ID: {row['doc_id']:<15} | Title: {row['doc_title']}")

    print("\nCleaning up Spark resources...", file=sys.stderr)
    doc_stats_df.unpersist()
    term_stats_df.unpersist()
    term_index_df.unpersist()
    spark.stop()
    print("Ranking finished.", file=sys.stderr)