from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import os
import time

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local[*]") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Caugth a small bug here, so had to add waiting to make sure hdfs is ready
def wait_for_hdfs():
    import subprocess
    while True:
        try:
            subprocess.run(["hdfs", "dfs", "-ls", "/"], check=True, capture_output=True)
            break
        except subprocess.CalledProcessError:
            print("Waiting for HDFS to be ready...")
            time.sleep(5)

wait_for_hdfs()

print("Reading parquet file...")
df = spark.read.parquet("hdfs://cluster-master:9000/n.parquet")

n = 1000
print("Processing documents...")
df = df.select(['id', 'title', 'text']) \
    .filter("text is not null and length(text) > 0") \
    .sample(fraction=100 * n / df.count(), seed=0) \
    .limit(n)

os.makedirs("data", exist_ok=True)

def create_doc(row):
    try:
        filename = f"data/{row['id']}_{sanitize_filename(row['title']).replace(' ', '_')}.txt"
        with open(filename, "w", encoding='utf-8') as f:
            f.write(row['text'])
    except Exception as e:
        print(f"Error processing document {row['id']}: {str(e)}")

print("Creating document files...")
df.foreach(create_doc)

print("Creating index data...")
index_df = df.select(
    df.id.cast("string").alias("doc_id"),
    df.title.alias("doc_title"),
    df.text.alias("doc_text")
)

print("Writing index data to HDFS...")
index_df.write.mode("overwrite").csv("hdfs://cluster-master:9000/index/data", sep="\t")

spark.stop()