#!/usr/bin/env python3
import os
from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
import sys

def create_doc(row):
    try:
        title = str(row['title']).replace("/", "_")
        filename = "data/" + sanitize_filename(str(row['id']) + "_" + title).replace(" ", "_") + ".txt"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(str(row['text']))
        
        return True
    except Exception as e:
        print(f"Error creating document: {str(e)}")
        return False

def main():
    print("Starting data preparation...")
    
    spark = SparkSession.builder \
        .appName('data preparation') \
        .master("local") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
    
    if not os.path.exists("data"):
        os.makedirs("data")
    
    # Just making sure parquet is found, had minor issues with it
    parquet_paths = [
        "/n.parquet",
        "n.parquet", 
        "../n.parquet",
    ]
    
    parquet_file = None
    for path in parquet_paths:
        if os.path.exists(path):
            print(f"Found parquet file at: {path}")
            parquet_file = path
            break
    
    if not parquet_file:
        print("Error: n.parquet file not found in any expected location")
        print("Current directory:", os.getcwd())
        print("Directory contents:", os.listdir("."))
        sys.exit(1)
    
    abs_path = os.path.abspath(parquet_file)
    file_uri = f"file://{abs_path}"
    
    try:
        print(f"Reading parquet file: {file_uri}")
        df = spark.read.parquet(file_uri)
        
        print("Parquet file schema:")
        df.printSchema()
        
        n = 1500
        print(f"Selecting up to {n} documents...")
        
        required_columns = ['id', 'title', 'text']
        for col in required_columns:
            if col not in df.columns:
                print(f"Error: Required column '{col}' not found in parquet file.")
                print(f"Available columns: {df.columns}")
                sys.exit(1)
        
        df = df.select(required_columns)
        
        total_count = df.count()
        print(f"Total records in parquet file: {total_count}")
        
        if total_count > n:
            df = df.sample(fraction=n / total_count, seed=42).limit(n)
        
        doc_count = df.count()
        print(f"Processing {doc_count} documents...")
        
        processed_docs = 0
        for row in tqdm(df.collect(), desc="Creating documents"):
            if create_doc(row):
                processed_docs += 1
        
        print(f"Successfully created {processed_docs} documents in the data directory")
        
        if processed_docs > 0:
            print("Sample of created documents:")
            for filename in os.listdir("data")[:5]:
                print(f"  - {filename}")
        
    except Exception as e:
        print(f"Error processing parquet file: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()


# df.write.csv("/index/data", sep = "\t")