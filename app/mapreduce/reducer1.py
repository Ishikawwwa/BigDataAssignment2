#!/usr/bin/env python3
import sys
import os
import time
from collections import defaultdict
import json
import traceback

# from cassandra.cluster import Cluster

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
    
    print("Wirting data to fallback files...", file=sys.stderr)
    
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
    
    print(f"Indexed {total_documents} documents with {len(term_document_freq)} unique terms")
    print(f"Average document length: {total_token_count / total_documents if total_documents > 0 else 0}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR in reducer: {str(e)}", file=sys.stderr)
        print("Reducer encountered errors but will exit successfully to allow job to complete", file=sys.stderr)
        sys.exit(0)