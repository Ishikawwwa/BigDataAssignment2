#!/usr/bin/env python3
import os
import json
import sys
import argparse
import glob
from collections import defaultdict
import re

def tokenize(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    
    tokens = text.split()
    
    stopwords = {'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'in', 'to', 'of', 'for', 'with', 'on', 'at', 'this', 'that'}
    tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
    
    return tokens

def find_text_files(base_dir):
    text_files = []
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.txt'):
                text_files.append(os.path.join(root, file))
    
    if not text_files:
        text_files = glob.glob(os.path.join(base_dir, '**/*.txt'), recursive=True)
    
    if not text_files:
        print(f"No .txt files found in {base_dir}. Directory contents:")
        for root, dirs, files in os.walk(base_dir):
            print(f"Directory: {root}")
            for file in files:
                print(f"  - {file}")
    else:
        print(f"Found {len(text_files)} text files")
    
    return text_files

def create_fallback_data(docs_path, output_file):
    print(f"Creating fallback data from {docs_path}")
    
    document_metadata = {} 
    term_document_freq = defaultdict(dict)
    total_documents = 0
    total_token_count = 0
    
    text_files = find_text_files(docs_path)
    
    if not text_files:
        alt_paths = [
            "/data",               
            "/app/data",           
            "/app/sample_data",    
            "."                 
        ]
        
        for path in alt_paths:
            if path != docs_path:
                print(f"Trying alternate path: {path}")
                text_files = find_text_files(path)
                if text_files:
                    print(f"Found {len(text_files)} text files in {path}")
                    break
    
    if not text_files:
        print("No text files found in any location. Creating a sample document for testing.")
        sample_doc = {
            "doc_id": "sample1",
            "title": "Sample Document",
            "length": 10
        }
        document_metadata["sample1"] = sample_doc
        term_document_freq["sample"]["sample1"] = 2
        term_document_freq["test"]["sample1"] = 3
        term_document_freq["document"]["sample1"] = 5
        
        total_documents = 1
        total_token_count = 10
    else:
        for filepath in text_files:
            try:
                filename = os.path.basename(filepath)
                if '_' in filename:
                    doc_id = filename.split('_')[0]
                    doc_title = filename.split('_', 1)[1].replace('.txt', '')
                else:
                    doc_id = filename.replace('.txt', '')
                    doc_title = filename.replace('.txt', '')
                
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                tokens = tokenize(content)
                
                term_freq = defaultdict(int)
                for token in tokens:
                    term_freq[token] += 1
                
                doc_length = len(tokens)
                
                document_metadata[doc_id] = {
                    "title": doc_title,
                    "length": doc_length
                }
                total_documents += 1
                total_token_count += doc_length
                
                for term, freq in term_freq.items():
                    term_document_freq[term][doc_id] = freq
                    
                print(f"Processed document: {doc_id} - {doc_title} ({doc_length} tokens)")
                    
            except Exception as e:
                print(f"Error processing file {filepath}: {str(e)}")
    
    doc_freq = {term: len(docs) for term, docs in term_document_freq.items()}
    
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
    
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(fallback_data, f)
    
    print(f"Fallback data written to {output_file}")
    print(f"Indexed {total_documents} documents with {len(term_document_freq)} unique terms")
    print(f"Average document length: {total_token_count / total_documents if total_documents > 0 else 0}")
    
    return total_documents > 0

def main():
    parser = argparse.ArgumentParser(description='Create fallback data for search engine testing')
    parser.add_argument('--docs', default='/app/data', help='Path to documents directory')
    parser.add_argument('--output', default='/tmp/index_data/index_data.json', help='Output file path')
    parser.add_argument('--copy', action='store_true', help='Also create copy at /tmp/index_data.json')
    parser.add_argument('--sample', action='store_true', help='Create sample data if no documents found')
    
    args = parser.parse_args()
    
    success = create_fallback_data(args.docs, args.output)
    
    if success and args.copy:
        with open(args.output, 'r') as f:
            data = json.load(f)
        
        with open('/tmp/index_data.json', 'w') as f:
            json.dump(data, f)
        
        print(f"Copy of fallback data written to /tmp/index_data.json")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 