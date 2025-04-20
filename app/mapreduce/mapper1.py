#!/usr/bin/env python3
import sys
import os
import re
import string
from collections import defaultdict

def tokenize(text):
    # Remove punctuation and convert to lowercase
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    tokens = text.split()
    stopwords = {'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'in', 'to', 'of', 'for', 'with', 'on', 'at', 'this', 'that'}
    tokens = [token for token in tokens if token not in stopwords and len(token) > 1]
    return tokens

def main():
    for line in sys.stdin:
        filepath = line.strip()
        
        try:
            filename = os.path.basename(filepath)
            if '_' in filename:
                doc_id = filename.split('_')[0]
                doc_title = filename.split('_', 1)[1].replace('.txt', '')
            else:
                doc_id = filename.replace('.txt', '')
                doc_title = filename.replace('.txt', '')
            
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tokens = tokenize(content)
            
            term_freq = defaultdict(int)
            for token in tokens:
                term_freq[token] += 1
            
            doc_length = len(tokens)
            
            print(f"METADATA\t{doc_id}\t{doc_title}\t{doc_length}")
            
            for term, freq in term_freq.items():
                print(f"TERM\t{term}\t{doc_id}\t{freq}")
                
        except Exception as e:
            sys.stderr.write(f"Error processing file {filepath}: {str(e)}\n")

if __name__ == "__main__":
    main()