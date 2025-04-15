#!/usr/bin/env python3

import sys
import re

def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text

for line in sys.stdin:
    line = line.strip()
    try:
        doc_id, doc_title, doc_text = line.split('\t', 2)
    except ValueError:
        continue

    cleaned_text = clean_text(doc_text)
    terms = cleaned_text.split()
    doc_length = len(terms)

    if doc_length == 0:
        continue

    # Using DOC_STAT as a special sign for the reducer
    print(f"{doc_id}\tDOC_STAT\t{doc_title}\t{doc_length}")

    tf = {}
    for term in terms:
        tf[term] = tf.get(term, 0) + 1

    # Emit term postings: term -> (doc_id, tf)
    # Using POSTING as a special sign for the reducer
    for term, freq in tf.items():
        print(f"{term}\tPOSTING\t{doc_id}\t{freq}")

