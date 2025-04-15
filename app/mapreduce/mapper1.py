import sys
import re
from collections import Counter

for line in sys.stdin:
    line = line.strip()
    fields = line.split('\t')
    if len(fields) != 3:
        continue

    doc_id, title, text = fields
    tokens = re.findall(r'\b\w+\b', text.lower())

    print(f"document_info\t{doc_id}\t{title}\t{len(tokens)}")

    for term in set(tokens):
        print(f"document_frequency\t{term}\t{doc_id}")
    
    tf_counts = Counter(tokens)
    for term, tf in tf_counts.items():
        print(f"term_frequency\t{term}\t{doc_id}\t{tf}")
