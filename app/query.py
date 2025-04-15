import sys
import math
from collections import defaultdict
from cassandra.cluster import Cluster

query_terms = sys.argv[1].lower().split()

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_bm25')

doc_data = {
    row.doc_id: {'title': row.title, 'len': row.doc_len}
    for row in session.execute("SELECT doc_id, title, doc_len FROM documents")
}

N = len(doc_data)
avg_len = sum(d['len'] for d in doc_data.values()) / N

df = {}
for term in query_terms:
    rows = list(session.execute("SELECT df FROM document_frequency WHERE term=%s", (term,)))
    df[term] = rows[0].df if rows else 0

tf = defaultdict(dict)
for term in query_terms:
    for row in session.execute("SELECT doc_id, tf FROM term_frequency WHERE term=%s", (term,)):
        tf[row.doc_id][term] = row.tf

def bm25(tf_val, df_val, dl, k1=1.5, b=0.75):
    idf = math.log((N - df_val + 0.5) / (df_val + 0.5) + 1)
    denom = tf_val + k1 * (1 - b + b * dl / avg_len)
    return idf * ((tf_val * (k1 + 1)) / denom)

results = [
    (doc_id, sum(
        bm25(tf_val, df[term], doc_data[doc_id]['len'])
        for term, tf_val in terms.items() if term in df
    ))
    for doc_id, terms in tf.items()
]

for doc_id, score in sorted(results, key=lambda x: -x[1])[:10]:
    print(f"[{score:.4f}] {doc_data[doc_id]['title']}")
