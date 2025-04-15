import sys
from collections import defaultdict
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS search_bm25 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace('search_bm25')

session.execute("""
CREATE TABLE IF NOT EXISTS term_frequency (
    term text,
    doc_id text,
    tf int,
    PRIMARY KEY (term, doc_id)
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS document_frequency (
    term text PRIMARY KEY,
    df int
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS documents (
    doc_id text PRIMARY KEY,
    title text,
    doc_len int
)
""")

doc_freq_dict = defaultdict(set)

for line in sys.stdin:
    line = line.strip()
    fields = line.split('\t')
    info = fields[0]

    if info == 'document_info':
        _, doc_id, title, doc_len = fields
        session.execute("INSERT INTO documents (doc_id, title, doc_len) VALUES (%s, %s, %s)", (doc_id, title, int(doc_len)))

    elif info == 'document_frequency':
        _, term, doc_id = fields
        doc_freq_dict[term].add(doc_id)

    elif info == 'term_frequency':
        _, term, doc_id, tf = fields
        session.execute("INSERT INTO term_frequency (term, doc_id, tf) VALUES (%s, %s, %s)", (term, doc_id, int(tf)))

for term, doc_ids in doc_freq_dict.items():
    session.execute("INSERT INTO document_frequency (term, df) VALUES (%s, %s)", (term, len(doc_ids)))
