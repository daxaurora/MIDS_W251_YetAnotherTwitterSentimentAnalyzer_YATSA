import sys
from cassandra.cluster import Cluster

cluster=Cluster(['cassandra1','cassandra2','cassandra3'])
session=cluster.connect('w251twitter')
session.execute(
	"""
	CREATE TABLE SENTIMENT (hashtag text, tweet text, sentiment int, insertion_time timestamp, PRIMARY KEY (hashtag, sentiment, insertion_time))
	""")

