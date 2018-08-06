from cassandra.cluster import Cluster

cluster=Cluster(['cassandra1','cassandra2','cassandra3'])
session=cluster.connect('w251twitter')


rows = session.execute('SELECT * FROM sentiment LIMIT 30')
for row in rows:
	print (row)


rows = session.execute('SELECT * FROM hashtag LIMIT 30')
for row in rows:
	print (row)
