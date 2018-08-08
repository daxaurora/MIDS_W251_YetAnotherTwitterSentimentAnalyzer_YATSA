from cassandra.cluster import Cluster

cluster=Cluster(['cassandra1','cassandra2','cassandra3'])
session=cluster.connect('w251twitter')
session.execute(
	"""
        INSERT INTO w251twitter.CONCEPTS (concept, hashtag_list)
        VALUES ('Trump',
        ['#Trump', '#trump'])
	""")
session.execute(
	"""
        INSERT INTO w251twitter.CONCEPTS (concept, hashtag_list)
        VALUES ('Ant Man and The Wasp',
        ['#antman', '#ant-man', '#antmanandthewasp',
        '#AntMan', '#Ant-Man', '#Ant-man', 'Antman',
        'TheWasp', '#AntManandTheWasp', '#AntManAndTheWasp'])
	""")
