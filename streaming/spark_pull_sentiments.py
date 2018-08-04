import datetime
import time
import json
import sys

from datetime import timedelta

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *


#import pyspark-cassandra
#$import pyspark_cassandra.streaming
#$from pyspark_cassandra import CassandraSparkContext
#import org.apache.spark.sql.cassandra.CassandraSQLContext


from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pycorenlp import StanfordCoreNLP


class CassandraPersist:

    def __init__(self):

        cluster=Cluster(['cassandra1','cassandra2','cassandra3'])
        self.session=cluster.connect('w251twitter')
        self.insert_sentiment_stmt = self.session.prepare("""
            INSERT INTO SENTIMENT(tweet_id, hashtags, tweet, geo, user_id, tweet_time, screen_name, sentiment, insertion_time)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
            """)
        self.insert_hashtag_stmt = self.session.prepare("""
            INSERT INTO HASHTAG(tweet_id, hashtag) VALUES (?, ?)
            """)


def save_sentiment_RDD(tweet):
    rdd = sc.parallelize([json.dumps(tweet)])

    rdd.saveToCassandra(
        "keyspace",
        "table",
	ttl=timedelta(hours=1),
    )

    print("****Saved Tweet with Sentiment:\n{}".format(tweet))


"""
def save_sentiment(self, tweet, sentiment):
# This method will save the sentiment for the DB for later pulling back by our visualization layer

    if tweet['hashtags']:
        for hashtag in tweet['hashtags']:
            self.session.execute(self.insert_hashtag_stmt, tweet['id'], hashtag)

        hashtags_set = "$" + " #".join(hashtags)
    else:
        hashtags_set = None

    self.session.execute(self.insert_sentiment_stmt, str(tweet['id']), hashtags_set, tweet['text'], tweet['geo'],
                    str(tweet['user_id']), tweet['timestamp'], tweet['screen_name'], sentiment)
        
    print("hashtags: {}".format(hashtags))
    print("tweet text: {}".format(tweet_text))
    print("sentiment: {}".format(sentiment))
"""

def derive_sentiment(sent, sentValue):
    if sent == 'Negative':
        return -1*sentValue
    else:
        return sentValue


def remove_non_ascii(s): return "".join(filter(lambda x: ord(x)<128, s))


def get_sentiment(tweet_text):
    sentiment = 0
    if tweet_text != None:

        print("Text to corenlop:\n{}".format(tweet_text))

        response = nlp.annotate(tweet_text,
                                properties={
                                    'annotators': 'sentiment',
                                    'outputFormat': 'json',
                                    'timeout': 10000,
                                })

        print("Sentiment response:\n{}".format(response))

        if isinstance(response, dict):
            for s in response['sentences']:
                print("\n****Sentence: \"{}\"i\n".format(s))
                ind = s["index"]
                print("ind: {}".format(ind))
                words = " ".join([t["word"] for t in s["tokens"]])
                print("words: {}".format(words))
                sent = s["sentiment"]
                print("sent: {}".format(sent))
                sent_value = s["sentimentValue"]
                print("sent_value: {}".format(sent_value))
                sentiment += derive_sentiment(sent, int(sent_value))
        
    print("Returning derived sentiment: {}".format(sentiment))

    return sentiment


def get_tweet_sentiment(tweet):
    tweet_dict = json.loads(tweet)
    print("tweet dict:\n{}".format(tweet_dict))
    
    hashtags = tweet_dict['hashtags']
    if (hashtags):
        tweet_text = remove_non_ascii(json.loads(tweet)['text'])
        sentiment = get_sentiment(tweet_text)
        tweet_dict['sentiment'] = sentiment
    
        # In this call, we'll save the sentiment to the DB:
        print("get_tweet_sentiment() : about to call cassandra_persist.save_sentiment({}, {})".format(tweet, sentiment))
        #save_sentiment_RDD(tweet)
        print_sent = {}
        print_sent['sentiment'] = sentiment
        print_sent['hashtags'] = " #".join(hashtags)

        return mapTweetDict(tweet_dict)
    else:
        return None
    

def setup_kafka_stream():
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    return kvs


def mapTweetDict(tweet_dict):
    print("Tweet dict:\n{}".format(tweet_dict))
    flattenedMap = (tweet_dict['id'], tweet_dict['hashtags'], tweet_dict['text'],
                    tweet_dict['geo'], tweet_dict['user_id'], tweet_dict['timestamp'],
                    tweet_dict['screen_name'], tweet_dict['sentiment'], datetime.datetime.now())
    return flattenedMap


def save_tweet_rdd(tweet_rdd):
    print("RDD:\n{}".format(tweet_rdd))
    schema = StructType([StructField("tweet_id",StringType(), nullable = False), \
             StructField("hashtags",StringType(), nullable = True), \
             StructField("tweet",StringType(), nullable = True), \
             StructField("geo",StringType(), nullable = True), \
             StructField("user_id",StringType(), nullable = True), \
             StructField("tweet_time",StringType(), nullable = True), \
             StructField("screen_name",StringType(), nullable = True), \
             StructField("sentiment",IntegerType(), nullable = True), \
             StructField("insertion_time",TimestampType(), nullable = False)])

    try:
        firstRow=tweet_rdd.first()
        tweet_rdd=tweet_rdd.filter(lambda row:row != firstRow)

        if not tweet_rdd.isEmpty():
            sqlContext.createDataFrame(tweet_rdd, schema).write \
                                                         .format("org.apache.spark.sql.cassandra") \
                                                         .mode('append') \
                                                         .options(table="sentiment", keyspace="w251twitter") \
                                                         .save()
    except ValueError:
        print("The RDD was empty...continuing...")

if __name__ == "__main__":
    sparkConf = SparkConf().setAppName("TwitterSentimentAnalysis") \
        .set("spark.cassandra.connection.host", "cassandra1, cassandra2, cassandra3")

    sc = SparkContext(conf=sparkConf)
    session = SparkSession(sc)
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]

    #cassandra_persist = CassandraPersist()

    kvs = setup_kafka_stream()
    
    nlp = StanfordCoreNLP('http://localhost:9000')

    tweets = kvs.filter(lambda x: x is not None).filter(lambda x: x is not '').map(lambda x: json.loads(x[1]))
    tweets.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()
#    tweets.foreachRDD(lambda rdd : rdd.saveAsTextFile( "tmp/tweet"+str(time.time())+".json" ))
    #text_dstream = tweets.map(lambda tweet: remove_non_ascii(json.loads(tweet)['text']))
    #sentiments = text_dstream.map(lambda tweet_text: (get_sentiment(tweet_text), tweet_text))


    sentiment_stream = tweets.map(lambda tweet: get_tweet_sentiment(tweet))
    sentiment_stream.foreachRDD(lambda rdd : save_tweet_rdd(rdd))
                                              
#                                  ['id_str', 'entities':['hashtags'], 'text',

#                                  ['id_str', 'entities':['hashtags'], 'text',

#                                  'user':['screen_name'], 'sentiment'])
#,
#	ttl=timedelta(hours=1),
    #)
    ssc.start()
    ssc.awaitTermination()
