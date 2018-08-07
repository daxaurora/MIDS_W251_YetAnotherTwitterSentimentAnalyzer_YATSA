import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

window_size = 30

def preprocess(tweet):
    # Manage errors if an empty tweet sneaks through the pipeline
    if tweet.get('text')==None:
        tweet_text = None
    else:
        tweet_text = tweet['text'].encode('ascii','ignore').split(" ")
    return tweet_text

def sentiment_analysis():
    return None


def main():
    sc = SparkContext(appName="ParseDataFromTwitterStream")
    # Above here is different in Walt's
    ssc = StreamingContext(sc, window_size)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # Below here is different in Walt's
    tweets = kvs.map(lambda x: json.loads(x[1]))
    tweets.count().map(lambda x:'Total number of tweets related to topic in this %d-second batch: %s'
                                % (window_size, x)).pprint()
    tweets.map(lambda tweet: preprocess(tweet)).pprint()
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()

# From Walt's code, for comparison:
if __name__ == "__main__":
        sparkConf = SparkConf().setAppName("TwitterSentimentAnalysis") \
            .set("spark.cassandra.connection.host", "cassandra1, cassandra2, cassandra3")

        sc = SparkContext(conf=sparkConf)
        session = SparkSession(sc)
        sqlContext = SQLContext(sc)
        ssc = StreamingContext(sc, 2)
        brokers, topic = sys.argv[1:]

        kvs = setup_kafka_stream()

        nlp = StanfordCoreNLP('http://localhost:9000')

        tweets = kvs.filter(lambda x: x is not None).filter(lambda x: x is not '').map(lambda x: json.loads(x[1]))
        tweets.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()

        sentiment_stream = tweets.map(lambda tweet: get_tweet_sentiment(tweet))
        sentiment_stream.foreachRDD(lambda rdd : save_tweet_to_cassandra(rdd))

        ssc.start()
        ssc.awaitTermination()
