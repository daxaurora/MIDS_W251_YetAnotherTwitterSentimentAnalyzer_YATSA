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
    ssc = StreamingContext(sc, window_size)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    tweets = kvs.map(lambda x: json.loads(x[1]))
    tweets.count().map(lambda x:'Total number of tweets related to topic in this %d-second batch: %s'
                                % (window_size, x)).pprint()
    tweets.map(lambda tweet: preprocess(tweet)).pprint()
    # Sentiment analysis goes here, or possibly wrapped into the same function or map with preprocessing
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
