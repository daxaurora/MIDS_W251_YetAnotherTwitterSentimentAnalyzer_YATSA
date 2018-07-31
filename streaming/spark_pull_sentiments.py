import json
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pycorenlp import StanfordCoreNLP


def derive_sentiment(sent, sentValue):
    if sent == 'Negative':
        return -1*sentValue
    else:
        return sentValue


def get_sentiment(tweet_text):
    nlp = StanfordCoreNLP('http://localhost:9000')
    sentiment = 0
    if isinstance(tweet_text, str) and tweet_text != None:
        response = nlp.annotate(tweet_text,
                                properties={
                                    'annotators': 'sentiment',
                                    'outputFormat': 'json',
                                    'timeout': 1000,
                                })

        for s in response['sentences']:
            ind = s["index"]
            words = " ".join([t["word"] for t in s["tokens"]])
            sent = s["sentiment"]
            sent_value = s["sentimentValue"]
            sentiment += derive_sentiment(sent, sent_value)
    return sentiment


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    tweets = kvs.map(lambda x: json.loads(x[1]))
    tweets.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()
    text_dstream = tweets.map(lambda tweet: tweet['text'])

    sentiments = text_dstream.map(lambda tweet_text: (get_sentiment(tweet_text), tweet_text))
    sentiments.pprint()
    ssc.start()
    ssc.awaitTermination()
