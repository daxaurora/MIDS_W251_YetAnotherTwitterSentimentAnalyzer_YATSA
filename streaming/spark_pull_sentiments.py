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


def removeNonAscii(s): return "".join(filter(lambda x: ord(x)<128, s))


def get_sentiment(tweet_text):
    nlp = StanfordCoreNLP('http://localhost:9000')
    sentiment = 0
    if tweet_text != None:
        response = nlp.annotate(removeNonAscii(tweet_text).encode('ascii'),
                                properties={
                                    'annotators': 'sentiment',
                                    'outputFormat': 'json',
                                    'timeout': 1000,
                                })

	print("Sentiment response:\n{}".format(response))

        for s in response['sentences']:
	    sent_response = json.loads(s)
            ind = sent_response["index"]
            words = " ".join([t["word"] for t in sent_response["tokens"]])
            sent = sent_response["sentiment"]
            sent_value = sent_response["sentimentValue"]
            sentiment += derive_sentiment(sent, sent_value)
	
	print("Returning derived sentiment: {}".format(sentiment))

    return sentiment


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    tweets = kvs.map(lambda x: json.loads(x[1]))
    tweets.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()
    text_dstream = tweets.map(lambda tweet: json.loads(tweet)['text'])

    sentiments = text_dstream.map(lambda tweet_text: (get_sentiment(tweet_text), tweet_text))
    sentiments.pprint()
    ssc.start()
    ssc.awaitTermination()
