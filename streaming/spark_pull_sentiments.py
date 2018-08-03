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


def remove_non_ascii(s): return "".join(filter(lambda x: ord(x)<128, s))


def get_sentiment(tweet_text):
    nlp = StanfordCoreNLP('http://localhost:9000')
    sentiment = 0
    if tweet_text != None:
	#print("Text to corenlop:\n{}".format(tweet_text))

        response = nlp.annotate(tweet_text.encode('ascii'),
                                properties={
                                    'annotators': 'sentiment',
                                    'outputFormat': 'json',
                                    'timeout': 1000,
                                })

	#print("Sentiment response:\n{}".format(response))

        if isinstance(response, dict):
	    for s in response['sentences']:
	        #print("\n****Sentence: \"{}\"i\n".format(s))
                ind = s["index"]
                #print("ind: {}".format(ind))
	        words = " ".join([t["word"] for t in s["tokens"]])
                #print("words: {}".format(words))
                sent = s["sentiment"]
                #print("sent: {}".format(sent))
                sent_value = s["sentimentValue"]
                #print("sent_value: {}".format(sent_value))
                sentiment += derive_sentiment(sent, int(sent_value))
	
	#print("Returning derived sentiment: {}".format(sentiment))

    return sentiment


def save_sentiment(hashtags, tweet_text, sentiment):
# This method will save the sentiment for the DB for later pulling back by our visualization layer

    print("hashtags: {}".format(hashtags))
    print("tweet text: {}".format(tweet_text))
    print("sentiment: {}".format(sentiment))


def get_tweet_sentiment(tweet):
    tweet_dict = json.loads(tweet)
    #print("tweet dict:\n{}".format(tweet_dict))
    
    hashtags = tweet_dict['hashtags']
    if (hashtags):
        tweet_text = remove_non_ascii(json.loads(tweet)['text'])
        sentiment = get_sentiment(tweet_text)
    
        # In this call, we'll save the sentiment to the DB:
        #save_sentiment(hashtags, tweet_text, sentiment)
        print_sent = {}
        print_sent['sentiment'] = sentiment
        print_sent['hashtags'] = " #".join(hashtags)

        return (json.dumps(print_sent), tweet_text.decode())
    else:
	return None
    

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    tweets = kvs.map(lambda x: json.loads(x[1]))
    tweets.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()
    #text_dstream = tweets.map(lambda tweet: remove_non_ascii(json.loads(tweet)['text']))
    #sentiments = text_dstream.map(lambda tweet_text: (get_sentiment(tweet_text), tweet_text))
    sentiment = tweets.map(lambda tweet: get_tweet_sentiment(tweet))
    sentiment.pprint()
    ssc.start()
    ssc.awaitTermination()
