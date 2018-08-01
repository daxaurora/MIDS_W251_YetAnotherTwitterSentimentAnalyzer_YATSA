import dateutil.parser
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from tweepy.streaming import StreamListener
import time
from tweepy import OAuthHandler
from tweepy import Stream
import twitter
from kafka import SimpleProducer, KafkaClient

access_token = "934581358344790016-yq3GrSkXlZoTM0Yskv9b2DoTNBz5YUW"
access_token_secret = "yaG9Qpjm5fmSyE2L5hU8uM54pvLbY5eC0iAzWms9Xd2CB"
consumer_key = "lMHMMbmNxtlLHcZA9Bqxh8h6w"
consumer_secret = "KAT8bFoZknI1TYnx19Gx6p4cQqMbTtFAUOoBf1ndmHQ7eskgEX"

SAMPLE_TWEET = '{"created_at": "Tue Jul 31 22:11:14 +0000 2018", "id": 1024417228400119809, "id_str": "1024417228400119809", "text": "RT @Astro_WRLD: Like to join an e-dating gc", "source": "\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e", "truncated": false, "in_reply_to_status_id": null, "in_reply_to_status_id_str": null, "in_reply_to_user_id": null, "in_reply_to_user_id_str": null, "in_reply_to_screen_name": null, "user": {"id": 787053226264698880, "id_str": "787053226264698880", "name": "\ud835\udd41\ud835\udd60\ud835\udd60\ud83d\ude80\ud83c\udf0f", "screen_name": "Astro_WRLD", "location": "Los Angeles, CA", "url": null, "description": "@Zendaya @Lakers @Dodgers @KingJames @B_Ingram13 @KendrickLamer @TrvisXX", "translator_type": "none", "protected": false, "verified": false, "followers_count": 1031, "friends_count": 847, "listed_count": 6, "favourites_count": 3467, "statuses_count": 5022, "created_at": "Fri Oct 14 22:11:26 +0000 2016", "utc_offset": null, "time_zone": null, "geo_enabled": false, "lang": "en", "contributors_enabled": false, "is_translator": false, "profile_background_color": "F5F8FA", "profile_background_image_url": "", "profile_background_image_url_https": "", "profile_background_tile": false, "profile_link_color": "1DA1F2", "profile_sidebar_border_color": "C0DEED", "profile_sidebar_fill_color": "DDEEF6", "profile_text_color": "333333", "profile_use_background_image": true, "profile_image_url": "http:\/\/pbs.twimg.com\/profile_images\/1024396180539682816\/Yf7iXooA_normal.jpg", "profile_image_url_https": "https:\/\/pbs.twimg.com\/profile_images\/1024396180539682816\/Yf7iXooA_normal.jpg", "profile_banner_url": "https:\/\/pbs.twimg.com\/profile_banners\/787053226264698880\/1533072450", "default_profile": true, "default_profile_image": false, "following": null, "follow_request_sent": null, "notifications": null}, "geo": null, "coordinates": null, "place": null, "contributors": null, "retweeted_status": {"created_at": "Tue Jul 31 08:02:38 +0000 2018", "id": 1024203669854539777, "id_str": "1024203669854539777", "text": "Like to join an e-dating gc", "source": "\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e", "truncated": false, "in_reply_to_status_id": null, "in_reply_to_status_id_str": null, "in_reply_to_user_id": null, "in_reply_to_user_id_str": null, "in_reply_to_screen_name": null, "user": {"id": 787053226264698880, "id_str": "787053226264698880", "name": "\ud835\udd41\ud835\udd60\ud835\udd60\ud83d\ude80\ud83c\udf0f", "screen_name": "Astro_WRLD", "location": "Los Angeles, CA", "url": null, "description": "@Zendaya @Lakers @Dodgers @KingJames @B_Ingram13 @KendrickLamer @TrvisXX", "translator_type": "none", "protected": false, "verified": false, "followers_count": 1031, "friends_count": 847, "listed_count": 6, "favourites_count": 3467, "statuses_count": 5021, "created_at": "Fri Oct 14 22:11:26 +0000 2016", "utc_offset": null, "time_zone": null, "geo_enabled": false, "lang": "en", "contributors_enabled": false, "is_translator": false, "profile_background_color": "F5F8FA", "profile_background_image_url": "", "profile_background_image_url_https": "", "profile_background_tile": false, "profile_link_color": "1DA1F2", "profile_sidebar_border_color": "C0DEED", "profile_sidebar_fill_color": "DDEEF6", "profile_text_color": "333333", "profile_use_background_image": true, "profile_image_url": "http:\/\/pbs.twimg.com\/profile_images\/1024396180539682816\/Yf7iXooA_normal.jpg", "profile_image_url_https": "https:\/\/pbs.twimg.com\/profile_images\/1024396180539682816\/Yf7iXooA_normal.jpg", "profile_banner_url": "https:\/\/pbs.twimg.com\/profile_banners\/787053226264698880\/1533072450", "default_profile": true, "default_profile_image": false, "following": null, "follow_request_sent": null, "notifications": null}, "geo": null, "coordinates": null, "place": null, "contributors": null, "is_quote_status": false, "quote_count": 0, "reply_count": 10, "retweet_count": 2, "favorite_count": 32, "entities": {"hashtags": [], "urls": [], "user_mentions": [], "symbols": []}, "favorited": false, "retweeted": false, "filter_level": "low", "lang": "en"}, "is_quote_status": false, "quote_count": 0, "reply_count": 0, "retweet_count": 0, "favorite_count": 0, "entities": {"hashtags": [], "urls": [], "user_mentions": [{"screen_name": "Astro_WRLD", "name": "\ud835\udd41\ud835\udd60\ud835\udd60\ud83d\ude80\ud83c\udf0f", "id": 787053226264698880, "id_str": "787053226264698880", "indices": [3, 14]}], "symbols": []}, "favorited": false, "retweeted": false, "filter_level": "low", "lang": "en", "timestamp_ms": "1533075074659"}'
class StdOutListener(StreamListener):
    def on_data(self, data):
        if data and ('delete' not in data):
            tweet_json = get_tweet_json(data)
            if tweet_json:
                producer.send_messages("twitter", tweet_json.encode('utf-8'))
                print(tweet_json)
        return True

    def on_error(self, status):
        print(status)


class Tweet(dict):
    def __init__(self, tweet_raw_json, encoding='utf-8'):
        super(Tweet, self).__init__(self)

        self['id'] = tweet_raw_json['id']
        self['geo'] = tweet_raw_json['geo']['coordinates'] if tweet_raw_json['geo'] else None
        self['text'] = tweet_raw_json['text']
        self['user_id'] = tweet_raw_json['user']['id']
        self['hashtags'] = [x['text'] for x in tweet_raw_json['entities']['hashtags']]
        self['timestamp'] = dateutil.parser.parse(tweet_raw_json[u'created_at']).replace(tzinfo=None).isoformat()
        self['screen_name'] = tweet_raw_json['user']['screen_name']


def connect_twitter():
    access_token = "934581358344790016-yq3GrSkXlZoTM0Yskv9b2DoTNBz5YUW"
    access_secret = "yaG9Qpjm5fmSyE2L5hU8uM54pvLbY5eC0iAzWms9Xd2CB"
    consumer_key = "lMHMMbmNxtlLHcZA9Bqxh8h6w"
    consumer_secret = "KAT8bFoZknI1TYnx19Gx6p4cQqMbTtFAUOoBf1ndmHQ7eskgEX"
    auth = twitter.OAuth(token=access_token,
                         token_secret=access_secret,
                         consumer_key=consumer_key,
                         consumer_secret=consumer_secret)
    return twitter.TwitterStream(auth=auth)


def get_tweet_json(data):
    if data and ('delete' not in data):
        tweet_raw_json = json.loads(data)

        if ('lang' in tweet_raw_json) and (tweet_raw_json['lang'] == 'en'):
            tweet_parsed = Tweet(tweet_raw_json)
            tweet_json = json.dumps(tweet_parsed)
            return json.dumps(tweet_json)
        else:
            return None
    else:
        return None


def get_next_tweet(twitter_stream, i):
    """
    Return : JSON
    """
    block = False  # True
    stream = twitter_stream.statuses.sample()
    tweet_in = None
    while not tweet_in or 'delete' in tweet_in:
        tweet_in = stream.next()
        tweet_parsed = Tweet(tweet_in)

    return json.dumps(tweet_parsed)


# def process_rdd_queue(twitter_stream, nb_tweets=5):
#    """
#     Create a queue of RDDs that will be mapped/reduced one at a time in 1 second intervals.
#    """
#    rddQueue = []
#    for i in range(nb_tweets):
#        json_twt = get_next_tweet(twitter_stream, i)
#        dist_twt = ssc.sparkContext.parallelize([json_twt], 5)
#        rddQueue += [dist_twt]
#
#    lines = ssc.queueStream(rddQueue, oneAtATime=False)
#    lines.pprint()


# sc = SparkContext(appName="PythonStreamingQueueStream")
# ssc = StreamingContext(sc, 1)

# twitter_stream = connect_twitter()
# process_rdd_queue(twitter_stream)

# try:
#    ssc.stop(stopSparkContext=True, stopGraceFully=True)
# except:
#    pass

# ssc.start()
# time.sleep(2)
# ssc.stop(stopSparkContext=True, stopGraceFully=True)

#tweet_json = get_tweet_json(SAMPLE_TWEET)
#print("tweet JSON:\n {}".format(tweet_json))

kafka = KafkaClient("kafka.spark:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
#stream.sample()
stream.filter(languages = ['en'],
              track = [
                 '#antman', '#ant-man', '#antmanandthewasp',
                 '#AntMan', '#Ant-Man', '#Ant-man', 'Antman',
                 'TheWasp', '#AntManandTheWasp', '#AntManAndTheWasp', '#Trump', '#trump'
                  ])
