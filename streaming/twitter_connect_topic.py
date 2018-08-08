from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

# Twitter credentials
# Could configure this to get them from Cassandra
access_token="798563487886606336-VmKXDY8NYOnoCf6TXBA7jjnXWPFXlM2"
access_token_secret="cgsRhCZsipYTQuwbSWgRrcW76VEyrYOfGbUHMQBVvMq4j"
consumer_key="ZSBVncc7Ducoi1wpagg1cVwZx"
consumer_secret="iHzPeZe6mY5YhwK43EsQwnXHMfAEzvcAJ1nsR8LCGSpeeflCgk"

# Define stream
class StdOutListener(StreamListener):

    def on_data(self,data):
        if data and ('delete' not in data):
            producer.send_messages("twitter", data.encode('utf-8'))
        return True

    def on_error(self, status):
        # If the Twitter account used to produce tweets exceeds rate limit,
        # continued failed attempts to connect will exponentially increase
        # the time until the next successful connection.
        # This code will cut off the stream if it's maxed out and prevents
        # repeated attempts from delaying reconnection.
        # This solution is probably not needed for enterprise solutions
        # that don't have rate limiting.
        if status_code == 420:
            return False

# Set up Kafka producer from Twitter 
kafka = KafkaClient("localhost:9092")
producer=SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
# Filter for tracking hashtags is case sensitive
stream.filter(languages = ['en'],
              track = [
                 '#antman', '#ant-man', '#antmanandthewasp',
                 '#AntMan', '#Ant-Man', '#Ant-man', 'Antman',
                 'TheWasp', '#AntManandTheWasp', '#AntManAndTheWasp',
                  'trump', 'Trump'])
