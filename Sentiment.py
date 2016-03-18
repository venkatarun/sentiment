__author__ = 'suri'
import json

import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch


# import twitter keys and tokens
from config import *

# create instance of elasticsearch
es = Elasticsearch()

class TweetStreamListener(StreamListener):

    # on success
    def on_data(self, data):

        # decode json
        dict_data = json.loads(data)

        # pass tweet into TextBlob
        tweet = TextBlob(dict_data["text"])
        print dict_data["text"]
        # output sentiment polarity
        print tweet.sentiment.polarity

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # output sentiment
        print sentiment
        print dict_data["created_at"]

        # add text and sentiment info to elasticsearch
        es.index(index="sentiment",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "created_at": dict_data["created_at"],
                       "message": dict_data["text"],
                       "polarity": tweet.sentiment.polarity,
                       "subjectivity": tweet.sentiment.subjectivity,
                       "sentiment": sentiment})
        return True

    # on failure
    def on_error(self, status):
        print status

if __name__ == '__main__':

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()
    consumer_key = 'AdvOk65QJr9sHdLo9Y0YQ'
    consumer_secret = 'iPL2nXya0WWtPiKmRtHNwaW3z2vO5zkVOkHgVIFIqw'

    access_token = '134427325-uLdeNNLecxG29Kaa3S1TexABid0W8o5ecq77dFlF'
    access_token_secret = 'l2hWWlv7zW2KDpgFuluZ6glYITH5IuO7tLMR8v4bwIE'

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
#    print consumer_key
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    query = "life insurance india"
    max_tweets = 100

    twitterData = [{"tweetID": tweet.id_str, "Text": tweet.text,
                    "retweetCnt": tweet.retweet_count,
                    "favCnt": tweet.favorite_count, "source": tweet.source,
                    "createdAt": tweet.created_at, "userID": tweet.user.id_str,
                    "userScreenName": tweet.user.screen_name,
                    "userName": tweet.user.name,
                    "followersCnt": tweet.user.followers_count,
                    "friendsCnt": tweet.user.friends_count,
                    "favCnt": tweet.user.favourites_count,
                    "timeZone": tweet.user.time_zone,
                    "location": tweet.user.location}
                    for tweet in tweepy.Cursor(api.search, q=query,
                                                lang='en').items(max_tweets)]
    print twitterData
    print len(twitterData)
    # create instance of the tweepy stream

    #stream = Stream(auth, listener)

    # search twitter for "congress" keyword
    #stream.filter(track=['Life Insurance'])
