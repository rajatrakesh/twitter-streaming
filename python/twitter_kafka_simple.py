#!/usr/bin/env python

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import string

consumer_key = '8k5tdg9bHUXmmf3eukpz2LPuk'#eWkgf0izE2qtN8Ftk5yrVpaaI
consumer_secret = 'qI8UNZydEGtEUprh0WNpbbHvU9ah5s4NA39DopZYqSDczV7PR0'#BYYnkSEDx463mGzIxjSifxfXN6V1ggpfJaGBKlhRpUMuQ02lBX
access_token = '415386202-7S4Kab9QcBw83s84sOflFk8ikIEXx0M5xTDVlaTe'#1355650081-Mq5jok7mbcrIbTpqZPcMHgWjcymqSrG1kVaut39
access_token_secret = '0vpq5qJIh6MAqr9hFz1zHDVy05RfIRw4kfaeHWwS15Xwb'#QovqxQnw0hSPrKwFIYLWct3Zv4MeGMash66IaOoFyXNWs

mytopic='twitterstream'

class StdOutListener(StreamListener):
    def on_status(self, status):
        print '%d,%d,%d,%s,%s' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.text, status.user.screen_name)
	message =  str(status.user.followers_count) + ',' + str(status.user.friends_count) + ',' + str(status.user.statuses_count) + ',' + status.text + ',' + status.user.screen_name
        msg = filter(lambda x: x in string.printable, message)
	producer.send_messages(mytopic, str(msg))
        return True
    def on_error(self, status_code):
        print (status_code)

kafka = KafkaClient("rr-cdh-11.vpc.cloudera.com:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=['BigData','Hadoop','Cloudera','Analytics','bigdata','Predictive','cloudera'])
