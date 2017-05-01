
# coding: utf-8

# In[ ]:

#get friends ids, followers ids and timeline of users

import requests
import tweepy
import psycopg2
import random
import time

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
cursor = database.cursor()

from twython import Twython 
from twython import TwythonRateLimitError
from twython import TwythonError
from twython import TwythonAuthError

t = Twython(app_key='CcPLR7gGCYAsWL5OGESzaSBXU',
    app_secret='LDyGXpavVLuBvQwlaX49ScbBa0LOKdgPY6izY14NnjsnfNB2h2',
    oauth_token='2179827990-xnhyW3hxAjRco0Ff50wEMXiXdt6JrWR9qAF6oLA',
    oauth_token_secret='y8iAJRPyAslCR8LPDMCnHUfB2uJOJ3lxW1oVBAFa31lqg')

cursor.execute("select id from t_gh_json")
ids = cursor.fetchall()
cursor.execute("create table ghtwdirectextra(user_id text, followerids text, friendids text)")
cursor.execute("create table ghtwdirecttimeline(user_id text, tweets text, tweetids text)")

for i in ids:
    k=""
    for j in i[0]:
        if j != "'":
            k = k + j
    print k
    try:        
        followers = t.get_followers_ids(user_id = k, count=5000) 
        #print followers['ids']
        friends = t.get_friends_ids(user_id = k, count=5000)
        #print friends['ids']
        timeline = t.get_user_timeline(user_id = k, count=200, include_rts=1)
        for i in timeline:
            #print i['text']
            #print i['id']
            cursor.execute("insert into ghtwdirecttimeline(user_id, tweets, tweetids) values(%s,%s,%s)",(k, i['text'], i['id']))
        cursor.execute("insert into ghtwdirectextra(user_id, followerids, friendids) values(%s,%s,%s)",(k, followers['ids'], friends['ids']))
        print "inserted"
    except TwythonRateLimitError as e:
        print "rate limit"
        time.sleep(60*15)
        continue
    except TwythonAuthError as e:
        continue
    except TwythonError as e:
        continue
cursor.close()
database.commit()
database.close()    

