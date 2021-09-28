#!/usr/bin/env python
# coding: utf-8

# # CALLER

# ## Directorio de trabajo

# In[ ]:


import pathlib
pathlib.Path().absolute()


# ## Instalar todas las librerías

# In[ ]:


get_ipython().system('pip install tweepy requests requests_oauthlib')


# ## Importar las librerías

# In[ ]:


from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import socket, json, sys, requests, requests_oauthlib


# ## Keys necesarias

# In[ ]:


CONSUMER_KEY = 'jvrWQACEPOYqquDQwc8ywquTH'
CONSUMER_SECRET = 'NfTFBSsaumtnRsyngEYWi1OCuccVVNOLDKDlWBO9Dz24UpLw2D'
ACCESS_TOKEN = '710043709094682624-5l2aPw4AF6di5LfRgnqyZak8BEhjCrz'
ACCESS_SECRET = 'BE6dR5XwW1amtMCyKc4zoW2KkgR7ZayU656RNnQHcuW55'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


# ## Coge tweets de la api

# In[ ]:


def get_tweets():
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        #query_data = [('language', 'es'), ('locations', '-15,28,2,41'),('track','Israel')]
        #query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        response = requests.get(url, auth=my_auth, stream=True, params={"track": "Israel", "language": "es"})
        print(response)
        return response


# ## Manda los twits a Spark Streaming

# In[ ]:


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text'] + '\n'
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text.encode())
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
            print(line)
            continue


# ## Asigna valores y aplica funciones

# In[ ]:


# El puerto = que en server
TCP_IP, TCP_PORT, conn = "localhost", 9009, None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)


# In[ ]:




