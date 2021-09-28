#!/usr/bin/env python
# coding: utf-8

# # Server

# ## Importar las librerías

# In[ ]:


# Spark
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, HiveContext, functions as F
import sys, requests, findspark

# Otras
from deep_translator import GoogleTranslator
# IMPORTANTE: from vaderSentiment.vaderSentiment funciona en spark 3.0 para adelante
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
from operator import add
import json
# Para limpieza de texto
import re


# ## Establece las configuraciones de Spark y crea el contexto de Spark

# In[ ]:


sc = SparkContext("local[*]", "server")
globals()['sqlContextSingletonInstance'] = HiveContext(sc)


# #### El streaming context es un siervo del demoño, adorador de satán y de los pokimans

# In[ ]:


# Intervalos de 5 segundos
ssc = StreamingContext(sc, 5)


# ## Crea un DStream que se conecta a un puerto

# In[ ]:


# IMPORTANTE: ESTABLECER EL MISMO PUERTO EN CALLER Y SERVER
# Puerto usado: 9009 (MIRAR POR QUÉ FALLAN OTROS)
lines = ssc.socketTextStream("localhost", 9009)


# ## Preprocesa los tweets recibidos

# In[ ]:


processedLines = lines.map(lambda line: re.sub(r'http\S+', '', line))                       .map(lambda line: re.sub('RT', '', line))                       .map(lambda line: re.sub('@\w+', '', line))                       .map(lambda line: re.sub('#', '', line))                       .map(lambda line: re.sub(':', '', line))


# ## Traducción al inglés

# In[ ]:


translator = GoogleTranslator(source='es', target='en')

def translateFunc(text):
    translated = ""
    try:
        translated = translator.translate(text)
    except:
        translated = ""
    return translated

translatedLines = processedLines.map(lambda line: translateFunc(line)).filter(lambda x: x != "")


# ## Análisis de sentimientos - Vader Sentiment Analysis

# In[ ]:


analyser = SentimentIntensityAnalyzer()
def sentiment_score(tweet):
    score = analyser.polarity_scores(tweet)
    return (tweet, str(score))


# ## Crear y guardar tablas

# In[ ]:


def get_sql_context_instance(spark_context):
            if ('sqlContextSingletonInstance' not in globals()):
               globals()['sqlContextSingletonInstance'] = HiveContext(sc)
            return globals()['sqlContextSingletonInstance']
        
def process_rdd(time, rdd):
            print("----------- %s -----------" % str(time))
            try:
                sql_context = get_sql_context_instance(rdd.context)
                # convierte el RDD a Row RDD
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                row_rdd = rdd.map(lambda w: Row(tweet=w[0], score=w[1], date=now))
                # crea un DF desde el Row RDD
                tweet_df = sql_context.createDataFrame(row_rdd)

                table_list=sql_context.sql("""show tables""")
                table_name=table_list.filter(table_list.tableName=="test").collect()

                if len(table_name)==0:
                    tweet_df.write.saveAsTable("test")
                else:
                    tweet_df.write.insertInto("test", overwrite=False)

                tweet_counts_df = sql_context.sql("select tweet, score from test")
                tweet_counts_df.show()
            except:
                e = sys.exc_info()
                print(e)


# ## Aplica Análisis de Sentimientos a los twits traducidas

# In[ ]:


analysedLines = translatedLines.map(lambda line: sentiment_score(line))

analysedLines.foreachRDD(process_rdd)


# In[ ]:


ssc.start()

ssc.awaitTermination()


# In[ ]:




