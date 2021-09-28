#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="OnlyOnes")
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)


from pyspark.sql import Row,HiveContext, functions as F


# In[ ]:


from time import sleep
from IPython.display import clear_output

def get_sql_context_instance(spark_context):
            if ('sqlContextSingletonInstance' not in globals()):
               globals()['sqlContextSingletonInstance'] = HiveContext(spark_context)
            return globals()['sqlContextSingletonInstance']
        
sql_context = get_sql_context_instance(sc)



while(True):
    sql_context.sql("refresh table test")
    
    data = sql_context.sql("select * from test").orderBy("date", ascending=False).limit(5).collect()
    
    for i, row in enumerate(data):
        print("Tweet: " + data[i][0])
        print("Score: " + data[i][1])
        print("\n-------------------------------- TWEET --------------------------------\n")
    sleep(10)
    clear_output(wait=True)
    


# In[ ]:




