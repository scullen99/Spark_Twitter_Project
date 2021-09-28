# twitter_data_processing
Trabajo realizado en PySpark con Jupyter Notebook.

## Crear un sistema de procesamiento en real time de tweets en castellano usando Python y Pyspark.

[X] (CALLER) Crear un programa Python que se conectevía socket a la API de Twitter (https://stream.twitter.com) y que descargue in real time datos sobre algunas # de interés en castellano, enviar los datos recibidos al Spark Streamming Server.

[X] (SERVER) Crear una aplicación Spark StreammingServer para recibir y procesar los datos recibidos en tiempo real.

[X] (SQL) Guardar los datos sumarizados de SparkStreamming en una base de dados SQL.

[X] Traducir del castellano al inglés usando alguna API de Python vía Spark en el SERVER con los Tweets recibidos del CALLER.

[X] Aplicar análisis de sentimiento como vaderSentiment de Python via Spark en el SERVER con los Tweets recibidos del CALLER.

[X] Aplicar algún modelo de ML a los datos.
