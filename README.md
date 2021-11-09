# Twitter Data Processing
Trabajo realizado en PySpark con Jupyter Notebook.

## Iniciar con Jupyter Notebook
```python
jupyter notebook
```

## Iniciar con Python
```python

```


## Crear un sistema de procesamiento en tiempo real de tweets en castellano usando Python y Pyspark.

- [X] (CALLER) Crear un programa Python que se conectevía socket a la API de Twitter ([API Twitter](https://developer.twitter.com/en/docs/twitter-api/getting-started/about-twitter-api)) y que descargue in real time datos sobre algunas # de interés en castellano, enviar los datos recibidos al Spark Streamming Server.

- [X] (SERVER) Crear una aplicación Spark StreammingServer para recibir y procesar los datos recibidos en tiempo real.

- [X] (SQL) Guardar los datos sumarizados de SparkStreamming en una base de dados SQL.

- [X] Traducir del castellano al inglés usando alguna API de Python vía Spark en el SERVER con los Tweets recibidos del CALLER.

- [X] Aplicar análisis de sentimiento como vaderSentiment de Python via Spark en el SERVER con los Tweets recibidos del CALLER.

- [X] Aplicar algún modelo de ML a los datos.
