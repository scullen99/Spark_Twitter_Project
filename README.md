# Twitter Data Processing

- English
- Spanish

## Project explanation:
This project aims to create a real-time processing system for tweets in Spanish using Python and Pyspark. The project consists of several steps:

1. Create a Python program that connects to the Twitter API and downloads in real time data on some hashtags of interest in Spanish, and send the data received to the Spark Streaming Server.
2. Create a Spark Streaming Server application to receive and process the data received in real time.
3. Save the summarised Spark Streaming data in a SQL database such as SQLite.
4. Translate from Spanish to English using some Python API via Spark in the SERVER with the tweets received from the CALLER.
5. Apply sentiment analysis using Python vaderSentiment via Spark on the SERVER with the tweets received from the CALLER.

In short, the project consists of collecting tweets in Spanish in real time, processing them using Spark Streaming, storing them in a SQL database, translating them into English, performing sentiment analysis and applying a Machine Learning model to obtain a presentation of the data.

## Explicación del proyecto:
Este proyecto tiene como objetivo crear un sistema de procesamiento en tiempo real de tweets en español utilizando Python y Pyspark. El proyecto consta de varios pasos:

1. Crear un programa Python que se conecte a la API de Twitter y descargue en tiempo real datos sobre algunas hashtags de interés en español, y enviar los datos recibidos al Spark Streaming Server.
2. Crear una aplicación Spark Streaming Server para recibir y procesar los datos recibidos en tiempo real.
3. Guardar los datos sumarizados de Spark Streaming en una base de datos SQL como SQLite.
4. Traducir del español al inglés usando alguna API de Python vía Spark en el SERVER con los tweets recibidos del CALLER.
5. Aplicar análisis de sentimiento utilizando vaderSentiment de Python vía Spark en el SERVER con los tweets recibidos del CALLER.

En resumen, el proyecto consiste en recolectar tweets en español en tiempo real, procesarlos utilizando Spark Streaming, almacenarlos en una base de datos SQL, traducirlos al inglés, realizar análisis de sentimiento y aplicar un modelo de Machine Learning para obtener una presentación de los datos.
