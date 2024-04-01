from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import os
import random
import json
import requests
import logging

load_dotenv()

with open(
    "dags/movie_ids.json",
    "r",
) as f:
    filtered_movie_ids = json.load(f)


def get_data():
    API_KEY = os.getenv("API_KEY")
    random_movie_id = random.choice(filtered_movie_ids)
    url = f"http://www.omdbapi.com/?i={random_movie_id}&apikey={API_KEY}"
    response = requests.get(url)
    movie_data = response.json()
    logging.info(f"Received data: {movie_data}")
    return movie_data


def format_data(res):
    data = {
        "Title": res.get("Title", "N/A"),
        "Year": res.get("Year", "N/A"),
        "Director": res.get("Director", "N/A"),
        "Actors": res.get("Actors", "N/A"),
        "Plot": res.get("Plot", "N/A"),
        "Ratings": res.get("Ratings", []),
        "BoxOffice": res.get("BoxOffice", "N/A"),
    }
    logging.info(f"Formatted data: {data}")
    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers="broker:29092", max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 10:
            break
        try:
            res_dict = get_data()
            res_dict = format_data(res_dict)

            producer.send("movie_data", json.dumps(res_dict).encode("utf-8"))
        except Exception as e:
            logging.error(f"Error: {e}")
            continue


default_args = {"owner": "airscholar", "start_date": datetime(2023, 9, 3, 10, 00)}

with DAG(
    "user_automation",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="streaming_data_from_api",
        python_callable=stream_data,
    )
