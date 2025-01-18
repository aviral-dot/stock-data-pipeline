import requests
import json
import time
from confluent_kafka import Producer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


class stock:

      def __init__(self):
        self.topic = "apple_data"
        self.conf = {'bootstrap.servers': 'localhost:9092'
                  }

      def delivery_callback(self,err, msg):
          if err:
              print('error:message failed delivery'.format(err))
          else:
              print(f"produced event")


      def produce_invoices(self,producer):
          response = requests.get(url)
          data = response.json()
          data = data['values'][0]
          datetime = data["datetime"]
          ee=json.dumps(data)
          eef = ee.encode('utf-8')
          producer.produce(self.topic, key=datetime, value=ee , callback=self.delivery_callback)
          producer.poll(1)


      def run(self, duration=2 * 60 * 60, interval=60):

         kafka_producer = Producer(self.conf)
         end_time = time.time() + duration
         try:
             while time.time() < end_time:
                   self.produce_invoices(kafka_producer)
                   kafka_producer.flush(10)
                   time.sleep(interval)
         except KeyboardInterrupt:
                pass



def run_stock_data():
    api_key = '06e0cc7c78e64ef2b8ac2324b6523e5a'
    symbol = 'AAPL'
    interval = '1min'
    url = f'https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&apikey={api_key}'
    stocks = stock()
    stocks.run(duration=2*60*60, interval=60)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 18),
    'email': ['aviralbharti832002@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_data_dag',
   default_args=default_args,
    description='A simple DAG to fetch stock data and produce Kafka events',
    schedule_interval=timedelta(days=1),
)

run_stock_data_task = PythonOperator(
    task_id='run_stock_data',
    python_callable=run_stock_data,
    dag=dag,
)

run_stock_data_task