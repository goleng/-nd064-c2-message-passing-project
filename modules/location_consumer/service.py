from kafka import KafkaConsumer
import logging
import os
import json
from sqlalchemy import create_engine

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

DB_HOST = os.environ["DB_HOST"] 
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
KAFKA_URL = os.environ["KAFKA_URL"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

logging.info(f'connecting to kafka {KAFKA_URL}')
logging.info(f'connecting to kafka topic {KAFKA_TOPIC}')


class dbWriter:
    def __init__(self, location) -> None:
        self.location = location

    @staticmethod
    def get_db_cursor():
        connection_string = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        connection = create_engine(connection_string, echo=True)
        cursor = connection.cursor()
        return cursor

    def write_message(self):
        cursor = self.get_db_cursor()
        query = "INSERT INTO public.location (person_id, coordinate, creation_time) VALUES ({}, ST_POINT({}, {}), {})" \
            .format(
                int(self.location['user_id']),
                float(self.location['latitude']),
                float(self.location['longitude']),
                float(self.location['creation_time']))
        logging.info(query)
        cursor.execute(query)
        logging.info('Task completed successfully!')
    
    def run(self):
        self.write_message()

if __name__ == '__main__':
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=[KAFKA_URL])
    for topic in consumer:
        message = json.loads(topic.value.decode('utf-8'))
        logging.info(f'Writing {message} to DB')
        writer_ = dbWriter(message)
        writer_.run()
