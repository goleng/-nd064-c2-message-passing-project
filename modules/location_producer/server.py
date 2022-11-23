import os
import time 
import json 
import logging
from concurrent import futures

import grpc 
import location_event_pb2
import location_event_pb2_grpc
from kafka import KafkaProducer

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

# To test gRPC locally, please comment out KAFKA related code in this file!
KAFKA_URL = os.environ["KAFKA_URL"] 
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
producer = KafkaProducer(bootstrap_servers=KAFKA_URL)


class LocationEventService(location_event_pb2_grpc.location_eventService):
    def Create(self, request, context):
        logging.info('Received a message!')

        request_ = {
            'user_id': request.user_id,
            'latitude': request.latitude,
            'longitude': request.longitude,
            'creation_time': request.creation_time
        }
        print(request_)
        user_encoded_data = json.dumps(request_, indent=2).encode('utf-8')
        producer.send(KAFKA_TOPIC, user_encoded_data)
        return location_event_pb2.LocationEventMessage(**request_)

port = f'[::]:50051'
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
location_event_pb2_grpc.add_location_eventServiceServicer_to_server(LocationEventService(), server)
print('Server starting on port 50051...')
server.add_insecure_port(port)
server.start()
try:
    while True:
        time.sleep(3600)
except KeyboardInterrupt:
    server.stop(0)