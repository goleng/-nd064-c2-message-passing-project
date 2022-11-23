from __future__ import print_function
import random
import logging

import grpc
import location_event_pb2
import location_event_pb2_grpc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class eventGenerator:

    PORT =  f'localhost:50051'

    def __init__(self, id, latitude, longitude, creation_time) -> None:
        self.user_id = id 
        self.latitude = latitude
        self.longitude = longitude
        self.creation_time = creation_time
        self.port = eventGenerator.PORT

    def get_event(self):
        return location_event_pb2.LocationEventMessage(
            user_id = self.user_id,
            latitude = self.latitude,
            longitude = self.longitude,
            creation_time = self.creation_time
        )
    
    def send_paylod(self):
        logging.info('Sending sample payload...')
        channel = grpc.insecure_channel(self.port)
        stub = location_event_pb2_grpc.location_eventServiceStub(channel)
        response = stub.Create(self.get_event())
        logging.info('Payload sent...')
        payload_content = f'Payload content are -> user_id: {response.user_id}, lat: {response.latitude}, lon: {response.longitude}, time: {response.creation_time}'
        logging.info(payload_content)

    def run(self):
        self.send_paylod()


if __name__ == '__main__':
    id_ = random.randint(11, 250)
    lat = round(random.uniform(-27.1011, -78.2122), 4)
    lon = round(random.uniform(57.1234, 107.1234), 4)
    time_ = random.randint(1331856000000, 2579856000000)
    event_generator = eventGenerator(id_, lat, lon, time_)
    event_generator.run()