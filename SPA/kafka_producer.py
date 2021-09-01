from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

# pip install kafka-python

KAFKA_TOPIC_NAME_1 = "Post"
KAFKA_TOPIC_NAME_2 = "Like"
KAFKA_TOPIC_NAME_3 = "Comment"
KAFKA_TOPIC_NAME_4 = "Share"
KAFKA_TOPIC_NAME_5 = "DM"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    input_data_list = []
    input_data = None
    for i in range(500):
        input_data = {}

        input_data = {
            'action_id': i, 
            'action_name': 'POST',
            'action_reference_id': 1275276452465,
            'user_id': 'Abdul_Kalam_abdul_kalam1@gmail.com',
            'user_name': 'Abdul Kalam',
            'group_id': 100045,
            'group_name': 'My Job Status',
            'group_description': 'Group in which we can post anything about our job.',
            'community_id': 200056,
            'communtiy_name': 'Job Community',
            'community_description': 'Community in which there are multiple groups for job search, sharing information about jobs etc.',
            'action_data_id': '1TEXT',
            'action_description': 'POST_TEXT',
            'action_data': 'Happy to share that Im joining Google as Software Engineer !!!!',
            'timestamp': str(datetime.now())
        } 

        print("Input Data: ", input_data)

        kafka_producer_obj.send(KAFKA_TOPIC_NAME_1, input_data)
        time.sleep(2)

    print("Kafka Producer Application Completed. ")