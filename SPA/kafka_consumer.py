from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import sys

KAFKA_CONSUMER_GROUP_NAME_CONS = "post-consumer-group"
KAFKA_TOPIC_NAME_CONS = "Post"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

try:
    myclient = MongoClient("mongodb://localhost:27017/")
    print("Connected successfully!!!")
    mydb = myclient['spa-assign']
    post = mydb['post']
except:
    print("Could not connect to MongoDB")

def post_data_preprocessing(data):
    user_id = data['user_id']
    user_name = data['user_name']
    action_reference_id = data['action_reference_id']
    community_id = data['community_id']
    communtiy_name = data['communtiy_name']
    community_description = data['community_description']
    group_id = data['group_id']
    group_name = data['group_name']
    group_description = data['group_description']
    action_data_id = data['action_data_id']
    action_name = data['action_name']
    is_owner = 'YES'
    action_owner_id = user_id
    receiver_id = 'NONE'
    timestamp = data['timestamp']
    action_description = data['action_description']
    action_data = data['action_data']
    action_id = data['action_id']
    
    users = {'user_id': user_id, 'user_name': user_name}
    communities = {'user_id': user_id, 'action_reference_id': action_reference_id, 'community_id': community_id, 'communtiy_name': communtiy_name, 'community_description': community_description}
    groups = {'user_id': user_id, 'action_reference_id': action_reference_id, 'group_id': group_id, 'group_name': group_name, 'group_description': group_description}
    actions = {'user_id': user_id, 'action_id': action_id, 'action_reference_id': action_reference_id, 'action_data_id': action_data_id, 'action_name': action_name, 'is_owner': is_owner, 'action_owner_id': action_owner_id, 'receiver_id': receiver_id, 'timestamp': timestamp}
    post = {'action_reference_id': action_reference_id, 'action_name': action_name, 'action_description': action_description, 'action_data': action_data}
    
    return {'users': users, 'communities': communities, 'groups': groups, 'actions': actions, 'post': post }

if __name__ == "__main__":

    print("Kafka Consumer Application Started ... ")
    try:
        consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME_CONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
        value_deserializer=lambda x: loads(x.decode('utf-8')))


        for message in consumer:
            print("Key: ", message.key)
            data = message.value
            print("Data received: ", data)
            # Posting the data to MongoDB.
            post.insert_one(post_data_preprocessing(message.value))
    except Exception as ex:
        print("Failed to read kafka data.")
        print(ex)
