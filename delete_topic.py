from kafka.admin import KafkaAdminClient
from sys import argv

admin_client = KafkaAdminClient(
    bootstrap_servers="34.142.63.76:19092", 
    client_id='test'
)

topic_name = argv[1]
topic_list = []
topic_list.append(topic_name)
admin_client.delete_topics(topics=topic_list)
print("Topic " + topic_name + " was deleted")