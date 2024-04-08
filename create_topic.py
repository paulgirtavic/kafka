from kafka.admin import KafkaAdminClient, NewTopic
from sys import argv

admin_client = KafkaAdminClient(
    bootstrap_servers="34.142.63.76:19092"
)

topic_name = argv[1]
topic_list = []
topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
print("Topic " + topic_name + " was created")