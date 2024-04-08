from confluent_kafka import Consumer
import random, string, json, time
from sys import argv

# Passing topic name specified in CLI to program
topic = argv[1]

# Initiating consumer and connecting to Broker
def Consume():
    print('\n<Consuming>')
    c = Consumer({
        'bootstrap.servers': "34.142.63.76:19092",
        'group.id': 'rmoff',
        'auto.offset.reset': 'earliest'
    })

    # Subscribing to Topic and starting to consume all messages
    c.subscribe([topic])
    try:
        msgs = c.consume(num_messages=len(topic),timeout=3)

        if len(msgs)==0:
            print("No messages consumed)\n")
        else:
            for msg in msgs:
                print('Message received:  "{}" from topic {}\n'.format(msg.value().decode('utf-8'),msg.topic()))
    except Exception as e:
        print("Consumer error: {}\n".format(e))
    c.close()

Consume()


    



