from confluent_kafka import Producer

# Dummy data
user_list = ["john", "rob", "paul", "tom", "luke"]

#Initiate connection to Kafka Broker and start Produce func
def Produce():
    p=Producer({'bootstrap.servers':'34.142.63.76:19092'})
    print('Kafka Producer has been initiated')
    
    def delivery_report(err, msg):
    #Called once for each message produced to indicate delivery result. Triggered by poll() or flush().
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered: "{}" to {} [partition {}]'.format(msg.value().decode('utf-8'),msg.topic(), msg.partition()))

    print('\n<Producing>')
    #Producing messages to specified topic
    for user in user_list:
        # immediate poll return , time of of block if data not available in buffer, if 0 and no data no message returned
        p.poll(0)
        p.produce('kafka-demo', user.encode('utf-8'), callback=delivery_report)
        # waiting for message to complete via flush
        p.flush()

    r=p.flush(timeout=5)
    if r>0:
        print('Message delivery failed ({} message(s) still remain, did we timeout sending perhaps?)\n'.format(r))
Produce()


    



