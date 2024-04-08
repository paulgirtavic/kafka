from confluent_kafka import Producer
import csv 
import json

# Define the path for csv file
csvFilePath = 'application_record_100.csv'

#Initiate connection to Kafka Broker and start Produce func
def Produce():
    p=Producer({'bootstrap.servers':'34.142.63.76:19092'})
    print('Kafka Producer has been initiated...')
    
    def delivery_report(err, msg):
        #Called once for each message produced to indicate delivery result. Triggered by poll() or flush().
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered: "{}" to {} message_key: {} [partition {}]'.format(msg.value().decode('utf-8'),msg.topic(), msg.key(), msg.partition()))

    print('\n<Producing>')

    #read csv file
    with open(csvFilePath, encoding='utf-8') as csvf: 

        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 

        #loop through dict and convert to json string
        for row in csvReader: 
            jsonString = json.dumps(row, indent=4)

            # Producing messages to specified topic, ensuring that ID is passed as key
            # immediate poll return , time of of block if data not available in buffer, if 0 and no data no message returned
            p.poll(0)
            p.produce('kafka-demo', jsonString, key=row['ID'], callback=delivery_report)
            # waiting for message to complete via flush
            p.flush()

    r=p.flush(timeout=5)
    if r>0:
        print('Message delivery failed ({})\n'.format(r))
Produce()