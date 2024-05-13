from kafka import KafkaConsumer
import report_pb2 
from datetime import datetime
import json

# Setup Kafka consumer
consumer = KafkaConsumer(
   'temperatures', #name to sub
   bootstrap_servers=['localhost:9092'],
   auto_offset_reset='latest',  # Start reading at the latest message
   group_id='debug',  # Consumer group ID
   enable_auto_commit=True  # Automatically commit offsets
)


# Deserialize the message
def de_msg(msg):
   report = report_pb2.Report()
   report.ParseFromString(msg) #deserialize binary to report

   return {
       'date': report.date,
       'degrees': report.degrees
   }


# Process messages
for i in consumer:
   msg_data = de_msg(i.value) #deserialize binary  msg_data

   print({
       'partition':i.partition,
       'key': i.key.decode('utf-8') if i.key else 'None',
       'date': msg_data['date'],
       'degrees': msg_data['degrees']
   })
