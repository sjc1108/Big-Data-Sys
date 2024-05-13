
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import weather
import calendar
import time
import datetime
import report_pb2

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:  #for deleting existing kafka "temperatures"
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
    

except UnknownTopicOrPartitionError: #if topic dne
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) 

admin_client.create_topics([NewTopic("temperatures", num_partitions=4, replication_factor=1)]) #create new Kafka "temperatures" 





pro = KafkaProducer(bootstrap_servers=[broker], acks="all", retries=10) #init kafka producer 

for date, degree in weather.get_next_weather(delay_sec=0.1): #iterate weather data
	report = report_pb2.Report()
	report.date = date
	report.degrees = degree
     
	key = calendar.month_name[int(date[5:7])] #extract and use as key for partitioning 
#	print(str(key))

	res = pro.send("temperatures", report.SerializeToString(), bytes(key, "utf-8"))
#	print(str(res.get()))
