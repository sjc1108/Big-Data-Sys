from kafka import TopicPartition, KafkaConsumer
import json
import calendar
import sys
import report_pb2
import os

broker = 'localhost:9092'
partitions = sys.argv[1:]

def file_update(partition_data):
	file_name = f"/files/partition-{partition_data['partition']}.json"
	tmp = file_name + ".tmp"
	with open(tmp, 'w') as f:
		json.dump(partition_data, f)
	os.rename(tmp, file_name)


def data_partition_load(partition): #function for loading partition from file/init doesnt exist
    partition_file = f"/files/partition-{partition}.json"

    if not os.path.exists(partition_file):
        initial_data = {"partition": partition, "offset": 0} #init partition if not exist
        with open(partition_file, "w") as f:
            json.dump(initial_data, f)

    with open(partition_file, "r") as f: #for loading partition from file
        return json.load(f)

def process_messages(message, data_by_partition, topic_partition):
    report = report_pb2.Report()
    report.ParseFromString(message.value)

    message_date = report.date #extract date from msg
    year = message_date[:4]
    day = int(message_date.split('-')[2])
    month = calendar.month_name[int(message_date[5:7])]

    if month not in data_by_partition: #init month dict if doesnt exist
        data_by_partition[month] = {}

    if year not in data_by_partition[month]: #init year dict if not exist
        data_by_partition[month][year] = {
            'count': 1,
            'sum': report.degrees, #for summing
            'avg': report.degrees, #for avg
            'start': message_date,
            'end': message_date
        }
        
    else:
        year_data = data_by_partition[month][year]   #updating data existing year
        
        if year_data['end'] < message_date:
            year_data['end'] = message_date
            year_data['count'] += 1
            year_data['sum'] += report.degrees
            year_data['avg'] = year_data['sum'] / year_data['count']
            year_data['start'] = year_data['start']

    data_by_partition['offset'] = consumer.position(topic_partition) #offset update partition latest message
    
    return data_by_partition

consumer = KafkaConsumer(bootstrap_servers=[broker])

partitions = [int(partition) for partition in sys.argv[1:]] #change cmd line arg to int to create TopicPartition 
assigned_partitions = [TopicPartition("temperatures", p) for p in partitions]

consumer.assign(assigned_partitions)


partition_data = {partition: data_partition_load(partition) for partition in partitions} #partition data dict

for partition in partitions: #find consumer latest offset
    tp = TopicPartition("temperatures", partition)
    consumer.seek(tp, consumer.position(tp))

#msg loop
while True:
    batch = consumer.poll(1000) #pollllling
    print(batch)
    
    for tp, messages in batch.items():
        current_partition_data = partition_data[tp.partition]
        for message in messages:
            current_partition_data = process_messages(message, current_partition_data, tp) 
        file_update(current_partition_data)  #update partition w/ new msg

