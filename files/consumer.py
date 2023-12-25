import calendar
import time
import weather
from report_pb2 import Report
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaConsumer
from kafka import TopicPartition
import sys
import os
import json
broker = 'localhost:9092'
consumer = KafkaConsumer(
    bootstrap_servers=[broker]
)
print("consumer started and assigned to producer")
# Manual partition assignment
partitions2 = [int(partition) for partition in sys.argv[1:]]

assigned_partitions = []
for partition in partitions2:
    #print(partition)
    assigned_partitions.append(TopicPartition("temperatures", partition))

consumer.assign(assigned_partitions)
print("consumer has been assigned to the following partitions:",partitions2)
partitions = [TopicPartition('temperatures', int(x)) for x in partitions2]

paths = []
for partition in partitions2:
    paths.append(f'files/partition-{partition}.json')
print("created filepaths for each partition")
partition_data={}
for path in paths:
    if not os.path.exists(path):
        with open(path, 'w') as f:
            json.dump({'partition': int(path[16:17]), 'offset': 0}, f)
            f.close() 
    
    with open(path) as f:
        
        partition_data[int(path[16:17])] = json.load(f)
        #print(data)
        f.close()
    partNum = path[16:17]
    consumer.seek(TopicPartition('temperatures', int(partNum)),partition_data[int(partNum)]['offset'])
    
print("initialized files to contain partition number,",path[16:17],"offset",partition_data[int(partNum)]['offset'],".")
print("offset should be 0 if files didn't exist beforehand. If not, the offset should be non-zero.")
"""
for partition in partitions: 
    consumer.seek(partition, data['offset'])
    
    partition_data = {
        'partition': partition,
        'offset': data['offset'], 
        'stats': {}
        }
"""
print("\ninitializing infinite consumer loop. Type Ctrl-C to end consumer.")
print("after running for a few seconds, check the files directory to see the partition files that have been created. The partition files are large dictionaries that contain:\n\npartition #, offset #\nmonth\n    year\n        count(number of days)\n        sum(sum of temperatures)\n        avg(average temperature over the month)\n        end(end date)\n        start(start date)")
print("\nwith month starting at January and continuing, and year starting at 1990 (when the producer starts), and continuing")
print("after checking the partitions, please run\n\n    docker exec -it p7 python3 /files/plot.py\n\nwhich will generate month.svg. month.svg contains a graph that has the average max-temperature for the latest recorded year of \"January\", \"February\", and \"March\".\nYou can you any .svg visualizer to view the graph. Here is one:\n\n    https://www.svgviewer.dev/\n\nThanks for using this project!")
while True:
    batch = consumer.poll(1000)
    #print("polled")
    #print (batch)
    for tp, messages, in batch.items():
        #print(tp)
        #print(messages)
        for message in messages:
            #print(message)
            p = message.partition
            #print(p)
            #print(p)
            partition = TopicPartition('temperatures', int(p)) 
            my_report = Report()
            my_report.ParseFromString(message.value)
            report_string = str(my_report) 
            date = report_string.split('\n')[0].split(':')[1].replace('"','').strip(),
            #print(type(date))
            temp = float(report_string.split('\n')[1].split(':')[1].strip()) 
            #print(report_string.split('\n')[1])
            # Exactly once semantics
            #print(partition_data)
            #print(date,date[0][5:7])   
            month =calendar.month_name[int(date[0][5:7])]
            year = date[0][:4]
            #print(day)
            #print(month,year,temp)
            # Update stats
            path = 'files/partition-'+str(p) + '.json'
            #print(path)
            #    print("loaded")
            #print(partition_data)
            #print(month)
            #print(partition_data)
            date = date[0]
            #print(type(date))
            #print("date",date,"partition",partition_data)
            if month not in partition_data[p]:
                #print("month not in partition yet")
                partition_data[int(p)][month] = {year: {'count': 1, 
                                                        'sum': temp,
                                                        'avg': temp,
                                                        'start': date,
                                                        'end': date}}
                
            elif year not in partition_data[p][month]:
                #print("year not in partition yet")
                partition_data[p][month][year] = {'count': 1, 
                                                        'sum': temp,
                                                        'avg': temp,
                                                        'start': date,
                                                        'end': date}
            else:
                #print("both already in")
                #IF DAY IS less that equal to end: SKIP
                day = date[8:]
                if date<=(partition_data[p][month][year]['end']):
             #       print("repeat",partition_data[p][month][year]['end'], "is bigger than", date)
                    continue
                #print(partition_data[p][month][year]['count'])
                
                partition_data[p][month][year]['count'] += 1
                #print(partition_data[p][month][year]['count'])
                partition_data[p][month][year]['sum'] += temp
                partition_data[p][month][year]['avg'] = partition_data[p][month][year]['sum'] / partition_data[p][month][year]['count'] 
                partition_data[p][month][year]['end'] = date
                
        # Commit offset and write partition data  
        #print(consumer.position(partition), "\n")
        partition_data[p]['offset'] = consumer.position(partition)
        tmp_path = 'files/partition-'+str(p) + '.json.tmp'
        #print(tmp_path)
        with open(tmp_path, 'w') as f:    
            json.dump(partition_data[p], f)
            os.rename(tmp_path, 'files/partition-'+str(p) + '.json')
            f.close()

