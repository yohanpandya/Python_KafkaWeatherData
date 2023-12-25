import time
import weather
from report_pb2 import Report
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer
broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])
print("starting producer. Going to delete topic \"temperatures\" if it exists")
try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

print("creating topic \"temperatures\" with replication factor 1 and 4 partitions")
topic_name = "temperatures"
num_partitions = 4
replication_factor = 1
new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
try:
    admin_client.create_topics([new_topic])
except TopicAlreadyExistsError:
    print("topic already exists, continuing the program")
print("Current topics:", admin_client.list_topics())


#producer
producer=KafkaProducer(
    bootstrap_servers=[broker],
    retries = 10,
    acks='all'
)
def get_month_name(date):
    from datetime import datetime
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    return date_obj.strftime("%B")
print("starting to read from weather.py. Sending messages with a protobuf object that has been encoded to utf-8 bytes and with delay 0.1 seconds")
print("please open new terminal window and run the consumer. you can do this with the following command:\n\n    docker exec -it p7 python3 /files/consumer.py 0 2\n\nwhere 0 and 2 specify the partitions. Feel free to choose any combination of partitions 0,1,2,3 on the next run.")
for date, degrees in weather.get_next_weather(delay_sec=0.1):
    report = Report()
    report.date = date
    report.degrees = degrees

    serialized = report.SerializeToString()
    month = get_month_name(date)
    result = producer.send("temperatures",key = bytes(str(month),"utf-8"),value = serialized)

