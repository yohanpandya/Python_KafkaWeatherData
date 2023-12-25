This project uses Kafka to send messages about weather data from producer.py to consumer.py. The producer sends messages as protobuf objects, which is built during the docker build... process. The protobuf object is then converted into bytes using the "serializeToString" method. 
For simplicity, the producer sends the messages to 1 topic ("temperatures"), and the replication factor is 1. The producer sets up 4 partitions, and the consumer can choose to read from any of the four. The producer sends the partition # as the key of the message. 
After the consumer decodes the producer's messages, the consumer creates files "partition-x.json", which x is the partition number(s) that is specified by the user. The consumer then reads each message and puts it in the corresponding .json file, using exactly-once semantics (if the message is a repeat, the consumer won't put it in the .json file).
The partition files will be ordered like this:

{
  "partition": 2,
  "offset": 4117,

  "January": {
    "1990": {
      "count": 31,
      "sum": 599,
      "avg": 19.322580645161292,
      "end": "1990-01-31",
      "start": "1990-01-01"
    },
    "1991": {
      "count": 31,
      "sum": 178,
      "avg": 5.741935483870968,
      "end": "1991-01-31",
      "start": "1991-01-01"
    }
  },

  "April": {
    "1990": {
      "count": 30,
      "sum": 1113,
      "avg": 37.1,
      "end": "1990-04-30",
      "start": "1990-04-01"
    },
    ...
  ...
}

To end the consumer, press Ctrl-C.

Here is a step by step way to run the project.


1) docker build . -t p7

2) docker run -d -v ./files:/files --name=p7 p7

3) docker exec -it p7 python3 /files/producer.py

5) docker exec -it p7 python3 /files/consumer.py 0 2

6) docker exec -it p7 python3 /files/plot.py



MORE DETAILED EXPLANATION OF CONSUMER:


The consumer uses a large dictionary to minimize file I/O. The dictionary is ordered as follows:

  Top level keys are partition numbers (integers)
  Each partition contains keys for months (strings)
  Each month contains keys for years (integers)
  Each year contains the actual metric data like count, sum, avg, etc.

So it is nested:
  partition_data[partition_num][month_name][year][metrics]

The consumer processes each message as follows:

  Extracting the partition number and message value
  Parsing the message value into a Report protobuf
  Extracting the date, temperature and other fields from the Report
  Tracking metrics per partition, month and year like count, sum, avg, etc.
  Skipping messages if date is less than or equal to last end date (exactly once semantics)
  Updating the metrics for that partition/month/year
  Committing the offset and writing the updated partition metrics to file


    
