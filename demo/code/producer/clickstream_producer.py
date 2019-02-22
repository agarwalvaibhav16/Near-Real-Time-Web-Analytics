# Libraries
from __future__ import print_function
import sys
import time
import itertools

import findspark
findspark.init("/opt/spark")

from kafka import KafkaProducer

# __name__
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    broker_list, topic = sys.argv[1:]
    producer = KafkaProducer(bootstrap_servers=broker_list)

    # Open file
    file = open('/home/centos/Downloads/final_project/clicks_data.csv')
    rdd_file = file.read()
    rdd_split = rdd_file.split('\n')

    # Loop trough file (the orders file has 500'000 rows)
    for batch in range(50000):
        print('Start batch #' + str(batch))
        for i in range(10):
            place = i + (batch * 10)
            print(rdd_split[place])
            producer.send(topic, rdd_split[place])
        print('Finish batch #' + str(batch))
        print('Sleep 1 minute')
        time.sleep(10)

    # End
    print('Finished!')
