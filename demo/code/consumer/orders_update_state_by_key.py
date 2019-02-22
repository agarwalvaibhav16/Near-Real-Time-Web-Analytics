# Libraries
from __future__ import print_function
from operator import add, sub
import sys
import csv
import datetime,time

import findspark
findspark.init("/opt/spark/")

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime



# Parse function
def parse_data(line):
    s = line.rstrip().split(",")
    try:

        return [
            {"time": datetime.strptime(s[5], "%Y-%m-%d %H:%M:%S"),
             "orderId": int(s[0]), "customerId": int(s[1]),
             "platform": s[3],"is_order_placed":s[4]}]
    except Exception as err:
        print("Wrong line format (%s): " % line)
        return []

# Define update function
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_consumer3.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)
      
    # Get brokers and topic
    broker_list, topic = sys.argv[1:]

    # Open spark context
    conf = SparkConf().setAppName("PythonStreamingDirectKafkaWordCount")
    conf = conf.setMaster("local[3]")
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc,30)
    ssc.checkpoint("checkpoint")
     
    accum = sc.accumulator(0)
    # Open kafka stream
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": broker_list})
    filestream = kvs.transform(lambda rdd: rdd.values())
    
    # Parse file
    rdd_orders = filestream.flatMap(parse_data)

    def updateTotalCount(currentCount, countState):
    	if countState is None:
       	   countState = 0
           
    	return sum(currentCount) + (countState)

   
    def getSparkSessionInstance(sparkConf):
    	if ("sparkSessionSingletonInstance" not in globals()):
        	globals()["sparkSessionSingletonInstance"] = SparkSession \
            	.builder \
            	.config(conf=sparkConf) \
            	.getOrCreate()
    	return globals()["sparkSessionSingletonInstance"]




    ordersWindow = rdd_orders.map(lambda x: (x['platform'],1))
    totalCounts = ordersWindow.updateStateByKey(lambda x,y: (y or []) + x)
    
    order_count = totalCounts.map(lambda r:('device:' + str(r[0]),'value:' + str(len(r[1])),'current_date_time:"'+str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + '"'))
    order_count.saveAsTextFiles("/home/centos/Downloads/final_project/output_dir/running_count.txt")

    finalStream = order_count
    
    # Print result
    finalStream.pprint()

    ssc.start()
    ssc.awaitTermination()
