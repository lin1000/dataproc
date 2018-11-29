# Example :  spark2-submit /home/training/wordcount_streaming2.py localhost 10000
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: StreamingRequestCount.py <hostname> <port>"
        exit(-1)

    sc = SparkContext()
    sc.setLogLevel("ERROR")
	
    #2 denote for 2 batchDuration
    ssc = StreamingContext(sc,2)
    #hostname='localhost'
    hostname = sys.argv[1]
    #port=1234
    port = int(sys.argv[2])

    # ----- Example 1: Count the number of requests for each user in each batch -----

    # create a new dstream with (userid,numrequests) pairs
    # count the total number of requests from each userID for each batch.
    logs = ssc.socketTextStream(hostname, port)
    userreqs = logs \
      .map(lambda line: (line.split(' ')[2],1)) \
      .reduceByKey(lambda v1,v2: v1+v2)

    #userreqs.pprint()
    #userreqs.saveAsTextFiles("counts")
    
    sortedDS = userreqs.map(lambda fields: (fields[1],fields[0])).transform(lambda rdd:rdd.sortByKey(ascending=False))
    sortedDS.foreachRDD(lambda time,rdd: printTop5(rdd,time))

    ssc.start()
    ssc.awaitTermination() 

def printTop5(r,t):
    print("Top keywords ", time)
    for count, keyword in r.take(5):
        print("keyword:",keyword, "("+ str(count) + ")")