from __future__ import print_function
from pprint import pprint 
import re
import sys
from operator import add

from pyspark.sql import SparkSession
from pprint import pprint

import time
#start_time = time.time()

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    start = time.time()


    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .config("spark.driver.memoryOverhead","2048")\
	.config("spark.executor.memoryOverhead","2048")\
	.config("spark.executor.cores","4")\
	.config("spark.task.cpus","1")\
	.config("spark.executor.instances","3")\
	.config("spark.default.parallelism","20")\
        .getOrCreate()


    partitions = 20
    iterations = 20
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    
    N = links.count()
    ranks = links.map(lambda node: (node[0],1.0/N))
    
    for iteration in range(iterations) :
        ranks = links.join(ranks).flatMap(lambda x : [(i, float(x[1][1])/len(x[1][0])) for i in x[1][0]])\
    .reduceByKey(lambda x,y: x+y)
    #sorted_ranks = ranks.top(5, key=lambda x: x[1])

    end = time.time()
    print(ranks.getNumPartitions)
    print("Power Iteration with %s partitions and %s iterations %s secs" % ( partitions, iterations, (end-start)))
    for link,rank in ranks.sortByKey().collect():
        print("%s has rank: %s." % (link, rank))
    
    spark.stop()    
