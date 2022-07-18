from __future__ import print_function
from pprint import pprint 
import re
import sys
from operator import add
import numpy as np
from pyspark.sql import SparkSession
from pprint import pprint
import time
from pyspark import SparkContext

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    start = time.time()


    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .config("spark.driver.memoryOverhead","2048")\
	.config("spark.executor.memoryOverhead","2048")\
	.config("spark.executor.cores","4")\
	.config("spark.task.cpus","1")\
	.config("spark.executor.instances","3")\
	.config("spark.default.parallelism","30")\
        .getOrCreate()
    tempRanks=None
    mergedRanks=None
    end=0
    iteration=0

    partitions = 30
    iterations = 10
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    
    N = links.count()
    ranks = links.map(lambda node: (node[0],1.0/N))
    
    for iteration in range(iterations) :
        ranks = links.join(ranks).flatMap(lambda x : [(i, float(x[1][1])/len(x[1][0])) for i in x[1][0]])\
    .reduceByKey(lambda x,y: x+y)
        if tempRanks is None: 
		    tempRanks=ranks
	    else:
            diff=[np.absolute(x-y) for x, y in zip(tempRanks.map(lambda x:x[1]).collect(),ranks.map(lambda x:x[1]).collect())]
		    hasNotConverged=len(filter(lambda x: x > 0.01,diff))>0
           	tempRanks=ranks
		if not hasNotConverged: 
			print("converged at %s" %(iteration))
			end = time.time()    				
			break         
    
    sorted_ranks = ranks.top(5, key=lambda x: x[1])
    print(sorted_ranks)



    end = time.time()
    print(ranks.getNumPartitions)
    print("Power Iteration with %s partitions and %s iterations %s secs" % ( partitions, iterations, (end-start)))
    for link,rank in ranks.sortByKey().collect():
       print("%s has rank: %s." % (link, rank))
    
    spark.stop()    
