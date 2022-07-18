import re
import sys
import numpy as np
from operator import add
import time
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark import SparkContext

def calculateVotes(nodes,rank):
    #Calculates votes to other nodes
    n=len(nodes)
    for node in nodes:
        yield (node,rank/n)


def splitFunction(nodes):
    #Fetches fromNodeId and toNodeId
    nodePair = re.split(r'\s+',nodes)
    return nodePair[0],nodePair[1]


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
	.config("spark.driver.memoryOverhead","2048")\
	.config("spark.executor.memoryOverhead","2048")\
	.config("spark.executor.cores","4")\
	.config("spark.task.cpus","1")\
	.config("spark.executor.instances","3")\
	.config("spark.default.parallelism","10")\
        .getOrCreate()
    
    N=685230
    tempRanks=None
    end=0
    rows=spark.read.text(sys.argv[1]).rdd.map(lambda x:x[0])
    sc=SparkContext.getOrCreate()

    links=rows.map(lambda nodes:splitFunction(nodes)).distinct().groupByKey().cache()

    start = time.time()
    ranks=links.map(lambda x:(x[0], 1.0/N))

    for i in range(int(sys.argv[2])):
    	start = time.time()    
        votes=links.join(ranks).flatMap(lambda y:calculateVotes(y[1][0],y[1][1]))
        ranks=votes.reduceByKey(add).mapValues(lambda vote:vote*0.85+0.15)
        if tempRanks is None:
		    tempRanks=ranks
	    else:
            diff=[np.absolute(x-y) for x, y in zip(tempRanks.map(lambda x:x[1]).collect(),ranks.map(lambda x:x[1]).collect())]
		    hasNotConverged= len(filter(lambda x: x > 0.01,diff))>0
           	tempRanks=ranks
		if not hasNotConverged:
			print("converged at %s" %(i))
			end = time.time()    				
			break
	  	
    
    print("\n RUNTIME FOR %s PARTITIONS AND %s ITERATIONS=%s"  %(10,int(sys.argv[2]),end-start))
    for (node,rank) in ranks.sortBy(lambda x:x[1],ascending=False).take(10):
    	print("%s has rank: %s." % (node,rank)) 
    spark.stop()
