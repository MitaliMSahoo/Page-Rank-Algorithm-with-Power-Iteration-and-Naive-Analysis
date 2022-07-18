import re
import sys
from operator import add
import time
from pprint import pprint
from pyspark.sql import SparkSession


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
    
    start = time.time()
    
    spark = SparkSession\
        .builder\
        .appName("PageRankImplementation")\
	.config("spark.driver.memoryOverhead","2048")\
	.config("spark.executor.memoryOverhead","2048")\
	.config("spark.executor.cores","4")\
	.config("spark.task.cpus","1")\
	.config("spark.executor.instances","3")\
	.config("spark.default.parallelism","10")\
        .getOrCreate()

    rows=spark.read.text(sys.argv[1]).rdd.map(lambda x:x[0])

    links=rows.map(lambda nodes:splitFunction(nodes)).distinct().groupByKey().cache()

    ranks=links.map(lambda x:(x[0], 1.0))

    for i in range(int(sys.argv[2])):
    	votes=links.join(ranks).flatMap(lambda y:calculateVotes(y[1][0],y[1][1]))
        ranks=votes.reduceByKey(add).mapValues(lambda vote:vote*0.85+0.15)
    end = time.time()
    print("\n RUNTIME FOR %s PARTITIONS AND %s ITERATIONS = %s"  %(ranks.getNumPartitions(),sys.argv[2],end-start))
    
    for (node,rank) in ranks.sortBy(lambda x:x[1],ascending=False).collect():
    	print("%s has rank: %s." % (node,rank))
    
    spark.stop()
