from pyspark import SparkContext, SparkConf
import json
import time
import sys 

input_filepath = sys.argv[1]
output_filepath = sys.argv[2]
n_partitions = int(sys.argv[3])

output = {"default": {}, 'customized':{}} 

conf = SparkConf().setAppName("task2")  
sc = SparkContext.getOrCreate(conf=conf)

#read json file 
rdd = sc.textFile(input_filepath).map(json.loads)  

#default partititon 
start1 = time.time()

default_partition = rdd.map(lambda x: (x["business_id"], 1)).reduceByKey(lambda a, b: a + b).cache()

# sort each parition and take only ten from it 
default_partition_sorted = default_partition.mapPartitions(lambda iterator: sorted(iterator, key=lambda x: (-x[1], x[0]))[:10])

# find final ten
default_output = default_partition_sorted.takeOrdered(10, key=lambda x: (-x[1], x[0]))

end1 = time.time()

output['default']['n_partition']=default_partition.getNumPartitions()
output['default']['n_items']= (default_partition.mapPartitions(lambda iterator:[sum(1 for _ in iterator)]).collect())
output['default']['exe_time']=end1-start1

#customized partition 
def top10_partition(key):
    return abs(hash(key))%n_partitions

start2 = time.time()

custome_partition = rdd.map(lambda x: (x["business_id"], 1)).partitionBy(n_partitions, top10_partition).reduceByKey(lambda a, b: a + b).cache()

# sort each parition and take only ten from it 
custom_partition_subsort = custome_partition.mapPartitions(lambda iterator: sorted(iterator, key=lambda x: (-x[1], x[0]))[:10])

# find final top ten records. 
custom_top10_businesses = custom_partition_subsort.takeOrdered(10, key=lambda x: (-x[1], x[0]))
end2 = time.time()

#log out put for customized paritition
output['customized']['n_partition'] = custome_partition.getNumPartitions()
output['customized']['n_items'] = (custome_partition.mapPartitions(lambda iterator: [sum(1 for _ in iterator)]).collect())
output['customized']['exe_time'] = end2 - start2


with open(output_filepath, "w") as f:
    json.dump(output,f)
sc.stop()

