import sys
import json
import time
from pyspark import SparkContext

review_file= sys.argv[1]
business_file = sys.argv[2]
output_filepath_a = sys.argv[3]
output_filepath_b = sys.argv[4]

sc = SparkContext.getOrCreate()

loading_start = time.time()
# get buiness_id and sum of stars
review = (sc.textFile(review_file) .map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], x['stars'])) 
    .filter(lambda x: x[1] is not None).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))) 

# get business_id,city from business 
business = (sc.textFile(business_file).map(lambda x: json.loads(x))
    .map(lambda x: (x['business_id'], x.get('city', "").strip())).filter(lambda x: x[1] != ""))

# inner join to get city, average star
parta = (business.join(review).map(lambda x: (x[1][0], x[1][1]))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: float(x[0]) / x[1]))

loading_end = time.time()
loading_time = loading_end-loading_start

# Sort by highest rating, then by city name
parta_ans = parta.sortBy(lambda x: (-x[1], x[0])).collect()

# Save to file
with open(output_filepath_a, 'w+') as output:
    output.write('city,stars\n') 
    for city, avg_stars in parta_ans:
        output.write(f"{city},{avg_stars}\n")

partb = {}
# task3 part b (python sort) M1
start1 = time.time()
m1 = sorted(parta.collect(), key=lambda x: (-x[1], x[0]))[:10]
print(m1) 
end1 = time.time()
partb["m1"] = (end1 - start1) + loading_time  

# task3 part b (spark sort) M2
start2 = time.time()
m2 = parta.takeOrdered(10, key=lambda x: (-x[1], x[0]))  
print(m2)  # Print top 10 cities
end2 = time.time()
partb["m2"] = (end2 - start2) + loading_time 

partb["reason"] = "the excuation time is almost identical, with spark being slightly faster"

# Save results
with open(output_filepath_b, 'w+') as output:
    json.dump(partb, output)