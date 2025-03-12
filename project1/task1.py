from pyspark import SparkContext, SparkConf
import json
import sys
def main():
    if len(sys.argv) != 3:
        sys.exit(1)  

    input_filepath = sys.argv[1]  
    output_filepath = sys.argv[2]  

    conf = SparkConf().setAppName("task1")  
    sc = SparkContext.getOrCreate(conf=conf)
    try:

        rdd = sc.textFile(input_filepath).map(lambda x: json.loads(x))  
        
        #a The total number of reviews 
        n_review = rdd.count()
        #b Number of reviews in 2018
        n_review_2018 = rdd.filter(lambda x: "2018" in x["date"]).count()
        #c Number of distinct users who wrote reviews:
        n_user = rdd.map(lambda x: x["user_id"]).distinct().count()
        #d Top 10 users who wrote the most reviews:
        top_users = (
            rdd.map(lambda x: (x["user_id"], 1))
            .reduceByKey(lambda a, b: a + b)
            .takeOrdered(10, key=lambda x: (-x[1], x[0]))
        )

        top10_user = []
        for user, count in top_users:
            top10_user.append([user,count])
        
        #e Number of distinct businesses that have been reviewed
        n_business = rdd.map(lambda x: x["business_id"]).distinct().count()
        #f Top 10 Businesses with Most Reviews:
        top_businesses = (
            rdd.map(lambda x: (x["business_id"], 1))  
            .reduceByKey(lambda a, b: a + b)          
            .takeOrdered(10, key=lambda x: (-x[1],x[0]))    
        )

        top10_business = []
        for business, count in top_businesses:
            top10_business.append([business,count])
        output = {
        "n_review": n_review,
        "n_review_2018": n_review_2018,
        "n_user": n_user,
        "top10_user": top10_user,
        "n_business": n_business,
        "top10_business": top10_business
        }

        save_output(output, output_filepath)
    finally:
        sc.stop() 

def save_output(output, output_filepath):
    with open(output_filepath, "w") as f:  
        json.dump(output, f) 
    
if __name__=="__main__":
    main()