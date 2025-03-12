# Data Exploration using SparkRDD

- data set: below are subset from yelp data <link> https://business.yelp.com/data/resources/open-dataset/ </link>
  - business.json: 192,609 business records. Primary_id by business.
    - Format:{"business_id":String,"name":String,"address":String,"city":String,"state":String,"postal_code":String,"latitude":float,"longitude":float,"stars":float,"review_count":int,"is_open":Bollean,"attributes":dictionary,"categories":String,"hours":String} 
 
  - test_review.json: individual review records. Primary_id by review_id
    - Format:  {"review_id":String,"user_id":String,"business_id":String,"stars":Int,"useful":Int,"funny":Int,"cool":Int,"text":String,"date":timestamp}
  --- 
- task1.py: explore data
  - tasks:    
    - The total number of reviews 
    - The number of reviews in 2018
    - The number of distinct users who wrote reviews
    - The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
    - The number of distinct businesses that have been reviewed (0.5 point)
    - The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
  - dataset: test_review.json
- task2.py:
  - goal: customized map reduce partition method to find top 10 business with highest review counts
  - dataset: test_review.json
- task3.py:
  - goal: top 10 cities with highest average stars
    - reduce excuation time using Spark 
  - dataset: join test_review.json and business.json
