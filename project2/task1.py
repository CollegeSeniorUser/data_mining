# 9:26
#current
import sys
from time import time
from collections import defaultdict
from pyspark import SparkContext
from itertools import combinations
from math import ceil
from itertools import combinations


def son1(basket_part, support, total_basket_count):
    basket_local = list(basket_part)  
    local_support = (support * (len(basket_local) / total_basket_count))  

    item_counter = defaultdict(int)

    for basket in basket_local:
        for item in basket: 
            item_counter[(item,)] += 1  

    frequent_single = {tuple(k) for k, v in item_counter.items() if v >= local_support}

    if not frequent_single:
        return []

    frequent_final = {1: frequent_single}
    current_set = frequent_single  

    basket_local = [set(basket).intersection(set().union(*frequent_single)) for basket in basket_local]

    k = 2 
    
    while current_set: 
        k_itemset = generate_kitemset(current_set, k)

        if not k_itemset:
            break  
        
        k_itemset_counter = defaultdict(int)
        for basket in basket_local:
            for itemset in k_itemset:
                if set(itemset).issubset(basket):  
                    k_itemset_counter[itemset] += 1  

        frequent_k_itemsets = {item for item, count in k_itemset_counter.items() if count >= local_support}

        if not frequent_k_itemsets:
            break

        frequent_final[k] = frequent_k_itemsets
        current_set = frequent_k_itemsets  
        k += 1  

    return list(frequent_final.values()) 


def generate_kitemset(current_set, k):
    
    return {
        tuple(sorted(set(set1) | set(set2))) 
        for set1, set2 in combinations(current_set, 2)
        if len(set(set1) | set(set2)) == k  
    }

from collections import defaultdict

def son2(subset, candidates):
    item_dict = defaultdict(int)
    baskets_set = set(subset) 

    for c in candidates:
        c_set = {c} if isinstance(c, str) else set(c)  

        if c_set.issubset(baskets_set): 
            item_dict[c] += 1 

    return item_dict.items()


def format_and_write(itemsets, label, output_file):
    with open(output_file, 'a') as output:
        output.write(f"{label}:\n")
        grouped = defaultdict(list)

        for x in itemsets:
            if isinstance(x, str):
                grouped[0].append(f"('{x}')")
            else:
                grouped[len(x) - 1].append(f"({', '.join([repr(item) for item in sorted(x)])})")

        for i in sorted(grouped.keys()):
            output.write(', '.join(sorted(grouped[i])) + '\n\n')
            
def main():
    # terminal input
    case = sys.argv[1]
    support = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]
        
    start = time()
    sc = SparkContext.getOrCreate()
    rdd = sc.textFile(input_file_path)
    header = rdd.first()
    rdd_csv = rdd.filter(lambda line: line != header)
    index = 0 if case == "1" else 1
    
    basket = (rdd_csv.map(lambda x: (x.split(',')[index], x.split(',')[1 - index]))
                  .groupByKey()
                  .mapValues(set)
                  .map(lambda x: x[1])).cache()
    
    total_basket_count = basket.count()
    
    candidates = basket.mapPartitions(lambda x: son1(x,support, total_basket_count)).flatMap(lambda x: x).distinct().sortBy(lambda x: (len(x), x)).collect()
    #print(candidates)
    frequent_itemsets = (basket.flatMap(lambda x: son2(x, candidates)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect())
    #print(frequent_itemsets)
    with open(output_file_path, 'w'): pass
    format_and_write(candidates, "Candidates", output_file_path)
    format_and_write(frequent_itemsets, "Frequent Itemsets", output_file_path)
    
    end = time()
    print('Duration:', end - start)
    
if __name__ == "__main__":
    main()
