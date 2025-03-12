# Mining frequent itemsets with A-Priori algorithm

- Dataset:
  - small1.csv: user and business they visited
    - user1: [business11, business12, business13, ...], user2: [business21, business22, business23, ...]
  - small2.csv: business with users who visit it
    - business1: [user11, user12, user13, ...], business2: [user21, user22, user23, ...]
  - Ta Feng data: ta_feng_all_months_merged.csv.zip
    <link>source: https://www.kaggle.com/datasets/chiranjivdas09/ta-feng-grocery-dataset</link>
---

- Task1.py: based on user input find frequent pairs from data in either small1.csv or small2.csv format
---
- Task2.py: consider each user purchase in a day as a basket. Then find frequent pair using build model
