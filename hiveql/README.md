# HiveQL
Dualcore recently started a loyalty program to reward their best customers. Dualcore has a sample of the data that contains information about customers who have signed up for the program, including their customer ID, first name, last name, email, loyalty level, phone numbers, a list of past order IDs, and a struct that summarizes the minimum, maximum, average, and total value of past orders.

Create the table loyalty_program.

```HiveQL
CREATE TABLE loyalty_program
  (cust_id INT,
  fname STRING,
  lname STRING,
  email STRING,
  loyalty_lvl STRING,
  phone MAP<STRING,STRING>,
  past_orders ARRAY<INT>,
  metrics STRUCT<min: INT, max: INT, avg: INT, total: INT>)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  COLLECTION ITEMS TERMINATED BY ','
  MAP KEYS TERMINATED BY ':';
```

Load the data in loyalty_data.txt into Hive. The data is located in the directory “/home/cloudera/training_materials/analyst/exercises/data_mgmt”.

`LOAD DATA LOCAL INPATH '/home/cloudera/training_materials/analyst/exercises/data_mgmt/loyalty_data.txt' INTO TABLE loyalty_program;`

Select the HOME phone number for customer ID 1200866.

`SELECT phone['HOME'] FROM loyalty_program WHERE cust_id = 1200866;`

### 408-555-4914

Select the third element from the order_ids for customer ID 1200866.

`SELECT past_orders[2] FROM loyalty_program WHERE cust_id = 1200866;`

### 5278505

---

Find how many products have been bought by the customer 1071189?

```HiveQL
SELECT COUNT(*) 
FROM orders o
JOIN order_details d ON (o.order_id = d.order_id)
GROUP BY o.cust_id
WHERE cust_id = 1071189;
```

### 9
#### Time taken: 31.277 seconds, Fetched: 1 row(s)

Find how many customers have spent more than 300000 on the total price of all products that s/he has bought?

```HiveQL
SELECT COUNT(*) FROM (
  SELECT o.cust_id, SUM(p.price)
  FROM order_details d
  JOIN orders o ON (o.order_id = d.order_id)
  JOIN products p ON (d.prod_id = p.prod_id)
  GROUP BY o.cust_id
  HAVING SUM(p.price) > 300000
) a;
```
### 16852
#### Time taken: 192.728 seconds, Fetched: 1 row(s)

List the customers (cust_id only) who have not placed any order.

```HiveQL
SELECT c.cust_id 
FROM orders o
FULL OUTER JOIN customers c ON (o.cust_id = c.cust_id)
WHERE o.order_id IS NULL;
```
### 1000051, ..., 1201372, 1201374 
#### Time taken: 49.1 seconds, Fetched: 9414 row(s)
---

Find the product with the lowest average rating among products with at least 50 ratings.

```HiveQL
SELECT prod_id, AVG(rating) AS average 
FROM ratings 
GROUP BY prod_id 
HAVING COUNT(*) > 50 
ORDER BY average ASC 
LIMIT 1;
```
### 1274673&nbsp;&nbsp;&nbsp;&nbsp;1.1025260029717683
#### Time taken: 59.933 seconds, Fetched: 1 row(s)


We observed earlier that customers are very dissatisfied with one of the products that Dualcore sells. Although numeric ratings can help identify which product that is, they don’t tell Dualcore why customers don’t like the product. We could simply read through all the comments associated with that output to learn this information, but that approach doesn’t scale. Now we want to analyze the comments to get more information on why customers don’t like the product.

Find the five most common trigrams (three-word combinations) in the comments for the product that you have identified in the previous question. Products’ comments are in the “message” field of table ratings.

```HiveQL
SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(message)), 3, 5)) 
FROM ratings 
WHERE prod_id = 1274673;
```
### {"ngram":["more", "than", "the"], "estfrequency":71.0}
### {"ngram":["one", "cost", "ten"], "estfrequency":71.0}
### {"ngram":["red", "one", "cost"], "estfrequency":71.0}
### {"ngram":["ten", "times", "more"], "estfrequency":71.0}
### {"ngram":["than", "the", "others"], "estfrequency":71.0}
#### Time taken: 33.571 seconds, Fetched: 5 row(s)


Among the patterns you see in the result of the previous question is the phrase “ten times more.” This might be related to the complaints that the product is too expensive. List the product’s (the product that you have identified in question 1 of this problem) comments that contain the phrase.

```HiveQL
SELECT message 
FROM ratings
WHERE prod_id = 1274673 
AND message LIKE "%ten time more%";
```
### Why does the red one cost ten times more than the others?
#### Time taken: 0.082 seconds, Fetched: 71 row(s)
