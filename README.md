# Amazon_Vines_Analysis

## Overview of Project

### Purpose of the Project
Our primary customer for this project is Jennifer, an analyst we worked with previously on a project for the company SellBy, who is looking to provide feedback on products to the SellBy stakeholders.  As part of this project we were asked to analyze Amazon reviews written by members of Amazon’s paid review program called Vine.  This program provides manufactures and publishers access to reviews on their products via a small fee.  Jennifer asked us to analyze the data provided by Amazon Vines and identify if there is any significant difference between the feedback from paid vs. unpaid user, particularly around 5-star ratings.  More directly, she wants to know if paid users are more likely than unpaid users to provide 5-star ratings.  


### Project Breakdown and Output
To answer this question, we used the publicly available [Amazon Reviews](https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt) website to identify our core dataset: [DVD v1 Reviews](https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Video_DVD_v1_00.tsv.gz).  This data set includes the following fields:
- marketplace (string): 2 letter country code of the marketplace where the review was written.
- customer_id (integer): Random identifier that can be used to aggregate reviews written by a single author.
- review_id (string): The unique ID of the review.
- product_id (string): The unique Product ID the review pertains to. In the multilingual dataset the reviews for the same product in different countries can be grouped by the same product_id.
- product_parent (integer): Random identifier that can be used to aggregate reviews for the same product.
- product_title (string): Title of the product.
- product_category (string): Broad product category that can be used to group reviews (also used to group the dataset into coherent parts).
- star_rating (integer): The 1-5 star rating of the review.
- helpful_votes (integer): Number of helpful votes.
- total_votes (integer): Number of total votes the review received.
- vine (string): Review was written as part of the Vine program.
- verified_purchase (string): The review is on a verified purchase.
- review_headline (string): The title of the review.
- review_body (string): The review text.
- review_date (string): The date the review was written.

To analyze the data, we used a combination of PySpark and SQL (PgAdmin) for both the ETL and analysis portion of our project.  For our initial load task, we utilized PySpark’s SparkFiles library to read the original CVS file and inspect the data field types.  


```
# Load the initial data from the S3 bucket and read the cvs as a dataframe
from pyspark import SparkFiles
url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Video_DVD_v1_00.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.option("encoding", "UTF-8").csv(SparkFiles.get("amazon_reviews_us_Video_DVD_v1_00.tsv.gz"), sep="\t", header=True, inferSchema=True)
df.show()

```

We then built four new DataFrames out of extracted and transformed data from this DataFrame: customer_df, products_df, review_id_df, and vines_df.  To support the transform step in the process, we needed to not only change the datatype for the ‘review_date’ (Step # 3b) but we also needed to drop duplicates for the product_id field (Step #2).  


**1. customer_df**

```
# 1. Create the customers_table DataFrame
# 1a. import the dependencies
from pyspark.sql.functions import count

# 1b. create the customer_table using the groupby & aggregate function
customers_df = df.groupby("customer_id").agg(count("customer_id").alias("count(customer_id)")).withColumnRenamed("count(customer_id)", "customer_count")
```

![customer_df](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/customer_df.png)



**2. products_df**

```
# 2. Create the products_table DataFrame and drop duplicates. 
products_df = df.select(['product_id', 'product_title']).drop_duplicates(['product_id'])
```

![products_df](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/products_df.png)



**3. review_id_df**

```
# 3. Create the review_id_table DataFrame. 

# 3a. import the appropriate dependencies
from pyspark.sql.functions import to_date

# 3b. use the to_date formula to change the review_date data type
review_id_df = df.select(['review_id', 'customer_id', 'product_id', 'product_parent', to_date('review_date', 'yyyy-MM-dd').alias('review_date')])
```

![review_id_df](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/review_id_df.png)



**4. vine_df**

```
# 4. Create the vine_table DataFrame
vine_df = df.select(['review_id', 'star_rating', 'helpful_votes', 'total_votes', 'vine', 'verified_purchase'])
```

![vine_df](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/vine_df.png)



Finally, we loaded these four new DataFrames into PgAdmin with four new tables for follow-on analysis: review_id_table, products_table, customers_table, and vine_table.  

