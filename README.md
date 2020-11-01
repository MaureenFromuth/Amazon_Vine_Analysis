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


Of note, because we need to conduct further analysis on vine_df, we conducted additional transformations on that DataFrame.  Specifically, we removed any reviews that had less than 20 votes for them.  We also filtered out any reviews in which their helpful votes were less than 50% of the total votes.  To ensure we accurately filtered out the correct reviews, we sorted the resulting DataFrames.  

```
# Filter out any rows that had less than 20 total votes
vine_filter = vine_df.filter(vine_df.total_votes >= '20').sort(['total_votes'])

# Filter out any rows that had less thatn 50% of their total votes identified as helpful
vine_filter_2 = vine_filter.withColumn('helpful_rate', vine_filter['helpful_votes']/vine_filter['total_votes'])
vine_filter_help = vine_filter_2.filter(vine_filter_2.helpful_rate >= '.50').sort(['helpful_rate'], ascending=True)
```

![vine_filter](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/vine_filter.png)


![vine_filter_help](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/vine_filter_help.png)


Finally, we loaded these four new DataFrames into PgAdmin with four new tables for follow-on analysis: review_id_table, products_table, customers_table, and vine_table.  


## Results

Using the resulting analysis from our ETL process, we are able to begin answering Jennifer’s question of whether or not a user is more likely to provide a 5-star rating if they are a paid or an unpaid vine user.  Jennifer’s overall question was broken down into three sub-questions, which required additional transformation on the initial vine_filter_help DataFrame above.   To answer these sub-questions, we continued our analysis within PySpark.  


***1. How Many Vine Reviews and Non-Vine Reviews Were There?***

To answer this question, we first need to take the vine_filter_help DataFrame and break it out into two new ones: one for paid users and the other for unpaid users.  We executed this using the .filter() function with a conditional statement.  

```
# Create a dataframe that filters out the rows that were not paid vine subscribers
vine_filter_paid = vine_filter_help.filter(vine_filter_help.vine == 'Y')

# Create a complimentary dataframe that filters out, instead, all of the rows that were part of the paid subscription
vine_filter_unpaid = vine_filter_help.filter(vine_filter_help.vine == 'N')
```

We must then count the number of reviews for each filtered DataFrame.  To do this, however, we first had to download the sql function, count.  Of note, we are purposefully creating new variables rather than just identifying the count because we will be using this variable in our follow-on analysis.  The overall count for paid vine reviews and unpaid vine reviews, however, is 49 and 151,400 respectively.  

```
# Import sql function count
from pyspark.sql.functions import count

# Identify the total number of paid and unpaid responses for the filtered dataframes
total_paid = vine_filter_paid.count()
total_unpaid = vine_filter_unpaid.count()
```


***2. How Many Vine Reviews Were 5-Stars? How Many Non-Vine Reviews Were 5-Stars***

To answer this question, we have to break down the two filtered DataFrames and group the reviews by star rating.  Using the groupby() and agg() functions, we then applied the count() sql function we downloaded in the previous question.  For the purposes of visual cleanliness, we then sorted the results by ascending star order.  As you can see from the resulting DataFrames, paid Vine users had 9 of their 49 users give 5 stars and unpaid Vine users had 78,061 of their 151,400 users give a 5-star rating.  

```
# Identify the number of star ratings for each of the five star types for both the paid and unpaid filtered dataframes 
total_star_paid = vine_filter_paid.groupby("star_rating").agg(count("star_rating").alias("count(star_rating)")).withColumnRenamed("count(star_rating)", "total_star_reviews_paid").sort(['star_rating'], ascending=True)

total_star_unpaid = vine_filter_unpaid.groupby("star_rating").agg(count("star_rating").alias("count(star_rating)")).withColumnRenamed("count(star_rating)", "total_star_reviews_unpaid").sort(['star_rating'], ascending=True)
```

![Total Star Ratings](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/total_star_ratings.png)


***3. What Percentage of Vine Reviews Were 5 Stars?  What Percentage of Non-Vine Reviews Were 5 Stars***

To answer this question, we first need to build out a new column in each of the grouped DataFrames that provides the percentage of the total votes each star rating has.  To do that we used the ‘withColumn’ and created a new column called ‘percent_of_total_paid’.  This new column takes the ‘total_star_reviews_paid’ from the grouped DataFrame and divides it by the variable we defined earlier with the total reviews.  By multiplying that result by 100 we get the percentage of total reviews for each of the star options.  

```
# Create a new column that shows what percentage of the total responses for that filtered dataframes each of the five star ratings received
percent_paid = total_star_paid.withColumn('percent_of_total_paid',(total_star_paid['total_star_reviews_paid'] / total_paid) * 100)

percent_unpaid = total_star_unpaid.withColumn('percent_of_total_unpaid',(total_star_unpaid['total_star_reviews_unpaid'] / total_unpaid) * 100)
```

![Percent Total Reviews](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/percent_total_reviews.png)

While we could end there with the summary table, if we want to go one step further we can then filter down those resulting DataFrames and just show the results for the 5 star reviews.  To do so, we used the filter() function with a conditional statement.  This provides us with our final answer: 18.37% of paid users provided a 5-star rating vs. 51.56% of unpaid users.  

```
# Filter our all of the other percentages to show specifically how many responses and what percentage the 
# five star ratings received for each of the two filtered dataframes
percent_five_paid = percent_paid.filter(percent_paid.star_rating == '5')

percent_five_unpaid = percent_unpaid.filter(percent_unpaid.star_rating == '5')
```
 
![Percent Five-Star Reviews](https://github.com/MaureenFromuth/Amazon_Vine_Analysis/blob/main/percent_five_total.png)
 


## Summary

Overall, the data identifies a bias towards 5-star reviews for unpaid users rather than paid users.  Of particular note, the percentage of overall reviews from paid users is relatively low (49: 151,400 or .03%).  Although there is a 2.5x difference between the two percentage (18 vs 52), it would be ideal to look at more than one product review category.  As a recommended follow-on analysis, we should look at 3-4 additional product review types from Amazon’s publicly available data set, and compare them to validate if this trend is an outlier or if represents the greater population.  More specifically, we could use the a two-sample t-test to identify is two product types are statistically similar or not.  
