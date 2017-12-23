## Info
Updated Version Support Indexing. 

Updated Time: Dec 23 2017

Members: Zhichun Wan, Zhiwei Zhang, Lihao Yu, and Rui Lan 

This file introduces how to compile and run the program for project track two. 

## Usage 
To build index: python3 build_index.py 

All the results are stored in "./index" folder for the use when process the query. 

If you need to create index for a file, change the file name inside the build_index.py script, and run by "python3 build_index.py"

To compile and run: python3 main_new.py

To use: 

\********************************************

A DB++ Group Product.

\********************************************

SELECT \<some attributes> \<enter>

FROM \<some tables> \<enter>

WHERE \<some conditions> \<enter>

Note that all of our .csv tables should be stored into the database folder, so 
that at the beginning of our program, we can read and store all the .csv files 
into dataframe for further use. 

Also note that there are some conventions when using our program: 

1. Rename format: 
FROM movies M1, movies M2

2. Put " " between every string, instead of single quote 

3. LIKE format:
LIKE "%Harry Potter%" for example

4. Need spaces between every parentheses

5. Add <DISTINCT, > to the SELECT stage to return distinct values 

6. Use <\*> in the SELECT stage to return all attributes in a table 

7. Need the full name of files in the FROM stage: review-1m.csv instead of review-1m 

## Examples 
To further illustrate our system, we include some examples of how the queries in our system like compared to q system. 

Ex.1 

q : 
SELECT R.review_id, R.stars, R.useful FROM review-1m.csv R WHERE R.stars >= 4 AND R.useful > 20

Our system:
SELECT R.review_id, R.stars, R.useful 
FROM review-1m.csv R
WHERE R.stars >= 4 AND R.useful > 20

Ex.2 

q : 
SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business.csv B JOIN review-1m.csv R ON (B.business_id = R.business_id) WHERE B.city = 'Champaign' AND B.state = 'IL'

Our system: 
SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful
FROM business.csv B, review-1m.csv R
WHERE B.business_id = R.business_id AND B.city = "Champaign" AND B.state = "IL"

Ex.3 

q : 
SELECT DISTINCT B.name FROM business.csv B JOIN review-1m.csv R JOIN photos.csv P ON (B.business_id = R.business_id AND B.business_id = P.business_id) WHERE B.city = 'Champaign' AND B.state = 'IL' AND R.stars = 5 AND P.label = 'inside'

Our system: 
SELECT DISTINCT, B.name
FROM business.csv B, review-1m.csv R, photos.csv P 
WHERE B.business_id = R.business_id AND B.business_id = P.business_id AND B.city = "Champaign" AND B.state = "IL" AND R.stars = 5 AND P.label = "inside"

Hopefully you have a good time using our system to query .csv files directly. 
Thanks a lot!

