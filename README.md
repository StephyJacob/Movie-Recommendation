# Movie-Recommendation

Domain: Entertainment

Description
Group Lens Research has collected and made available rating data sets from the Movie Lens web site. The data sets were collected over 
various periods of time, depending on the size of the set. The film industry has seen many technical changes over the years, making itself 
adaptable to its evolving audience's mentality and contributing to that evolution. Using movies ratings & reviews, both target audiences 
and public reviews, we have a glimpse into the change process. Any of these data points, once analysed, may contribute to greater business 
intelligence. After analysing the users’ ratings, based on ages, occupations etc. the movie makers can have better understanding about 
the viewers’ choice expectations which in turn is beneficial for marketing of their movies. This is done by determining the relationship
between viewers and their ratings.

Analysis is performed for the below ideas
1. Find out the animated movies that are rated 4 or above?
2. Detect the gender bias on movie ratings for a genre?
3. Group the ratings by age?
4. Find out the average rating for movies?
5. Find out the titles of the best rated movies?

Machine Learning Model Created for Movie Recommendation using rating as a parameter

Data: http://files.grouplens.org/datasets/movielens/ml-1m.zip

Data Description: http://files.grouplens.org/datasets/movielens/ml-20m-README.html

Program Approach:
Created two scala files. One for analysis and one for Recommendation Model using Machine Learning

Technologies Used for analysis:
Spark, Spark SQL, Hortonwork Hadoop, HDFS

Technologies used for Recommendation Model:
Spark MLLIB, Hortonwork Hadoop, HDFS, Algoritmhm Used: ALS (Alternating Least Square) 
