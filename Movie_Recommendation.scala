//Importing all the packages that are necessary for the program execution//

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

//Defining case class outside the main method that are required for inferring the schema from the data //

  case class User(UserID:Int,Gender:String,Age:Int,Occupation:Int,Zipcode:String)
  case class Ratings(UserID:Int,MovieID:Int,Rating:Int,Timestamp:String)
  case class Movie(MovieID:Int,Title:String,Genre:Array[String])

//Defining the object and the main method//

object Movie_Recommendation {
  
   def main(args: Array[String]){
     
     //Creating Spark context 'sc' and Spark session 'spark'//
     
      val conf = new SparkConf().setAppName("Movie_Spark_Project")
      val sc = new SparkContext(conf)
      val spark = SparkSession.builder().appName("Movie_Spark_Project").getOrCreate()
      
    //Importing spark implicits for dataframe and dataset//
      
      import spark.implicits._
      
    //Creating RDD's for all the three data files using textFile function//
      
      val RatingsData = sc.textFile("/user/mrstephyjacob4610/Spark/Movie_Recommendation_Data/ratings.dat")
      val UserData = sc.textFile("/user/mrstephyjacob4610/Spark/Movie_Recommendation_Data/users.dat")
      val moviesdata = sc.textFile("/user/mrstephyjacob4610/Spark/Movie_Recommendation_Data/movies.dat")
      
    //Converting the RDD's in to Dataframes after spliting it and applying the case class defined above //
      
      val userdf = UserData.map(line => line.split("::")).map(x => User(x(0).toInt,x(1).toString,x(2).toInt,x(3).toInt,x(4).toString)).toDF
      val Ratingsdf = RatingsData.map(x => x.split("::")).map(x => Ratings(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toString)).toDF
      val moviesextractdf = moviesdata.map(x => x.split("::")).map(x => Movie(x(0).toInt,x(1).toString,x(2).split('|').toArray)).toDF
      
    //Joining all the three dataframes in to one single data frames using inner join //
      
      val RatingsUserMoviedf = Ratingsdf.join(userdf,"UserID").join(moviesextractdf,"MovieID")
      
    //Registering the joined dataframe in to a temporary table //  
      
      RatingsUserMoviedf.registerTempTable("RatingsUserMovieTable")
      
    //selecting the animated movies which is rated greater than four and printing it //
      
      val amrgreat4 = spark.sql("select Title,Rating,Genre FROM RatingsUserMovieTable WHERE array_contains(Genre,'Animation') and Rating >= 4").select("Title").distinct
      println("Animated movie that are rated above 4 are:"+"\n"+amrgreat4.collect.mkString("\n"))
     
    //selecting gender bias in rating by calculating the average of rating based on gender and grouping it based on Gender and displaying it //
      
      val Genderbiasactiondf = spark.sql("select Title,ROUND(Avg(Rating),2) as Average_rating,Gender from RatingsUserMovieTable where array_contains(Genre,'Action') group by Title,Gender order by Title,Gender")
      val  Genderbiasactioncount = Genderbiasactiondf.count.toInt
      println("Rating grouped by Gender to analyse Gender bias is as below:")
      Genderbiasactiondf.show(Genderbiasactioncount,false)
    
    //selecting average rating grouped by age and printing it //
      
      val AverageRatingAge = spark.sql("select Title,ROUND(AVG(Rating),2) AS Average_rating,Age FROM RatingsUserMovieTable GROUP BY Title,Age ORDER BY Title,Age")
      val AverageRatingAgecount = AverageRatingAge.count.toInt
      println("Rating grouped by Age is as below:")
      AverageRatingAge.show(AverageRatingAgecount,false)
      
    //selecting average rating grouped by Title  and printing it //
      
      val AverageRating = spark.sql("select Title,ROUND(AVG(Rating),2) AS Average_rating FROM RatingsUserMovieTable GROUP BY Title ORDER BY Title")
      val AverageRatingCount = AverageRating.count.toInt
      println("Average Rating for movies is as below:")
      AverageRating.show(AverageRatingCount,false)
      
    //Printing best rated movies based on Rating // 
      
      val ratingsorted = spark.sql("select Title,ROUND(AVG(Rating),2) as Average_rating FROM RatingsUserMovieTable GROUP BY Title ORDER BY Average_rating desc")
      val bestratedmovie = ratingsorted.filter(ratingsorted("Average_rating") === "5.0").select("Title")
      println("Best rated movies are:"+"\n"+bestratedmovie.collect.mkString("\n"))
   }
}