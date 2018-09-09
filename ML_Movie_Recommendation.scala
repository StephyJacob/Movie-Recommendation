import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object ML_Movie_Recommendation {
   
  def main(args: Array[String]){
      val conf = new SparkConf().setAppName("Movie_Spark_Project")
      val sc = new SparkContext(conf)
      val RatingsData = sc.textFile("/user/mrstephyjacob4610/Spark/Movie_Recommendation_Data/ratings.dat").map(x => x.split("::").take(3))
      val RatingsDataConverted = RatingsData.map {case Array(user,movie,ratings) => Rating(user.toInt,movie.toInt,ratings.toDouble)}
      val model = ALS.train(RatingsDataConverted, 50, 20, 0.01)
      val moviesdata = sc.textFile("/user/mrstephyjacob4610/Spark/Movie_Recommendation_Data/movies.dat")
      val titles = moviesdata.map(x => x.split("::").take(2)).map(array => (array(0).toInt,array(1).toString)).collectAsMap()
      println("Enter the User ID of the user for which Movies should be recommended")
      val UserID = scala.io.StdIn.readInt()
      println("Enter how many movies needs to be recommended")
      val k = scala.io.StdIn.readInt()
      val topKrecs = model.recommendProducts(UserID,k)
      val movierecommended = topKrecs.map {case Rating(user,movie,rating) => (movie)}
      println("Movies recommended for "+UserID+" are:")
      for ( i <- 0 to topKrecs.size-1){ println(titles(movierecommended(i)))}
 }
}