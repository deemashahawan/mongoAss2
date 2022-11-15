import Helpers._
import com.mongodb.client.model.Filters.{and, geoWithinCenter, gte, lte}
import com.mongodb.client.model.Indexes
import com.mongodb.spark.config.WriteConfig
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.mongodb.scala.model.Filters
import org.mongodb.scala.{MongoClient, MongoDatabase}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale
object Main {
  def main(args: Array[String]): Unit = {

    BasicConfigurator.configure(new NullAppender)

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkWithMongo")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/tweetsDB.tweets")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/tweetsDB.tweets")
      .getOrCreate();
    /*
           val dataset=spark.read
             .json("C:/Users/Deema/BD/Ass2mongodb/boulder_flood_geolocated_tweets.json")

                dataset.write.format("com.mongodb.spark.sql.DefaultSource")
                  .option("database","tweetsDB")
                  .option("collection","tweets")
                  .mode("append").save()
    */
    //step 1
      val sc = spark.sparkContext
      val df = spark.read
              .format("json")
              .load("C:/Users/Deema/BD/Ass2mongodb/boulder_flood_geolocated_tweets.json")

                val conf = WriteConfig(
                  Map("collection" -> "Tweets","writeConcern.w" -> "majority"),
                  Some(WriteConfig(sc))
                )
    ///////////////////////////////////////////
    //step 2
    def sf = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH)

    sf.setLenient(true)

    def parseDatetimeString(dateStr: String): Timestamp = {
      new Timestamp(sf.parse(dateStr).getTime)
    }

    import org.apache.spark.sql.functions.udf
    def toTimestamp = udf(parseDatetimeString _)
    val updatedDf1 = df.withColumn("created_at", toTimestamp (col("created_at")))//.show(3)
    //MongoSpark.save(updatedDf1, conf)

    ////////////////////////////////////////////////

    //step 3

     val mongoClient: MongoClient = MongoClient()
     val database: MongoDatabase = mongoClient.getDatabase("tweetsDB")
     val collection = database.getCollection("Tweets")
     collection.createIndex(Indexes.geo2dsphere("coordinates.coordinates")).printResults()
     collection.createIndex(Indexes.ascending("created_at")).printResults()

    ///////////////////////////////////////////////////////////////

    //step 4
    println("Enter radius : ")
    val r=scala.io.StdIn.readDouble()
    println("Enter (lon, lat) : ")
    val lon=scala.io.StdIn.readDouble()
    val lat=scala.io.StdIn.readDouble()
    println("Enter time (start, end) : ")
    val start=scala.io.StdIn.readLine()
    val End=scala.io.StdIn.readLine()
    println("Enter word to search : ")
    val w=scala.io.StdIn.readLine()

    val Format = new SimpleDateFormat("yyyy-MM-dd")
    val Starts =Format.parse(start)
    val Ends = Format.parse(End)

    val coll = collection.find(and(gte("created_at", Starts), lte("created_at", Ends),
       Filters.regex("text", w),
      geoWithinCenter("coordinates.coordinates", lon, lat, r))).results().toList

    val wordCount = coll.map(x => x.getString("text"))
    val wordCount2 = wordCount.flatMap(x => x.split(" "))
                     .count(_.equals(w))

    println(s"the number of occurrences of word ( ${w} ) published= "+wordCount2)





  }

}
