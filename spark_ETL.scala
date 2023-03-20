//https://github.com/manojkamothi/Spark-Hands-On/tree/master/AirlinesDemo

https://github.com/yennanliu/spark-etl-pipeline


-------------------------------------------------------------------------------airlinesDemo------------------------------------------------------------------------------
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._ 
    
    //online streming data
    //val airportDF = sqlContext.load("com.databricks.spark.csv", Map("path" -> "--path of CSV -airports.csv", "header" -> "true"))
    
    val airportDF = sqlContext.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/workspace/data/airports.csv")
    //airportDF.show
    
    airportDF.createOrReplaceTempView("airports")
    
    // Let's find out how many airports are there in South east part in our dataset
    sqlContext.sql("select AirportID, Name, Latitude, Longitude from airports where Latitude<0 and Longitude>0").collect
    
    // We can do aggregations in sql queries on Spark We will find out how many unique cities have airports in each country
    sqlContext.sql("select Country, count(distinct(City)) from airports group by Country").collect
    
    //What is average Altitude (in feet) of airports in each Country?
    sqlContext.sql("select Country , avg(Altitude) from airports group by Country").collect
    
    sqlContext.sql("select Country , avg(Altitude) from airports group by Country order by avg(Altitude) desc").collect
    
    //Now to find out in each timezones how many airports are operating?
    sqlContext.sql("select Tz , count(Tz) from airports group by Tz order by count(Tz) desc").collect.foreach(println)
    
    //We can also calculate average latitude and longitude for these airports in each country
    
   sqlContext.sql("select Country, avg(Latitude), avg(Longitude) from airports group by Country").collect.foreach(println)
    
    
    //And save it to CSV file
    //NorthWestAirportsDF.save("com.databricks.spark.csv", org.apache.spark.sql.SaveMode.ErrorIfExists, Map("path" -> "---HDFS SAVE PATH /airportprojects","header"->"true"))
    
    
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------


//https://github.com/manojkamothi/Spark-Hands-On/tree/master/Querying%20Using%20Spark2%20SQL%20With%20DataFrame%20and%20DataSets%20

//all basic etl in scala very important
---------------------------------------
//https://github.com/manojkamothi/Spark-Hands-On/blob/master/Querying%20Using%20Spark2%20SQL%20With%20DataFrame%20and%20DataSets%20/SparkSQLExample.scala


---------------------------------------------------withcolume------------------------------------------------------------

import cars.rawData
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source



object cars extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("cars")
    .getOrCreate()
  import spark.implicits._
  //define RDD with raw data
  val rawData = spark.read
    .format("csv")
    .option("header", "true") //first line in file has headers
    .option("inferSchema","true")
    .load("/home/ubuntu/workspace/data/statesPopulation.csv")

  //adding a column in the table
  val demo = rawData.withColumn("Country", lit("USA")).withColumn("anotherColumn", lit("anotherValue"))
  //demo.show()

  //Change Value of an Existing Column
  //add and multiply row data
  val demo1 = rawData.withColumn("Year",col("Year")*100)
  //demo1.show()

  //Derive New Column From an Existing Column
  //add duplicate column
  val demo2=rawData.withColumn("CopiedColumn",col("Year")* -1)
  //demo2.show

  //Change Column Data Type
  rawData.withColumn("salary",col("salary").cast("Integer"))

  //Rename Column Name
  rawData.withColumnRenamed("gender","sex")

//json formate explode

  import spark.implicits._
  val df = spark.read.json(Seq(jsonString).toDS())
  df.printSchema()

  val explodedDf = df.select($"hobbies.*").withColumn("indoor", explode($"indoor"))
  val explodedDf1 = df.select($"hobbies.*").withColumn("outdoor", explode($"outdoor"))

  val indoor = explodedDf.select("indoor").show()
  val outdoor = explodedDf1.select("outdoor").show()

}
------------------------------------------------------------hadoop_cluster--------------------------------------------------------------------------------------------
//https://sparkbyexamples.com/spark/spark-read-write-files-from-hdfs-txt-csv-avro-parquet-json/

log-WARN
--------

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.fs.{FileSystem,Path}


object cars extends App {
  // turn off logger messages in INFO level
  // and only show messages in WARN level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder
      .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  val myFile = spark.read.parquet("hdfs://192.168.0.50:54310/usr/paxel_dw/prod/shipments/")
  myFile.printSchema()

}

_-----------------------------------------------------------------------textfile__editer-----------------------------------------------------------------------------
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row


object cars {

  // Reducing the error level to just "ERROR" messages
  // It uses library org.apache.log4j._
  // You can apply other logging levels like ALL, DEBUG, ERROR, INFO, FATAL, OFF etc
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Defining Spark configuration to set application name and master
  // It uses library org.apache.spark._
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  // Main function where all operations will occur
  def main(args: Array[String]): Unit = {

    // Reading the text file
    val squidString = spark.sparkContext.textFile("/home/ubuntu/data_engin/Apache-Spark-Projects/CarsProject/cars.txt")

    // Defining the data-frame header structure
    val squidHeader = "time duration client_add result_code bytes req_method url user hierarchy_code type"

    // Defining schema from header which we defined above
    // It uses library org.apache.spark.sql.types.{StructType, StructField, StringType}
    val schema = StructType(squidHeader.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //schema.foreach(println)

    // Converting String RDD to Row RDD for 10 attributes
//    val rowRDD = squidString.map(_.split(" ")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9)))

//    // Creating data-frame based on Row RDD and schema
//    val squidDF = spark.createDataFrame(rowRDD, schema)
//
//    // Saving as temporary table
//    squidDF.registerTempTable("squid")
//
//    // Retrieving all the records
//    val allrecords = spark.sql("select * from squid")
//
//    // Showing top 5 records with false truncation i.e. showing complete row value
//    allrecords.show(5, false)
//
//
//    /* Further you can apply Spark transformations according to your need
//     */
//    allrecords.write.saveAsTable("allrecords")
//
//    // Printing schema before transformation
//    allrecords.printSchema()
  }
}
------------------------------------------------------------------Save-Table-Without-Explicit-DDL----------------------------------------------------------------------------------------

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.{SparkConf, SparkContext}

object Basic_spark extends App {

  // turn off logger messages in INFO level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  val conf = new SparkConf().setAppName("Banking_Domain").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlc = new SQLContext(sc)
  val spark = SparkSession.builder.appName("Banking_Domain").master("local[*]").getOrCreate


  val raw = scala.io.Source.fromURL("https://raw.githubusercontent.com/Thomas-George-T/Movies-Analytics-in-Spark-and-Scala/master/Movielens/users.dat").mkString
//  1::F::1::10::48067
//  2::M::56::16::70072
//  3::M::25::15::55117
//  4::M::45::7::02460
//  5::M::25::20::55455
val list = raw.split("\n").filter(_ != "")
  val rdd = sc.parallelize(list)
  import spark.implicits._
  val Data = rdd.toDF

  Data.createOrReplaceTempView("users_staging")
  val viewDF =
    spark.sql(""" Select
            split(value,'::')[0] as userid,
            split(value,'::')[1] as gender,
            split(value,'::')[2] as age,
            split(value,'::')[3] as occupation,
                split(value,'::')[4] as zipcode
from users_staging """)
  viewDF.createOrReplaceTempView("users")
  spark.sql("select * from users").show(10)


}

-----------------------------------------------------------------spark_Structre_scala_--------------------------------------------------------------------------------------------------------------------------------

https://github.com/ManikHossain08/Spark-ETL-Data-Pipeline-using-SparkStreaming-HDFS-Kafka-Hive


import configurations.{SparkAppConfig,Base}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
object Example_scala extends App with SparkAppConfig with Base {
  //  val url = "https://raw.githubusercontent.com/mkuthan/example-spark/master/src/main/resources/data/employees.txt"
  //  val result = scala.io.Source.fromURL(url).mkString

  import spark.implicits._
  val rawTrips:RDD[String] = sc.textFile("/home/ubuntu/Documents/demo.txt")
  val tripsRdd = rawTrips.map(x => x.split(","))


  //123234877,Michael,Rogers,14
  case class demo(RollNo: Int, FirstName: String, LastName: String,Age: Int)

  val demordd = tripsRdd.map(
              x => demo(x(0).toInt,
                x(1).replaceAll("\"", ""),
                x(2).replaceAll("\"", ""),
                x(3).toInt))

  val bankdf = demordd.toDF()
  bankdf.registerTempTable("demo")
  val age = spark.sql("select * from demo").show()