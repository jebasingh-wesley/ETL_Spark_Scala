//https://github.com/sagarnildass/Banking-Domain-Data-Analysis-with-Spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.{SparkConf, SparkContext}


import scala.Seq


object Banking_Domain extends App {

  // turn off logger messages in INFO level
  // and only show messages in WARN level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  val conf = new SparkConf().setAppName("Banking_Domain").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlc = new SQLContext(sc)
  //val hqlc = new org.apache.spark.sql.hive.HiveContext(sc)
  //sc.setLogLevel("ERROR");
  val spark = SparkSession.builder.appName("Banking_Domain").master("local[*]").getOrCreate


  val myFile = sc.textFile("hdfs://192.168.0.48:9000/user/hduser/bankmarketingdata.csv")
  //println(myFile.getClass)

  //split word
  val bank = myFile.map(x => x.split(";"))
  //println(bank.getClass)

  //drop header
  val bankf = bank.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

  import spark.sqlContext.implicits._

  case class Banks(age: Int, job: String, marital: String, education: String,
                   default_s: String, balance: Int, housing: String,
                   loan: String, contact: String, day: Int,
                   month: String, duration: Int, campaign: Int,
                   pday: Int, previous: Int, poutcome: String, Y: String)


  val bankrdd = bankf.map(
    x => Banks(x(0).toInt,
      x(1).replaceAll("\"", ""),
      x(2).replaceAll("\"", ""),
      x(3).replaceAll("\"", ""),
      x(4).replaceAll("\"", ""),
      x(5).toInt,
      x(6).replaceAll("\"", ""),
      x(7).replaceAll("\"", ""),
      x(8).replaceAll("\"", ""),
      x(9).toInt,
      x(10).replaceAll("\"", ""),
      x(11).toInt,
      x(12).toInt,
      x(13).toInt,
      x(14).toInt,
      x(15).replaceAll("\"", ""),
      x(16).replaceAll("\"", "")))

  val bankdf = bankrdd.toDF()
  bankdf.registerTempTable("bank")
  //bankdf.printSchema()
  //bankdf.select(max($"age")).show()
  //  bankdf.select(min($"age")).show()
  //  bankdf.select(avg($"age")).show()
  //  bankdf.select(avg($"balance")).show()

  val age = sqlc.sql("select age,count(*) as number from bank where Y ='yes' group by age order by number desc")

  val marital = sqlc.sql("select marital,count(*) as number from bank where Y='age' group by marital order by number desc")


  val ageRDD = sqlc.udf.register("ageRDD", (age: Int) => {
    if (age < 20)
      "TEEN"
    else if (age > 20 && age <= 32)
      "Young"
    else
      "Old"
  })

  val banknewdf = bankdf.withColumn("age", ageRDD(bankdf("age")))
  //    val demo = bankdf.withColumn("anotherColumn_age", ageRDD(bankdf("age")))
  banknewdf.registerTempTable("bank_new")
  val demo1 = sqlc.sql("select * from bank_new").show(10)
  //val age_taget = sqlc.sql("select age,count(*) as number from bank_new where y='age' group by age order by number desc")


  var agelnd = new StringIndexer().setInputCol("age").setOutputCol("ageIndex")

  var strlndModel = agelnd.fit(banknewdf)

  strlndModel.transform(banknewdf).select("age", "ageindex").show(100)


}