//https://github.com/Thomas-George-T/Movies-Analytics-in-Spark-and-Scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.{SparkConf, SparkContext}

object Movies_Analyticsextends extends App {

  // turn off logger messages in INFO level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


  val conf = new SparkConf().setAppName("Banking_Domain").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlc = new SQLContext(sc)
  //val hqlc = new org.apache.spark.sql.hive.HiveContext(sc)
  //sc.setLogLevel("ERROR");
  val spark = SparkSession.builder.appName("Banking_Domain").master("local[*]").getOrCreate


  val movies_rdd=sc.textFile("/home/ubuntu/Datamozart/datamozart_backup/Demo_Data/movies.dat")
//  1::Toy Story (1995)::Animation|Children's|Comedy
//  2::Jumanji (1995)::Adventure|Children's|Fantasy
//  3::Grumpier Old Men (1995)::Comedy|Romance
  val genre=movies_rdd.map(lines=>lines.split("::")(2))
//  it split second :: part________()
//  Animation|Children's|Comedy
//  Adventure|Children's|Fantasy
//  Comedy|Romance
  val flat_genre=genre.flatMap(lines=>lines.split("\\|"))
//  Animation
//  Children's
//  Comedy
//  Drama
//  Romance
  val genre_kv=flat_genre.map(k=>(k,1))
//  (Drama, 1)
//  (Romance, 1)
//  (Animation, 1)
//  (Children's, 1)
  val genre_count=genre_kv.reduceByKey((k,v)=>(k+v))
//  (Sci - Fi, 276)
//  (Comedy, 1200)
//  (Documentary, 127)
//  (Mystery, 106)
//  (Romance, 471)
  val genre_sort= genre_count.sortByKey()
//  (Film - Noir, 44)
//  (Action, 503)
//  (Adventure, 283)
//  (Animation, 105)

//  next processing data
//  1056 :: 1339 :: 3 :: 974952715
//  1056 :: 1345 :: 1 :: 974952596
//  1056 :: 2148 :: 3 :: 974952864
//  1056 :: 1347 :: 5 :: 974952632
//  1056 :: 2167 :: 5 :: 974952734
  val ratingsRDD=sc.textFile("/home/ubuntu/Datamozart/datamozart_backup/Demo_Data/ratings.dat")
  val movies=ratingsRDD.map(line=>line.split("::")(1).toInt)
//  1320(take first :: part (1).toInt
//  1339
//  1345
//  2148
  val movies_pair=movies.map(mv=>(mv,1))
//  (2527,1)
//  (2381,1)
//  (2528,1)
//  (2382,1)
  val movies_count=movies_pair.reduceByKey((x,y)=>x+y)
  val movies_sorted=movies_count.sortBy(x=>x._2,false,1)
//  (3174, 126)
//  (2160, 126)
//  (2872, 126)
//  (3210, 126)
//  (1204, 126)
  val mv_top10List=movies_sorted.take(10).toList
  val mv_top10RDD=sc.parallelize(mv_top10List)
//  (2858, 624)
//  (1196, 522)
//  (260, 516)
//  (1210, 513))
  val mv_names =movies_rdd.map(line=>(line.split("::")(0).toInt,line.split("::")(1)))
  val join_out=mv_names.join(mv_top10RDD)
//  (2571,(Matrix, The (1999),466))
//  (480,(Jurassic Park (1993),507))
//  (2028,(Saving Private Ryan (1998),472))
  val restult = join_out.sortBy(x=>x._2._2,false).map(x=> x._1+","+x._2._1+","+x._2._2).repartition(1)
//  restult.foreach(println)
//  2858, American Beauty (1999), 624
//  1196, Star Wars: Episode V - The Empire Strikes Back(1980), 522

//------------------------------------------------------------------------------------------------------------to df spliting ----------------------------------------------------
import spark.implicits._
  // Clean data into DataFrame
  //Extract the movie id
  //  1::Toy Story (1995)::Animation|Children's|Comedy
  //  2::Jumanji (1995)::Adventure|Children's|Fantasy
  //  3::Grumpier Old Men (1995)::Comedy|Romance
  val m_id=movies_rdd.map(lines=>lines.split("::")(0)).toDF("MovieID")

  //Extract the title
  val title = movies_rdd.map(lines => lines.split("::")(1))
  val m_title = title.map(x => x.split("\\(")(0)).toDF("Title")

  //Extract the year (1995) (1995)
  val year = movies_rdd.map(lines => lines.substring(lines.lastIndexOf("(") + 1, lines.lastIndexOf(")"))).toDF("Year")

  //Extract the m_genre
  val m_genre = movies_rdd.map(lines => lines.split("::")(2)).toDF("Genres")

  // For appending the dataframes, we need to import monotonically_increasing_id
  import org.apache.spark.sql.functions.monotonically_increasing_id
// seq (id) used for arrange the data in a order form
  val m_res1=m_id.withColumn("id", monotonically_increasing_id()).join(m_title.withColumn("id", monotonically_increasing_id()), Seq("id")).drop("id")

  val m_res2 = m_res1.withColumn("id", monotonically_increasing_id()).join(year.withColumn("id", monotonically_increasing_id()), Seq("id")).drop("id")

  val m_result = m_res2.withColumn("id", monotonically_increasing_id()).join(m_genre.withColumn("id", monotonically_increasing_id()), Seq("id")).drop("id")

  //m_result.show()
  //m_result.sort("MovieID","Title").show(false)
  //m_result.orderBy("MovieID","Year").show(false)

  //m_result.where("MovieID=1").show()


  //------------------------------------------------------------------------------------------------------------prepare_ratings.scala----------------------------------------------------

  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.types.{StructType, StructField, StringType};

  val schemaString = "UserID MovieID Rating Timestamp"
// how to split the header by string
  val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

  val rowRDD = ratingsRDD.map(_.split("::")).map(x ⇒ Row(x(0), x(1), x(2), x(3)))

  val ratings = spark.createDataFrame(rowRDD, schema)

  //ratings.show()
  //ratings.printSchema()






}