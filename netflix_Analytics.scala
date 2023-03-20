import configurations.{Base, SparkAppConfig}
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Example_scala extends App with SparkAppConfig with Base {
  //  val url = "https://raw.githubusercontent.com/mkuthan/example-spark/master/src/main/resources/data/employees.txt"
  //  val result = scala.io.Source.fromURL(url).mkString

  val netflixdf = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/Downloads/netflix1.csv")
  //netflixdf.printSchema()
//  netflixdf.show(10)
//
  println("remove the duplicates  values in the columns ")

  netflixdf.count
  netflixdf.filter(col("show_id").isNull || col("show_id") === "")

  println("Replace the null with default values ")

//    val dfcsvb=netflixdf.na.fill("UNKNOWN", Array("lockers_last_locker_group"));
//    dfcsvb.where("type ='UNKNOWN'").show(40)


  println("Replace the key with the respective values in the columns ")

  val map = Map("Movie" -> "cinema")
  println(netflixdf.filter("type in ('Movie')"))
  // change movie name in to cinema
  val dfcsvc = netflixdf.na.replace(Array("type"), map);
  dfcsvc.where("type in ('cinema')")


   println("Dropping Duplicate records ")


//  val dropDisDF = myFile.dropDuplicates("canceled_at","images_papv_signature_name")
//    println("dropDuplicates count of canceled_at & images_papv_signature_name : "+dropDisDF.count())
//    dropDisDF.show(10)

  import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min}

//
//      //Rename Column Name
//  netflixdf.withColumnRenamed("title","motion_picture").show(10)
//     //adding a column in the table values
//
//  netflixdf.withColumn("delivery_instruction_note", lit("USA")).withColumn("pickup_address_zip_code", lit("anotherValue")).show(10)
//
//    //Change Value of an Existing Column //add and multiply row data
//  netflixdf.withColumn("pickup_address_zip_code", col("pickup_address_zip_code") * 100)
//
//    //Derive New Column From an Existing Column //add duplicate column
//  netflixdf.withColumn("CopiedColumn", col("pickup_address_zip_code") * -1).show(10)
//
//    //Change Column Data Type
//  netflixdf.withColumn("salary", col("salary").cast("Integer"))


//  val footercnt=netflixdf.filter("type like 'movie%'").show(10)
//
//  if (netflixdf == 1)
//  {
//    netflixdf.filter("_c0 like 'trailer%'").show
//  }
//  else
//    println(" Data seems to be having issue ")

  println("DSL Queries on DF | and rating>35")
//  netflixdf.select("*").filter("type='Movie' and country='India'").show(10)

//  netflixdf.where("show_id is null or show_id in (s939,s106,s117,s940)").show

  println("Number of partitions in the given DF " + netflixdf.rdd.getNumPartitions)

  val dfcsv=netflixdf.repartition(4)

  // after the shuffle happens as a part of wide transformation (groupby,join) the partition will be increased to 200

//  dfcsv.cache()
//  println(dfcsv.rdd.getNumPartitions)

//
//  val tempviewoutputasdf = spark.sql(
//    """ select _c0 as custid,_c4 as profession,
//                          case when _c4 like ('%Pilot%') then true else false end as IsAPilot,
//                         'CustInfo' as Typeofdata,concat(_c1," ",_c2) as fullname,
//                          cast(_c3 as string) as stringage,
//                          case when _c4 like ('%Pilot%') then _c4 else null end as Pilot,
//                          case when _c4 like ('%Stars%') then _c4 else null end as Stars
//                          from customer
//                          where _c4 not like '%Reporter%' and _c3 >20
//                          """).sort("custid")



  println("Applying UDF in DSL")


  //val ageRDD = spark.udf.register("ageRDD", (age: Int) => {
  //  if (age < 20)
  //    "TEEN"
  //  else if (age > 20 && age <= 32)
  //    "Young"
  //  else
  //    "Old"
  //})

//  val banknewdf = bankdf.withColumn("age", ageRDD(bankdf("age")))
//  //    val demo = bankdf.withColumn("anotherColumn_age", ageRDD(bankdf("age")))
  netflixdf.registerTempTable("netflix")


  println(" Spark SQL Query udf Register and Apply")

//
//  val dfsqltransformedaggr = spark.sql(
//    """select director,max(release_year) as maxage,min(release_year) as minage
//                 from netflix
//                 where release_year>2000
//                 group by director""")
//
//  dfsqltransformedaggr.show(5, false)






}





