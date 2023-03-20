  //https://github.com/LearningJournal/Spark-Streaming-In-Scala
//https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-20-04
//su -l kafka
//Create topic
//------------
//bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mytopic
//bin/kafka-topics.sh --list --zookeeper localhost:2181
//Producer
//-------
//bin/kafka-console-producer.sh --broker-list localhost:9092 --topic mytopic
//Consumer
//----------
//bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic mytopic --from-beginning  (or)
//bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic mytopic --from-beginning

//sudo systemctl start kafka
//sudo systemctl enable zookeeper
//sudo systemctl enable kafka
//2.8.2 --version



  import org.apache.kafka.common.serialization.StringDeserializer
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.streaming.kafka010.KafkaUtils
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe



  object SparkStreaming extends App {

    val conf = new SparkConf()
      .setAppName("SparkStreaming")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")

    val sc = new SparkContext(conf)

    val streamingContext = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_demo_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("mytopic")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => record.value)
      .flatMap(line => line.split("[ ,\\.;:\\-]+"))
      .map(word => word.toLowerCase)
      .filter(_.size > 0)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .repartition(1)
      .transform(rdd => rdd.sortBy(-_._2))
      .saveAsTextFiles("/home/ubuntu/data_engin/output/words")

    streamingContext.start()
    streamingContext.awaitTermination()

}




//ThisBuild / version := "0.1.0-SNAPSHOT"
//
//ThisBuild / scalaVersion := "2.12.11"
//
//lazy val root = (project in file("."))
//  .settings(
//    name := "spark_work"
//  )
//val sparkVersion = "3.2.1"
//val kafka_clients = "2.8.2"
//
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion,
//  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
//  "org.apache.kafka" % "kafka-clients" % "2.8.2"
//
//)
//
//
//fork in run := true
//javaOptions in run ++= Seq(
//
//  "-Dlog4j.debug=true",
//  "-Dlog4j.configuration=log4j.properties"
//)
//outputStrategy := Some(StdoutOutput)