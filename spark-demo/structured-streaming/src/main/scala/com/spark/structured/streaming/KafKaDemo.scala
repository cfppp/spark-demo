package com.spark.structured.streaming

import org.apache.spark.sql.SparkSession

/**
  * from kafka to 控制台
  */
object KafKaDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("StreamDemo1")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel( "WARN" )


    import spark.implicits._

    val line = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.136.102:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


    val words = line.flatMap(_.toString().split(" "))

    val wordCount = words.groupBy("value").count()

    val query = wordCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()


  }
}
