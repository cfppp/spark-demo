package com.spark.structured.streaming

import org.apache.spark.sql.SparkSession

/**
  * from socket to 控制台
  */
object StreamDemo1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("StreamDemo1")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")


    import spark.implicits._

    val line = spark.readStream
      .format("socket")
      .option("host", "192.168.136.102")
      .option("port", 9999)
      .load()

    val words = line.as[String].flatMap(_.split(" "))


    val wordCount = words.groupBy("value").count()

    val query = wordCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
