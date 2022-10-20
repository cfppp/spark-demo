package com.spark.structured.streaming

import org.apache.spark.sql.SparkSession

/**
  * from kafka to kafka
  */
object StreamDemo2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("StreamDemo1")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.136.102:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.136.103:9092")
      .option("topic", "test_sink")
      .option("checkpointLocation", "M:\\intelliJ\\local\\spark-demo\\structured-streaming\\src\\main\\resources")
      .start()

    query.awaitTermination()

  }

}
