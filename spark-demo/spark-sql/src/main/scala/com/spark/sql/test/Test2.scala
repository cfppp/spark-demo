package com.spark.sql.test

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * sparksql读取json
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test2").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val jsondf = spark.read
      .format("json")
      .option("path", "M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\json.txt")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ") //spark 2.3.0bug 不这么写会报错
      .load()


    //输出csv
    jsondf.write
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .mode("append")
      .csv("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\output")

    //输出json
    jsondf.write
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .mode("append")
      .json("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\output")

    jsondf.write
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .mode("append")
      .parquet("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\output")

    jsondf.show()
  }
}
