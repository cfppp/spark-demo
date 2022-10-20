package com.spark.sql.test

import org.apache.spark.sql.SparkSession

object Test3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test3").master("local[*]").getOrCreate()
    val csvdf = spark.read.format("csv")
      .option("path", "M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ") //spark 2.3.0bug 不这么写会报错
      .load()

    csvdf.write
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .mode("append")
        .csv("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\output")

    csvdf.show()

  }
}
