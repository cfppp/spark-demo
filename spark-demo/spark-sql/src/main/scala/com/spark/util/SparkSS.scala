package com.spark.util

import org.apache.spark.sql.SparkSession

object SparkSS {

  def getSparkSS:SparkSession = {
     val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark
  }
}
