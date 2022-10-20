package com.spark.sql.test

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * spark & hive
  */
object SparkSQLtest {
  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://hadoop102:9083")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    import spark.implicits._
    import spark.sql

//    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    println("==============show databases ==================")
    spark.sql("show databases").show()
    println("============== use database ==================")
    spark.sql("use test").show
    println("============== show tables ==================")
    spark.sql("show tables").show
    println("============== select * ==================")
    spark.sql("select * from test.orders limit 5").show()
    println("==============count==================")
    spark.sql("select count(1) from test.orders").show()




    spark.close()

  }
}
