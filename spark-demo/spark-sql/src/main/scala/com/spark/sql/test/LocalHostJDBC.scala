package com.spark.sql.test

import java.io.File

import org.apache.spark.sql.SparkSession

object LocalHostJDBC {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("jdbc test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._


    //方式1：通用的load方法读取
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
      .option("dbtable", "mytable")
      .option("user", "postgres")
      .option("password", "123456")
      .load()

    jdbcDF.show()

  }
}
