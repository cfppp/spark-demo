package com.spark.sql.test

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark & jdbc
  */
object SparkHiveJDBCDemo {
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
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.136.102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()
    jdbcDF.show()

    //方式2:通用的load方法读取 参数另一种形式

    spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://linux1:3306/spark-sql?user=root&password=123123",
        "dbtable"->"user","driver"->"com.mysql.jdbc.Driver")).load().show


    //方式3:使用jdbc方法读取

    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://linux1:3306/spark-sql", "user", props)

    df.show


  }
}
