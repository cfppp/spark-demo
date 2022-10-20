package com.spark.sql.test

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * sparksql读取jdbc
  */
object Test1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()



    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mark")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "good")
      .load()
    df.show()

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    df.write
        .jdbc("jdbc:mysql://localhost:3306/mark","outputTable",prop)

    spark.stop()
  }
}
