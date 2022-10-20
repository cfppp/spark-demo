package com.spark.transfermation

import com.spark.util.{SparkCXT, SparkSS}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object UnionDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSS.getSparkSS
    val sourcedf: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("user", "root")
      .option("password", "123456")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "emp")
      .load()
    println("=============sourcedf=================")
    sourcedf.show()
    println("=============select column=================")
    sourcedf.select("ename").show()
    println("=============ds1=================")
    val ds1: Dataset[Row] = sourcedf.limit(5)
    ds1.show()
    println("=============ds2=================")
    val ds2 = sourcedf.take(3)
    ds2.foreach(
      ss => println(ss)
    )
    println("=============filter=================")
    val filter = sourcedf.filter("ename = 'MILLER'")
    filter.show()
    println("=============union=================")

    ds1.union(filter).show()

  }
}
