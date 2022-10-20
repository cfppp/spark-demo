package com.spark.transfermation

import com.spark.util.SparkCXT
import org.apache.spark.rdd.RDD

object FilterDemo {
  def main(args: Array[String]): Unit = {
    val rdd: RDD[String] = SparkCXT.getSC.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt")

    val filter = rdd.filter(
      row => row.contains("you")
    ).flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
    filter.foreach(
      ss => println(ss)
    )

  }
}
