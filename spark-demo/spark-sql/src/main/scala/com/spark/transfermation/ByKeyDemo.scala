package com.spark.transfermation

import com.spark.util.SparkCXT
import org.apache.spark.rdd.RDD

object ByKeyDemo {
  def main(args: Array[String]): Unit = {
    val rdd = SparkCXT.getSC.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt")

    val reduceByKey = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    println("=================reduceByKey=================")
    reduceByKey.foreach(
      ss => println(ss)
    )

    println("=================groupByKey=================")
    val groupByKey = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey().map {
    case (word, iter) => {
      (word, iter.size)
    }

  }
    groupByKey.foreach(
      ss => println(ss)
    )


    val map = rdd.flatMap(_.split(" "))
      .map((_, 1))
    println("==============map================")
    map.foreach(
      ss => println(ss)
    )
    println("==============aggregateByKey================")
    val aggregateByKey = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => (x + y))
    aggregateByKey.foreach(
      ss => println(ss)
    )

  }
}
