package com.spark.transfermation

import com.spark.util.SparkCXT

object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sc = SparkCXT.getSC

    val rdd = sc.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt",1)
    val maprdd = rdd.flatMap(_.split(" "))
      .map((_, 1))
    println("========map==========")
    maprdd.foreach(
      ss => println(ss)
    )

    val sortByKey = maprdd.sortByKey()
    println("========sortByKey==========")
    sortByKey.foreach(
      ss => println((ss))
    )

    val partitions = sortByKey.getNumPartitions
    println("partitions :: "+ partitions)
  }
}
