package com.spark.action

import com.spark.util.SparkCXT

object CollectDemo {
  def main(args: Array[String]): Unit = {

    val rdd = SparkCXT.getSC.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt")

    println("==========collect=============")
    val tuples: Array[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
    tuples.foreach(
      xx => println(xx)
    )

    println("==========reduceByKey=============")
    val reduceByKey = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    reduceByKey.foreach(
      ss => println(ss)
    )


    println("==========count=============")
    val count = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
        .count()

    println("count is : " + count)


    println("==========first=============")
    val first = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .first()

    println(first)

    println("==========take=============")
    val take = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .take(4)

    take.foreach(
      ss => println(ss)
    )

    println("==========takesample=============")
    val takesample = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .takeSample(true,10)

    takesample.foreach(
      ss => println(ss)
    )

    println("==========takeOrdered=============")
    val takeOrdered = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .takeOrdered(9)

    takeOrdered.foreach(
      ss => println(ss)
    )
  }
}
