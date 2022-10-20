package com.spark.action

import com.spark.util.SparkCXT

object SaveDemo {
  def main(args: Array[String]): Unit = {
    val rdd = SparkCXT.getSC.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt")

    val reduceByKeyRDD = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

//    reduceByKeyRDD.saveAsTextFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\output")

//    reduceByKeyRDD.saveAsObjectFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\output")

    reduceByKeyRDD.saveAsSequenceFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\output")
  }
}
