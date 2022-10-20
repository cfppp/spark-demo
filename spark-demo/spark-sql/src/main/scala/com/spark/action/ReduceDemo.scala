package com.spark.action

import com.spark.source.SourceType
import com.spark.util.SparkCXT
import org.apache.spark.rdd.RDD

object ReduceDemo {
  def main(args: Array[String]): Unit = {

    val sc = SparkCXT.getSC
    sc.setLogLevel("WARN")
    val rdd: RDD[Int] = sc.parallelize(SourceType.ints)

    val num = rdd.reduce(_+_)
    println("sum is : " + num)
  }
}
