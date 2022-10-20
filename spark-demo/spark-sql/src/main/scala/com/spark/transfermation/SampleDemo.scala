package com.spark.transfermation

import com.spark.source.SourceType
import com.spark.util.SparkCXT

object SampleDemo {
  def main(args: Array[String]): Unit = {

    val rdd = SparkCXT.getSC.parallelize(SourceType.ints)
    println("===============rdd====================")
    rdd.foreach(
      ss => println(ss)
    )
    println("===============collect====================")
    rdd.collect().foreach(
      uu => println( uu )
    )

    println("===============sample====================")
    val sampleRDD = rdd.sample(true,1)
    sampleRDD.foreach(
      yy => println(yy)
    )
  }
}
