package com.spark.util

import org.apache.spark.{SparkConf, SparkContext}

object SparkCXT {
  /**
    * 获取sparkcontext
    */

  def getSC: SparkContext = {
    val conf: SparkConf = new SparkConf().setAppName("sc").setMaster("local[*]")
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    sc
  }
}
