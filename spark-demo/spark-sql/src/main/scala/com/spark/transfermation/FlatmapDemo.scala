package com.spark.transfermation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatmapDemo {

  def main(args: Array[String]): Unit = {

    flatMap

  }

  def flatMap = {

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    val rdd: RDD[String] = sc.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt")

    println("===================rdd=======================")
    rdd.foreach(
      ss => println(ss)
      )


//    val result: RDD[(String, Int)] = rdd.flatMap(line => line.split(" ")).map((_,1)).reduceByKey(_+_)

    println("===================flatMapRDD=======================")
    val flatMapRDD = rdd.flatMap(line => line.split(" "))
    flatMapRDD.foreach(
      value => println(value)
    )
    println("===================reduceRDD=======================")
    val reduceRDD: String = flatMapRDD.reduce(_+_)
    reduceRDD.foreach(
      ss => println(ss)
    )
    println("===================mapRDD=======================")
    val mapRDD = flatMapRDD.map((_,1))
    mapRDD.foreach(
      value => println(value)
    )
    println("===================reduceByKeyRDD=======================")
    val reduceByKeyRDD = mapRDD.reduceByKey(_+_)
    reduceByKeyRDD.foreach(
      word => println(word)
    )


    println("===================countByKeyRDD=======================")
    val countByKey = mapRDD.countByKey()
    countByKey.foreach(
      xx => println(xx)
    )
  }
}
