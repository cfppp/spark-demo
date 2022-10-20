package com.spark.transfermation

import com.spark.util.SparkCXT

object CoalesceDemo {
  def main(args: Array[String]): Unit = {
    val rdd = SparkCXT.getSC.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt",4)
    val partitions = rdd.getNumPartitions
    println("rdd partitions is : "+ partitions)
    val maprdd = rdd.flatMap(_.split(" "))
      .map((_, 1))
    val maprddpar = maprdd.getNumPartitions
    println("maprdd partitions is : "+maprddpar)

    val coardd = maprdd.coalesce(2)
    val sh = coardd.getNumPartitions
    println(" result partitions is : " + sh)

    val reprdd = maprdd.repartition(6)
    val repart = reprdd.getNumPartitions
    println("repartition is :"+repart)

//    maprdd.repartitionAndSortWithinPartitions()

  }
}
