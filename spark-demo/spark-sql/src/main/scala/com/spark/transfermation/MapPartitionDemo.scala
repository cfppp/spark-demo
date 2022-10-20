package com.spark.transfermation

import com.spark.util.SparkCXT

object MapPartitionDemo {
  def main(args: Array[String]): Unit = {

    val rdd = SparkCXT.getSC.parallelize(Array(1,2,3,6,4))
    println("===========rdd=============")
    rdd.foreach(
      ll =>println(ll)
    )
    println("===========mapPartitions=============")

    def mapPart(it:Iterator[Int]):Iterator[(Int,Int)]={
      var list=List[(Int,Int)]()
      while (it.hasNext) {
        val i = it.next()
        list=list.::(i,i*2)
      }
      list.iterator
    }

    val result = rdd.mapPartitions(mapPart)

    result.foreach(
      ss => println(ss)
    )

    println("===========map=============")
    rdd.map(
      xx => (xx,xx*3)
    ).foreach(
      dd => println(dd)
    )

    println("===========mapPartitions=============")

    def custom (it: Iterator[Int]):Iterator[(Int,Int)] ={
      var list1 = List[(Int, Int)]()
      while (it.hasNext) {
        val i = it.next()
        list1=list1.::(i,i*3)
      }
      list1.iterator
    }
    val mapPartition = rdd.mapPartitions(custom)

    mapPartition.foreach(
      gg => println(gg)
    )


  }
}
