package com.spark.transfermation

import com.spark.util.SparkCXT

object PipeDemo {
  def main(args: Array[String]): Unit = {
    // pipe()函数调用外部程序
    val cFile = "M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.py"

    val rdd = SparkCXT.getSC.textFile("M:\\intelliJ\\local\\spark-demo\\spark-sql\\src\\main\\resources\\test.txt")
    val pipeOut = rdd.pipe(Seq(cFile, "command", "cmd"))
    // Seq()函数负责调用程序，以及传参。外部程序通过向标准输出来写结果，此结果就是pipe()函数的返回RDD
    pipeOut.foreach(println)
  }
}
