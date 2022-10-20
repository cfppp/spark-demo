package com.spark.transfermation

import java.sql
import java.sql.{Connection, DriverManager, ResultSet}

import com.spark.util.SparkSS

object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSS.getSparkSS
        val empdf = spark.read
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "123456")
          .option("dbtable", "test.emp")
          .load()
    val aa = empdf.count()
        empdf.show()
    println(aa)

       val distinct = empdf.select("hiredate").distinct()
    val gg = empdf.select("hiredate").distinct().count()
    println("===============distinct=================")
    distinct.foreach(
      ss => println(ss)
    )

    println(gg)
  }

  def mysqlQuery = {
    val connect = getConnectMysql
    connect.prepareStatement("show databases").executeQuery()
    connect.prepareStatement("use test").execute()
    val set = connect.prepareStatement("select * from emp ").executeQuery()
    val distinct = connect.prepareStatement("select distinct(hiredate) from emp").executeQuery()

    println("=============ename==============")
    while (set.next()) {
      val str = set.getString("ename")
      println(str)
    }

    println("=============distinct==============")
    while (distinct.next()) {
      val hiredate = distinct.getString("hiredate")
      println(hiredate)
    }
  }

  def getConnectMysql: Connection = {

    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "123456")
  }
}
