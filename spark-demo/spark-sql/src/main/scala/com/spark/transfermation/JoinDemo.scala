package com.spark.transfermation

import java.util.Properties

import com.spark.util.SparkSS

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSS.getSparkSS

    val empdf = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "emp")
      .load()
    println("=============empdf================")
    empdf.show()

    val deptdf = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "dept")
      .load()
    println("=============deptdf================")
    deptdf.show()

    import spark.implicits._
    val filter = empdf.filter($"sal" > 2000)
    println("=============filter================")
    filter.show()
    println("=============join================")
    val join = deptdf.join(filter,"deptno").filter($"deptno" === 10)
    join.show()

    //将结果发送至mysql
    val prop= new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    join.write
      .jdbc("jdbc:mysql://localhost:3306/test","joinTable",prop)

    spark.stop()
  }
}
