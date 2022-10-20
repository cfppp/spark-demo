package com.spark.source

import java.io.File
import java.sql.Connection
import java.util
import java.util._

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceType {


  val ints = Array(1, 2, 3, 4, 5, 6, 7, 8)

  val scoreArray = Array(("lili", "ml", 90),
    ("lucy", "ml", 85),
    ("cherry", "ml", 80),
    ("terry", "ml", 85),
    ("tracy", "cs", 82),
    ("tony", "cs", 86),
    ("tom", "cs", 75))

  /**
    * mysql source
    */
  def getLocalMysqlSource: DataFrame = {

    val spark = SparkSession.builder()
      .appName("jdbc test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._


    //方式1：通用的load方法读取
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mark")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "good")
      .load()

    jdbcDF
  }

  /**
    * mysql source
    */
  def getHiveMysqlSource(spark: SparkSession): DataFrame = {

    val spark = SparkSession.builder()
      .appName("jdbc test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._


    //方式1：通用的load方法读取
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.136.102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()
    jdbcDF.show()

    jdbcDF
  }

  def getHiveSource: SparkContext = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://hadoop102:9083")
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    sc
  }

  /**
    * PG source
    */
  def getPGSource(spark: SparkSession): DataFrame = {

    val spark = SparkSession.builder()
      .appName("jdbc test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._


    //方式1：通用的load方法读取
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
      .option("dbtable", "mytable")
      .option("user", "postgres")
      .option("password", "123456")
      .load()

    jdbcDF.show()

    jdbcDF
  }
}
