package com.spark.sql.window

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window


object WindowDemo {
  def main(args: Array[String]): Unit = {

    //constructData()

    //windowUse()

    windowUse1()

//    dfAPI()
  }


  /**
    * 构造数据
    */
  def constructData() = {
    val sparkConf = new SparkConf().setMaster("local[2]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val data = Array(("lili", "ml", 90),
      ("lucy", "ml", 85),
      ("cherry", "ml", 80),
      ("terry", "ml", 85),
      ("tracy", "cs", 82),
      ("tony", "cs", 86),
      ("tom", "cs", 75))

    val schemas = Seq("name", "subject", "score")
    val df = spark.createDataFrame(data).toDF(schemas: _*)

    df.show()
  }

  /**
    * window test
    * rank() over (partition by subject order by score desc) as rank
    */
  def windowUse() = {
    val sparkConf = new SparkConf().setMaster("local[2]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext
    spark.sparkContext.setLogLevel("WARN")


    val data = Array(("lili", "ml", 90),
      ("lucy", "ml", 85),
      ("cherry", "ml", 80),
      ("terry", "ml", 85),
      ("tracy", "cs", 82),
      ("tony", "cs", 86),
      ("tom", "cs", 75))

    val schemas = Seq("name", "subject", "score")
    val df = spark.createDataFrame(data).toDF(schemas: _*)
    df.createOrReplaceTempView("person_subject_score")

    val sqltext = "select name, subject, score, rank() over (partition by subject order by score desc) as rank from person_subject_score";
    val ret = sqlContext.sql(sqltext)
    ret.show()
  }

  /**
    * rank生成不连续的序号，上面的例子是1,2,2,4这种
    * dense_rank生成连续的序号，上面的例子是1,2,2,3这种
    * row_number顾名思义，生成的是行号，上面的例子是1,2,3,4这种。
    */
  def windowUse1() = {
    val sparkConf = new SparkConf().setMaster("local[2]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext
    spark.sparkContext.setLogLevel("WARN")


    val data = Array(("lili", "ml", 90),
      ("lucy", "ml", 85),
      ("cherry", "ml", 80),
      ("terry", "ml", 85),
      ("tracy", "cs", 82),
      ("tony", "cs", 86),
      ("tom", "cs", 75))

    val schemas = Seq("name", "subject", "score")
    val df = spark.createDataFrame(data).toDF(schemas: _*)
    df.createOrReplaceTempView("person_subject_score")

    val sqltable = "select * from person_subject_score";
    val dftable = sqlContext.sql(sqltable)
    dftable.show()

    val sqltext = "select name, subject, score, rank() over (partition by subject order by score desc) as rank from person_subject_score";
    val ret = sqlContext.sql(sqltext)
    ret.show()

    val sqltext2 = "select name, subject, score, row_number() over (partition by subject order by score desc) as row_number from person_subject_score";
    val ret2 = sqlContext.sql(sqltext2)
    ret2.show()

    val sqltext3 = "select name, subject, score, dense_rank() over (partition by subject order by score desc) as dense_rank from person_subject_score";
    val ret3 = sqlContext.sql(sqltext3)
    ret3.show()

    val sqltext5 = "select name, subject, score, cume_dist() over (partition by subject order by score desc) as cumedist from person_subject_score";
    val ret5 = sqlContext.sql(sqltext5)
    ret5.show()

  }

  /**
    * df api 实现方式
    */
  def dfAPI() = {
    val sparkConf = new SparkConf().setMaster("local[2]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val data = Array(("lili", "ml", 90),
      ("lucy", "ml", 85),
      ("cherry", "ml", 80),
      ("terry", "ml", 85),
      ("tracy", "cs", 82),
      ("tony", "cs", 86),
      ("tom", "cs", 75))

    val schemas = Seq("name", "subject", "score")
    val df:DataFrame = spark.createDataFrame(data).toDF(schemas: _*)
    df.createOrReplaceTempView("person_subject_score")

    val window = Window.partitionBy("subject").orderBy("score")
    val df2 = df.withColumn("rank",df("window"))
    df2.show()
  }


}
