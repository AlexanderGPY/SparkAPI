package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/5/12 下午 1:11
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("LocalCalculation")
        .getOrCreate()

    val test = spark.sparkContext.textFile("file:///C:\\Users\\dell\\Desktop\\乱七八糟的文件\\welove.log_2021-12-*.log.active")
    test
      .map { line =>
        val ss = line.split(" ")
        (ss(0), 1)
      }
      .filter(_._1.startsWith("5"))
      .filter(_._1.length == 15)
      .reduceByKey(_ + _)
      .coalesce(1)
      .sortBy(_._2, ascending = false)
      .keys
      .saveAsTextFile("file:///D://active\\welove_active_29_30")

    val test_new = spark.sparkContext.textFile("file:///D:\\active\\welove_active_29_30\\*").map((_, 1))
    val test_old = spark.sparkContext.textFile("file:///D:\\active\\welove_active\\*").map((_, 2))

    test_new
      .union(test_old)
      .reduceByKey(_ + _)
      .filter(_._2 == 1)
      .keys
      .coalesce(1)
      .saveAsTextFile("file:///D://active\\welove_active_29_30_distinct")

  }

}
