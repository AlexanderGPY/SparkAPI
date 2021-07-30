package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/12/22 下午 6:55
  */
object ParseMTADataUtils {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()
    val filePath = "data/WeloveBasePoint.json"
    ParseBaseData(spark, filePath)
  }

  def ParseBaseData(spark: SparkSession, fileName: String) = {
    spark.read.json(fileName).show(20)
  }
}
