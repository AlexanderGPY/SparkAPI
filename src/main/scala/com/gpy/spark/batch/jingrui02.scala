package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/29 下午 6:12
  */
object jingrui02 {
  def main(args: Array[String]): Unit = {
    //1.构建本地测试环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val welove_space_parse_df =
      spark.read.json("data/welove_anni_photo_stat.json0")
    val sweet_space_parse_df =
      spark.read.json("data/sweet_anni_photo_stat.json0")

    welove_space_parse_df
      .selectExpr(
        "avg(anni_create) as avg_anni",
        "avg(photo_create) as avg_photo"
      )
      .show()

    sweet_space_parse_df
      .selectExpr(
        "avg(anni_create) as avg_anni",
        "avg(photo_create) as avg_photo"
      )
      .show()
  }
}
