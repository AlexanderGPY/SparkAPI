package com.gpy.spark.batch

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/6/9 下午 3:24
  */
object ParseMicLog {
  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    //1.构建本地测试环境
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val user_info_file =
      "file:///D:\\IdeaProjects\\SparkAPI\\data\\welove.log_2020-01-19.log.merge"
    val outPutPath = ""
    val df = spark.read
      .textFile(user_info_file)
      .repartition(200)
      .filter(line => !line.contains("Microscopic_view"))
      .map { line =>
        val arr = line.split(" ")
        val date = arr(0)
        val ip_1 = arr(2)
        val ip_2 = arr(3)
        val user_id = arr(4)
        val space_id = arr(5)
        (date, user_id, ip_1, ip_2)
      }
      .toDF("date", "user_id", "ip_1", "ip_2")
      .where("user_id is not null")
      .groupBy("date", "user_id", "ip_1", "ip_2")
      .count()
      .alias("ip_num_cnt")
      .orderBy("date")
    df.show(100)

    val end_time = System.currentTimeMillis()
    println("time_cost: " + (end_time - start_time) / 1000 + " sec")
  }
}
