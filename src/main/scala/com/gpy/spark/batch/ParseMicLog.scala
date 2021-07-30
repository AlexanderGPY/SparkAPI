package com.gpy.spark.batch

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val user_info_file = args(0)
    val outPutPath = args(1)
    val df = spark.read
      .textFile(user_info_file)
      .repartition(200)
      .filter(line => line.contains("Microscopic_view"))
      .filter(line => line.contains("|| SweetLogin"))
      .map(line => {
        val arr = line.split(" ")
        val date = arr(1)
        val user_id = arr(7)
        val space_id = arr(8)
        val plat = arr(9) match {
          case "1" => "QQ"
          case "7" => "WeChat"
          case _   => "Phone Or Email"
        }
        val ua = arr(11).contains("Android")
        val os = ua match {
          case true  => "android"
          case false => "ios"
          case _     => "dev_tool"
        }
        (date, user_id, space_id, plat, os)
      })
      .toDF("date", "user_id", "space_id", "platform", "os")
      .where("user_id is not null")
    df.coalesce(1).write.mode("append").json(outPutPath)

    val end_time = System.currentTimeMillis()
    println("time_cost: " + (end_time - start_time) / 1000 + " sec")
  }
}
