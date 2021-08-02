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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val user_info_file = args(0)
    val outPutPath = args(1)
    val df = spark.read
      .textFile(user_info_file)
      .repartition(100)
      .filter(line => !line.contains("Microscopic_view"))
      .map { line =>
        try {
          val arr = line.split(" ")
          val date = arr(0)
          val ip = arr(3)
          val user_id = arr(4)
          val space_id = arr(5)
          (date, user_id, ip)
        } catch {
          case ex: Exception =>
            println(line)
            ("0", "0", "0")
        }
      }
      .toDF("date", "user_id", "ip")
      .where("user_id is not null")
      .groupBy("date", "user_id", "ip")
      .count()
      .alias("ip_num_cnt")
      .orderBy("date", "user_id")
    df.coalesce(1).write.csv(outPutPath)

    import spark.implicits
    val end_time = System.currentTimeMillis()
    println("time_cost: " + (end_time - start_time) / 1000 + " sec")
  }
}
