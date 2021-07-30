package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, countDistinct, desc}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/28 下午 5:50
  */
object jingrui01 {
  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    //1.构建本地测试环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val user_info_file = "file:///D:\\ideaProject\\sparkAPI\\data/周活用户IP地址1.csv"
    val new_user_info_file =
      "file:///D:\\ideaProject\\sparkAPI\\data/周活新用户IP地址1.csv"

    //2.加载维度文件
    val user_info_rdd = sc.textFile(user_info_file)
    val new_user_info_rdd = sc.textFile(new_user_info_file)
    // 合并IP和用户信息
    val user_info_df = MatchIpArea.MergeIpAddressOfCsv(user_info_rdd, spark)
    val new_user_info_df =
      MatchIpArea.MergeIpAddressOfCsv(new_user_info_rdd, spark)

    val res1 = user_info_df
      .where("city !='0'")
      .dropDuplicates()
      .groupBy("city")
      .agg(countDistinct("user_id").alias("act_number"))

    val res2 = new_user_info_df
      .where("city !='0'")
      .dropDuplicates()
      .groupBy("city")
      .agg(countDistinct("user_id").alias("new_number"))

    res1
      .join(res2, "city")
      .orderBy(
        desc("act_number"),
        desc("new_number")
      )
      .coalesce(1)
      .write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .csv("file:///D:\\ideaProject\\sparkAPI\\result_data/周活跃新增城市分布")

    //结束时间
    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000)
  }
}
