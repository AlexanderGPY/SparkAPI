package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/19 下午 4:21
  */
object dexin06 {
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
    val user_info_file = "data/user_info_5.txt"

    //2.加载维度文件
    val user_info_rdd = sc.textFile(user_info_file)

    val user_info_df = MatchIpArea.MergeIpAddressOfCsv(user_info_rdd, spark)

    /**
      * 注册判断国家分组的UDF
      */
    val jug_country_type_udf = udf(MyUdf.jug_user_country_type)

    //
    user_info_df
      .where("a_country != '0' and b_country != '0'")
      .select("space_id", "a_country", "b_country", "total_cost")
      .withColumn(
        "country_type",
        jug_country_type_udf(col("a_country"), col("b_country"))
      )
      .groupBy("country_type")
      .agg(
        count("space_id").alias("space_num"),
        sum(col("total_cost")).alias("total_cost")
      )
      .selectExpr(
        "country_type",
        "space_num",
        "space_num * 2 as user_num",
        "total_cost/(space_num * 2) as avg_cost"
      )
      .show(200)

    user_info_df
      .where("a_country != '0' and b_country != '0'")
      .where(
        "datediff('2021-04-15',to_date(a_last_use_time)) <= 365 or  datediff('2021-04-15',to_date(b_last_use_time)) <= 365"
      )
      .select("space_id", "a_country", "b_country", "total_cost")
      .withColumn(
        "country_type",
        jug_country_type_udf(col("a_country"), col("b_country"))
      )
      .groupBy("country_type")
      .agg(
        count("space_id").alias("space_num"),
        sum(col("total_cost")).alias("total_cost")
      )
      .selectExpr(
        "country_type",
        "space_num",
        "space_num * 2 as user_num",
        "total_cost/(space_num * 2) as avg_cost"
      )
      .show(200)

    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000)
  }
}
