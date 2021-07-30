package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/20 下午 8:04
  */
object dexin08 {
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
    // 合并IP和用户信息
    val user_info_df = MatchIpArea.MergeIpAddressOfCsv(user_info_rdd, spark)

    val jug_age_udf = udf(MyUdf.jug_age)
    val jug_country_udf = udf(MyUdf.jug_lovers_country_type)
    //输出一千天内的异国数据
    user_info_df
      .selectExpr(
        "space_id",
        "a_gender",
        "b_gender",
        "a_birthday",
        "b_birthday",
        "datediff('2021-04-15', registe_space_dt) as since_days",
        "total_cost",
        "a_country",
        "a_province",
        "a_city",
        "b_country",
        "b_province",
        "b_city"
      )
      .withColumn("a_age", jug_age_udf(col("a_birthday")))
      .withColumn("b_age", jug_age_udf(col("b_birthday")))
      .withColumn(
        "country_type",
        jug_country_udf(col("a_country"), col("b_country"))
      )
      .where(" country_type = '异国' and since_days <= 1000")
      .coalesce(1)
      .write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .csv("result_data/diff_country_list")
    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000)
  }
}
