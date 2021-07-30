package com.gpy.spark.batch

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, desc, rank, sum, udf}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/19 上午 11:07
  */
object dexin05 {
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

    //注册判断用户类型的UDF
    val jug_user_type_new = udf(MyUdf.jug_user_type_new_old)
    val jug_user_type_days = udf(MyUdf.jug_user_type_love_days)

    //聚合
    val result_df1 =
      user_info_df
        .where("a_province !=  '0' ")
        .selectExpr(
          "space_id",
          "datediff('2021-04-15',a_regist_time) as since_days",
          "datediff(registe_space_dt,anniversary_day) as love_days",
          "datediff('2021-04-15',to_date(a_last_use_time)) as last_active_days",
          "a_province as province"
        )
        .withColumn(
          "user_type",
          jug_user_type_new(col("since_days"), col("last_active_days"))
        )
        .withColumn("love_days_type", jug_user_type_days(col("love_days")))

    //.withColumn("user_type", jug_user_type(col("since_days")))

    val result_df2 =
      user_info_df
        .where("b_province != '0'")
        .selectExpr(
          "space_id",
          "datediff('2021-04-15',b_regist_time) as since_days",
          "datediff(registe_space_dt,anniversary_day) as love_days",
          "datediff('2021-04-15',to_date(b_last_use_time)) as last_active_days",
          "b_province as province"
        )
        .withColumn(
          "user_type",
          jug_user_type_new(col("since_days"), col("last_active_days"))
        )
        .withColumn("love_days_type", jug_user_type_days(col("love_days")))

    //新老用户窗口
    val winSec1 = Window.partitionBy("user_type").orderBy(desc("nums"))
    val winSecSum1 = Window.partitionBy("user_type")

    val winSec2 = Window.partitionBy("love_days_type").orderBy(desc("nums"))
    val winSecSum2 = Window.partitionBy("love_days_type")

    /**
      * 合并并输出 1、按新老用户跑地域分布前20名
      * 2、按开通时恋爱时长30天跑地域分布前二十名
      */
    val old_new_province_count_rank: Unit = result_df1
      .union(result_df2)
      .where("user_type != '0'")
      .groupBy("user_type", "province")
      .agg(
        count("space_id").alias("nums")
      )
      .select(
        col("user_type"),
        col("province"),
        //col("love_days"),
        col("nums"),
        rank().over(winSec1).alias("rk"),
        //sum("nums").over(winSecSum).alias("total_nums"),
        (col("nums") / sum("nums").over(winSecSum1)).*(100).alias("percent")
      )
      .where("rk <= 20")
      .show(100)
    //结束时间
    val end_time1 = System.currentTimeMillis()
    println("time_cost1:" + (end_time1 - start_time) / 1000)

    val love_days_province_count_rank: Unit = result_df1
      .union(result_df2)
      .where("love_days >=0 and love_days is not null")
      .groupBy("love_days_type", "province")
      .agg(
        count("space_id").alias("nums")
      )
      .select(
        col("love_days_type"),
        col("province"),
        //col("love_days"),
        col("nums"),
        rank().over(winSec2).alias("rk"),
        //sum("nums").over(winSecSum).alias("total_nums"),
        (col("nums") / sum("nums").over(winSecSum2)).*(100).alias("percent")
      )
      .where("rk <= 20")
      .show(100)

    val end_time2 = System.currentTimeMillis()

    println("time_cost2:" + (end_time2 - start_time) / 1000)
  }
}
