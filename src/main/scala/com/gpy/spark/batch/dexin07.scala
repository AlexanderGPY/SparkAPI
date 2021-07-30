package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, count, desc, sum, udf}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/19 下午 6:25
  */
object dexin07 {
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

    /** 1.新疆、西藏、四川、河南，存量用户和新增用户中，各地级市的样本及占比；按照省份分别统计。
      * 2. 新疆、西藏、四川、河南，存量用户和新增用户平均年龄。
      */

    //注册判断年龄的UDF
    val jug_age_udf = udf(MyUdf.jug_age)
    val jug_user_type_new = udf(MyUdf.jug_user_type_new_old)
    //窗口函数
    val winSec = Window.partitionBy("user_type", "province")

    val df1 =
      user_info_df
        .where("a_province in ('新疆','四川','西藏','河南') ")
        .selectExpr(
          "a_province as province",
          "a_city as city",
          "a_birthday as birthday",
          "datediff('2021-04-15',a_regist_time) as since_days",
          "datediff('2021-04-15',to_date(a_last_use_time)) as last_active_days"
        )

    val df2 =
      user_info_df
        .where(" b_province in ('新疆','四川','西藏','河南') ")
        .selectExpr(
          "b_province as province ",
          "b_city as city",
          "b_birthday as birthday",
          "datediff('2021-04-15',b_regist_time) as since_days",
          "datediff('2021-04-15',to_date(b_last_use_time)) as last_active_days"
        )

    /** 1.新疆、西藏、四川、河南，存量用户和新增用户中，各地级市的样本及占比（占省份的比）；按照省份分别统计。
      */
    val result1 = df1
      .union(df2)
      .withColumn(
        "user_type",
        jug_user_type_new(col("since_days"), col("last_active_days"))
      )
      .withColumn("age", jug_age_udf(col("birthday")))
      .where("user_type != 'null' and user_type != '0' ")
      .groupBy("user_type", "province", "city")
      .agg(count("city").alias("city_user_num"))
      .select(
        col("user_type"),
        col("province"),
        col("city"),
        col("city_user_num"),
        (col("city_user_num") / sum("city_user_num")
          .over(winSec)).*(100).alias("city_province_percent")
      )
      .orderBy(asc("user_type"), asc("province"), desc("city_province_percent"))

    /**
      * 2. 新疆、西藏、四川、河南，存量用户和新增用户平均年龄。
      */

    val result2 = df1
      .union(df2)
      .withColumn(
        "user_type",
        jug_user_type_new(col("since_days"), col("last_active_days"))
      )
      .withColumn("age", jug_age_udf(col("birthday")))
      .where("age != -1 and user_type != 'null' and user_type != '0' ")
      .groupBy("user_type", "province")
      .agg(avg("age").alias("avg_age"))
      .orderBy(asc("user_type"), asc("province"), desc("avg_age"))

    val result3 = df1
      .union(df2)
      .withColumn(
        "user_type",
        jug_user_type_new(col("since_days"), col("last_active_days"))
      )
      .withColumn("age", jug_age_udf(col("birthday")))
      .where("age != -1 and user_type != 'null' and user_type != '0' ")
      .groupBy("user_type")
      .agg(avg("age").alias("avg_age"))

    //输出处理
    //result1.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").csv("result_data/province_city_percent")
    //result2.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").csv("result_data/province_age_stat")

    result3.show()
    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000)

  }
}
