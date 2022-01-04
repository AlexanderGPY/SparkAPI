package com.gpy.spark.batch
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.{stddev, _}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/1 下午 2:52
  */

/**
  * 需求：在特定人群中，计算分组平均值、中位数
  * love day（开通空间时的恋爱天数）步长为5、10、20，进行分组，loveday、留存率、平均花费ARPU、花费中位数、标准差、付费均值ARPPU、样本数量。
  * 其中，按步长分组解释为 5步一组:0-4,5-9,10-14 ...etc
  * 10步一组：0-9,10-19，...etc
  * 20步一组：0-19,20-39,etc
  * 分组实现方法设想为: 逻辑窗口、打分组标签（推荐）
  */
object dexin04 {
  def main(args: Array[String]): Unit = {
    //1.构建本地测试环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val user_info_file = "data/"

    //2.加载用户信息文件
    val user_info_rdd = sc.textFile(user_info_file)
    val user_info_df =
      user_info_rdd
        .map(line => line.split("[\t]"))
        .map(array =>
          (
            array(0),
            array(1),
            array(2),
            array(3),
            array(4),
            array(5).toInt,
            array(6).toInt,
            array(7),
            array(8),
            array(9),
            array(10),
            array(11),
            array(12),
            array(13),
            array(14) match {
              case "NULL" => 0
              case null   => 0
              case _      => array(14).toLong
            },
            array(15) match {
              case "NULL" => 0
              case null   => 0
              case _      => array(15).toLong
            }
          )
        )
        .toDF(
          "space_id",
          "a_gender",
          "b_gender",
          "a_birthday",
          "b_birthday",
          "a_retention",
          "b_retention",
          "last_use_time",
          "a_regist_time",
          "b_regist_time",
          "registe_space_dt",
          "anniversary_day",
          "close_time",
          "total_cost",
          "a_ip",
          "b_ip"
        )

    //计算分组求平均值并输出

    val result1 = merge_cal(user_info_df, 5)
    result1.show()
    // result1.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").csv("result_data/user_info_5_csv")
    val result2 = merge_cal(user_info_df, 10)
    result2.show()
    //result2.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").csv("result_data/user_info_10_csv")
    val result3 = merge_cal(user_info_df, 20)
    result3.show()
    //result3.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").csv("result_data/user_info_20_csv")
  }

  /**
    * 合并计算
    * @param df
    * @param num
    * @return
    */
  def merge_cal(df: DataFrame, num: Int): DataFrame = {

    /**
      * udf 给表打分组标签：按某一列值区间分组
      */

    val udf_add_group_tag: UserDefinedFunction = udf(MyUdf.add_tag_num)

    /**
      * 也可以开窗，Window.RangeBetween()逻辑窗口去做，效率低些
      * 不做尝试
      * 。。。
      */

    //过滤掉花费为0的
    val re1 = df
      .dropDuplicates()
      .where("total_cost !=0")
      .selectExpr(
        "space_id",
        "a_retention",
        "b_retention",
        "registe_space_dt",
        "anniversary_day",
        "datediff(registe_space_dt,anniversary_day) as love_days",
        "total_cost",
        s"$num as num"
      )
      .where("love_days is not null and love_days >= 0 and love_days <= 1095 and anniversary_day is not null")
      .withColumn("love_days", udf_add_group_tag(col("love_days"), col("num")))
      .groupBy("love_days")
      .agg(
        count("space_id").alias("space_cnt"),
        sum("total_cost").alias("cost"),
        (sum("a_retention") + sum("b_retention")).alias("retention"),
        sort_array(collect_list("total_cost")).alias("cost_list"),
        stddev(col("total_cost")).alias("std_cost")
      )
      .select(
        col("love_days"),
        col("space_cnt"),
        col("cost"),
        col("retention"),
        col("std_cost"),
        element_at(col("cost_list"), (size(col("cost_list")) / 2 + 1).cast("int")).alias("median_value_of_cost")
      )
      .selectExpr(
        "love_days",
        " retention / (space_cnt*2) *100 as retention_rate",
        "cost / (space_cnt*2) as cost_avg",
        "space_cnt",
        "median_value_of_cost",
        "std_cost"
      )
      .orderBy("love_days")
    // 不过滤花费为0的
    val re2 = df
      .dropDuplicates()
      .selectExpr(
        "space_id",
        "a_retention",
        "b_retention",
        "registe_space_dt",
        "anniversary_day",
        "datediff(registe_space_dt,anniversary_day) as love_days",
        "total_cost",
        s"$num as num"
      )
      .where("love_days is not null and love_days >= 0 and love_days <= 1095 and anniversary_day is not null")
      .withColumn("love_days", udf_add_group_tag(col("love_days"), col("num")))
      .groupBy("love_days")
      .agg(
        count("space_id").alias("space_cnt"),
        sum("total_cost").alias("cost"),
        (sum("a_retention") + sum("b_retention")).alias("retention")
      )
      .selectExpr(
        "love_days",
        " retention / (space_cnt*2) *100 as retention_rate",
        "cost / (space_cnt*2) as cost_avg",
        "space_cnt"
      )
      .orderBy("love_days")

    re2
      .join(re1, re1("love_days") === re2("love_days"), "left")
      .drop(re1("retention_rate"))
      .drop(re1("love_days"))
      .select(
        re2("love_days").alias("恋爱天数"),
        re2("retention_rate").alias("留存率"),
        (re1("space_cnt") / re2("space_cnt")).alias("付费率"),
        re2("cost_avg").alias("人均付费(ARPU)"),
        re1("median_value_of_cost").alias("付费中位数"),
        re1("cost_avg").alias("付费均值(ARPPU)"),
        re1("std_cost").alias("标准差"),
        re2("space_cnt").alias("样本数量(包含付费为0)")
      )
      .orderBy("love_days")
  }
}
