package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/12/24 18:11:42
  */
object LocalToHDFSTest {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("LocalCalculation")
        .getOrCreate()

    val wxmp = spark.read
      .json("/Front_BPL_Sv/front_end_wxmp_sweet/2021-12-24/*")
      .where("space_id is not null and open_id is not null and space_id !=0 and open_id!=0")
      .where("`@timestamp-log` <= 1640322000000")
      .where("app_name = 'sweet'")
      .selectExpr("space_id", "open_id")

    val qqmp = spark.read
      .json("/Back_BPL_Sv/back_end_qqmp_sweet/2021-12-24/*")
      .filter("bn = 'login'")
      .where("space_id is not null and user_id is not null and space_id !=0 and user_id!=0")
      .where("`@timestamp-log` <= 1640322000000")
      .where("app_name = 'sweet'")
      .selectExpr("space_id", "user_id")

    val farm = spark.read
      .json("/Back_BPL_Sv/back_end_farm/2021-12-24/*")
      .where("space_id is not null and user_id is not null and space_id !=0 and user_id!=0")
      .where("`@timestamp-log` <= 1640307600000 and `@timestamp-log`>= 1640279400000")
      .where("app_name = 'sweet'")
    val house = spark.read
      .json("/Back_BPL_Sv/back_end_house/2021-12-24/*")
      .filter("bn = 'login'")
      .where("space_id is not null and user_id is not null and space_id !=0 and user_id!=0")
      .where("`@timestamp-log` <= 1640322000000")
      .where("app_name = 'sweet'")
      .selectExpr("space_id", "user_id")

    val tree = spark.read
      .json("/Back_BPL_Sv/back_end_tree/2021-12-24/*")
      .filter("bn = 'click_trace'")
      .where("space_id is not null and user_id is not null and space_id !=0 and user_id!=0")
      .where("`@timestamp-log` <= 1640322000000")
      .where("app_name = 'sweet'")
      .selectExpr("space_id", "user_id")

    val app = spark.read
      .json("/Front_BPL_Sv/front_end_app/2021-12-24/*")
      .where("space_id is not null and user_id is not null and space_id !=0 and user_id!=0")
      .where("`@timestamp-log` <= 1640322000000")
      .where("app_name = 'sweet'")
      .selectExpr("space_id", "user_id")

    val df = tree.union(farm).union(house).union(qqmp).union(wxmp).union(app)

    val df_res = df.where("space_id LIKE '7%'")

    import spark.implicits._
    val day7 = spark.read
      .textFile("/tmp/sweet/sweet-single.txt")
      .map { line =>
        val ar = line.split("[\t]")
        val space_id = ar(0).toLong
        val user_id = ar(1).toLong
        (space_id, user_id)
      }
      .toDF("space_id", "user_id")

    val res = df_res.union(day7).selectExpr("concat(space_id,' ',user_id) as value")

    res.coalesce(1).rdd.saveAsTextFile("/tmp/sweet/active_user_problem_1224")
    farm.write.saveAsTable("default.farm_behavior_type")
  }
}
