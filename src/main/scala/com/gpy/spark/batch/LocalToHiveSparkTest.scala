package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/13 下午 8:57
  */
object LocalToHiveSparkTest {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .config("hive.metastore.uris", "thrift://192.168.248.32:7004")
        .master("local[*]")
        .appName("LocalCalculation")
        .enableHiveSupport()
        .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    val start_time = System.currentTimeMillis()
    val tableName = "xianqueqiao_dwd.dwd_behavior_wedding_marry"
    val sql = """SELECT app_name,
               |       app_os,
               |       this_day AS begin_day,
               |       CASE datediff(c_date, this_day)
               |           WHEN 0 THEN 0
               |           WHEN 1 THEN 1
               |           ELSE datediff(c_date, this_day) + 1
               |       END AS by_day,
               |       count(DISTINCT(user_id)) AS num,
               |       "active" AS user_type,
               |       c_date
               |FROM
               |  (SELECT user_id,
               |          c_date,
               |          this_day,
               |          datediff(c_date,this_day) AS by_day,
               |          app_name,
               |          app_os
               |   FROM
               |     (SELECT b.user_id,
               |             b.c_date,
               |             b.app_name,
               |             b.app_os,
               |             c.this_day
               |      FROM
               |        (SELECT user_id,
               |                app_name,
               |                app_os,
               |                c_date
               |         FROM xianqueqiao_dwd.dwd_behavior_farm_login
               |         WHERE c_date = date_sub(current_date(),1)
               |         GROUP BY user_id,
               |                  app_name,
               |                  app_os,
               |                  c_date) b
               |      LEFT JOIN
               |        (SELECT user_id,
               |                c_date AS this_day,
               |                app_name,
               |                app_os
               |         FROM
               |           (SELECT user_id,
               |                   c_date,
               |                   app_name,
               |                   app_os
               |            FROM xianqueqiao_dwd.dwd_behavior_farm_login
               |            WHERE c_date IN (date_sub(current_date(),14),
               |                             date_sub(current_date(),30))) a
               |         GROUP BY user_id,
               |                  app_name,
               |                  app_os,
               |                  c_date) c ON b.user_id = c.user_id) e)f
               |WHERE this_day IS NOT NULL
               |  AND by_day IN(13,
               |                29)
               |GROUP BY this_day,
               |         app_name,
               |         app_os,
               |         c_date""".stripMargin
    val df = spark.sql(sql)
    df.show()
    //resDF.write.format("parquet").mode("append").saveAsTable("xianqueqiao_mid.mid_sweet_app_user")
    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000)
  }

}
