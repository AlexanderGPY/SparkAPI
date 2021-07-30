package com.gpy.spark.batch

import java.io.{File, PrintWriter}

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization

import scala.beans.BeanProperty

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/5/11 上午 10:58
  */
object LocalToHiveJdbcTest {
  import java.sql.Connection
  import java.sql.DriverManager
  import java.sql.SQLException
  private val driverName = "org.apache.hive.jdbc.HiveDriver"
  @throws[SQLException]
  def main(args: Array[String]): Unit = {
    try Class.forName(driverName)
    catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
        System.exit(1)
    }
    val hs2host = "192.168.248.54"
    val hs2port = "7001"
    val con = DriverManager.getConnection(
      s"jdbc:hive2://$hs2host:$hs2port/xianqueqiao_dwd?hive.exec.dynamic.partition.mode=nonstrict;hive.exec.max.dynamic.partitions=100000;hive.mapred.mode=nonstrict;spark.executor.memoryOverhead=15g;spark.executor.memory=12g;spark.executor.cores=8;spark.executor.instances=16;",
      "hadoop",
      "IKWYj3PpM6gADJ0j"
    )

    val stmt = con.createStatement
    val tableName = "xianqueqiao_dwd.dwd_behavior_app_front_bpl"

    val start_time = System.currentTimeMillis()
    for (x <- 1 to 1) {
      val y = x + 13
      val z = x + 29
      val sql =
        """
          |INSERT OVERWRITE TABLE xianqueqiao_dws.dws_trade_welove_ltv_base_data_1 partition(c_date)
          |SELECT nvl(ad.user_id,b.user_id) AS user_id,
          |       sum(nvl(act_num,0)) AS act_num,
          |       sum(nvl(open_num,0)) AS open_num,
          |       sum(nvl(b.price,0)) AS td_price,
          |       nvl(ad.c_date,b.pay_date) AS c_date
          |FROM
          |  (SELECT nvl(act.user_id,open.user_id) AS user_id,
          |          nvl(act.c_date,open.c_date) AS c_date,
          |          nvl(act.td_ad_num,0) AS act_num,
          |          nvl(open.td_ad_num,0) AS open_num
          |   FROM
          |     (SELECT user_id,
          |             c_date,
          |             "active" AS ad_type,
          |             count(user_id) AS td_ad_num
          |      FROM
          |        (SELECT user_id,
          |                c_date
          |         FROM xianqueqiao_dwd.dwd_opera_tree_video_ad
          |         WHERE c_date >= date_sub(current_date(),182)
          |           AND app_name = "welove"
          |           AND ad_action = "onAdComplete"
          |         UNION ALL SELECT cast(user_id AS BIGINT) AS user_id,
          |                          c_date
          |         FROM xianqueqiao_dwd.dwd_opera_farm_ad_video
          |         WHERE c_date>=date_sub(current_date(),182)
          |           AND app_name = "welove"
          |           AND ad_action = "onAdComplete"
          |         UNION ALL SELECT user_id,
          |                          c_date
          |         FROM xianqueqiao_dwd.dwd_opera_zoo_video_ad
          |         WHERE c_date>=date_sub(current_date(),182)
          |           AND app_name = "welove"
          |           AND ad_action = "onAdComplete"
          |         UNION ALL SELECT user_id,
          |                          c_date
          |         FROM xianqueqiao_dwd.dwd_opera_fairyland_video_ad
          |         WHERE c_date>=date_sub(current_date(),182)
          |           AND app_name = "welove"
          |           AND ad_action = "onAdComplete"
          |         UNION ALL SELECT user_id,
          |                          c_date
          |         FROM xianqueqiao_dwd.dwd_opera_house_ad_video
          |         WHERE c_date>=date_sub(current_date(),182)
          |           AND app_name = "welove"
          |           AND ad_action = "onAdComplete"
          |         GROUP BY c_date,
          |                  user_id) act_ad
          |      GROUP BY user_id,
          |               c_date) act
          |   FULL JOIN
          |     (SELECT user_id,
          |             c_date,
          |             "open" AS ad_type,
          |             count(user_id) AS td_ad_num
          |      FROM xianqueqiao_dwd.dwd_behavior_app_front_bpl
          |      WHERE c_date>=date_sub(current_date(),182)
          |        AND app_name = "welove"
          |        AND bn = "reward_video_ad"
          |        AND ad_action = "onAdShow"
          |      GROUP BY c_date,
          |               user_id) OPEN ON act.c_date = open.c_date
          |   AND act.user_id = open.user_id) AS ad
          |FULL JOIN
          |  (SELECT user_id,
          |          pay_date,
          |          sum(price) AS price
          |   FROM xianqueqiao_dwd.dwd_trade_welove_order
          |   WHERE pay_date>=date_sub(current_date(),182)
          |     AND pay_date !='null'
          |   GROUP BY pay_date,
          |            user_id) AS b ON ad.c_date = b.pay_date
          |AND ad.user_id = b.user_id
          |GROUP BY pay_date,
          |         b.user_id,
          |         ad.user_id,
          |         c_date
           |
           |""".stripMargin
      //println("Running: " + sql)
      stmt.execute(sql)
//
//      while (res.next()) {
//        val app_name = res.getString("app_name")
//        val app_os = res.getString("app_os")
//        val begin_day = res.getString("begin_day")
//        val by_day = res.getInt("by_day")
//        val num = res.getInt("num")
//        val user_type = res.getString("user_type")
//        val c_date = res.getString("c_date")
//        println(
//          app_name + " " + app_os + " " + begin_day + " " + by_day + " " + num + " " + user_type + " " + c_date
//        )
//      }
      val end_time = System.currentTimeMillis()
      println(x)
      println("time_cost:" + (end_time - start_time) / 1000)
    }
  }
}
