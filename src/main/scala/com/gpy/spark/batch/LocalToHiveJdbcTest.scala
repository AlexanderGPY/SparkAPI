package com.gpy.spark.batch

import java.io.{File, PrintWriter}

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
      s"jdbc:hive2://$hs2host:$hs2port/default?hive.exec.dynamic.partition.mode=nonstrict;hive.exec.max.dynamic.partitions=100000;hive.mapred.mode=nonstrict;spark.executor.memoryOverhead=15g;spark.executor.memory=12g;spark.executor.cores=8;spark.executor.instances=16;",
      "hadoop",
      "IKWYj3PpM6gADJ0j"
    )

    val stmt = con.createStatement

    val start = System.currentTimeMillis()

    val sql_str =
      """
        |
        |SELECT space_id,
        |       user_id,
        |       coin_op,
        |       feed_op,
        |       plant_op,
        |       produce_op,
        |       materials_op
        |FROM
        |  (SELECT a.space_id AS space_id,
        |          a.user_id AS user_id,
        |          nvl(get_coin,0) coin_op,
        |          feed_op,
        |          plant_op,
        |          produce_op,
        |          materials_op
        |   FROM
        |     (SELECT space_id,
        |             user_id
        |      FROM farm_behavior_type
        |      WHERE space_id IS NOT NULL
        |        AND space_id !=0
        |        AND user_id !=0
        |        AND user_id IS NOT NULL
        |      GROUP BY space_id,
        |               user_id) AS a
        |   LEFT JOIN
        |     (SELECT space_id,
        |             user_id,
        |             sum(amount) AS cost_coin
        |      FROM farm_behavior_type
        |      WHERE bn ="rainbow_coin_cost"
        |      GROUP BY space_id,
        |               user_id) AS b ON a.space_id = b.space_id
        |   AND a.user_id = b.user_id
        |   LEFT JOIN
        |     (SELECT space_id,
        |             user_id,
        |             sum(amount) AS get_coin
        |      FROM farm_behavior_type
        |      WHERE bn ="rainbow_coin_get"
        |      GROUP BY space_id,
        |               user_id) AS c ON a.space_id = c.space_id
        |   AND a.user_id = c.user_id
        |   LEFT JOIN
        |     (SELECT space_id,
        |             user_id,
        |             collect_set(feed_op) AS feed_op
        |      FROM
        |        (SELECT space_id,
        |                user_id,
        |                map("item_id",cast(item_id AS INT),"count",sum(number)) AS feed_op
        |         FROM farm_behavior_type
        |         WHERE behavior_type ="feed"
        |         GROUP BY space_id,
        |                  user_id,
        |                  item_id) aa
        |      GROUP BY space_id,
        |               user_id) AS d ON a.space_id = d.space_id
        |   AND a.user_id =d.user_id
        |   LEFT JOIN
        |     (SELECT space_id,
        |             user_id,
        |             collect_set(plant_op) AS plant_op
        |      FROM
        |        (SELECT space_id,
        |                user_id,
        |                map("item_id",cast(item_id AS INT),"count",sum(number)) AS plant_op
        |         FROM farm_behavior_type
        |         WHERE behavior_type ="plant"
        |         GROUP BY space_id,
        |                  user_id,
        |                  item_id) aa
        |      GROUP BY space_id,
        |               user_id) AS e ON a.space_id = e.space_id
        |   AND a.user_id =e.user_id
        |   LEFT JOIN
        |     (SELECT space_id,
        |             user_id,
        |             collect_set(produce_op) AS produce_op
        |      FROM
        |        (SELECT space_id,
        |                user_id,
        |                map("item_id",cast(item_id AS INT),"count",sum(1)) AS produce_op
        |         FROM farm_behavior_type
        |         WHERE behavior_type ="produce"
        |         GROUP BY space_id,
        |                  user_id,
        |                  item_id) aa
        |      GROUP BY space_id,
        |               user_id) AS f ON a.space_id = f.space_id
        |   AND a.user_id =f.user_id
        |   LEFT JOIN
        |     (SELECT space_id,
        |             user_id,
        |             collect_set(materials_op) AS materials_op
        |      FROM
        |        (SELECT space_id,
        |                user_id,
        |                map("item_id",cast(item_id AS INT),"count",sum(number)) materials_op
        |         FROM farm_behavior_type
        |         WHERE bn IN("building_materials_get")
        |         GROUP BY space_id,
        |                  user_id,
        |                  item_id) aa
        |      GROUP BY space_id,
        |               user_id) AS g ON a.space_id = g.space_id
        |   AND a.user_id =g.user_id) AS bb
        |WHERE (coin_op != 0
        |       OR size(feed_op) >0
        |       OR size(materials_op) >0
        |       OR size(plant_op) >0
        |       OR size(produce_op) >0)
           |""".stripMargin

    val sql = sql_str
    //println("Running: " + sql)
    val rs = stmt.executeQuery(sql)
    val writer = new PrintWriter(new File("D:\\farm.txt"))
    while (rs.next()) {
      val space_id = rs.getString(1)
      val user_id = rs.getString(2)
      val coin_op = rs.getInt(3)
      val feed_op = rs.getString(4)
      val plant_op = rs.getString(5)
      val produce_op = rs.getString(6)
      val materials_op = rs.getString(7)

      val value: String =
        space_id + " " + user_id + " " + coin_op + " " + feed_op + " " + plant_op + " " + produce_op + " " + materials_op
      println(value)
      writer.println(value)
    }

    writer.close()
    val end = System.currentTimeMillis()
    println("time_cost_total:" + (`end` - start) / 1000)
  }
}
