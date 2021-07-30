package com.gpy.spark.batch

import java.io.{File, PrintWriter}

import org.json4s.DefaultFormats
import org.json4s.native.Serialization

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/7/6 下午 2:55
  */
object propose_success {
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
      s"jdbc:hive2://$hs2host:$hs2port/xianqueqiao_dwd?hive.mapred.mode=nonstrict;spark.executor.memoryOverhead=8g;spark.executor.memory=8g;spark.executor.cores=8;spark.executor.instances=16;",
      "hadoop",
      "IKWYj3PpM6gADJ0j"
    )
    val stmt = con.createStatement
    val tableName = "xianqueqiao_dwd.dwd_behavior_wedding_marry"

    val sql =
      "select * from " + tableName + " WHERE el = 'propose_success' order by `@timestamp-log`"
    println("Running: " + sql)
    val res = stmt.executeQuery(sql)
    //用于case class 转 json
    implicit val formats: DefaultFormats.type = DefaultFormats

    //用于输出文件
    val writer = new PrintWriter(new File("propose_success.json"))
    while (res.next()) {
      val p = propose_suc.apply(
        res.getLong("user_id"),
        res.getLong("lover_id"),
        res.getInt("prop_id"),
        res.getLong("@timestamp-log")
      )

      val jsonString = Serialization.write(p)

      writer.write(jsonString)
      writer.write("\n")

    }
    writer.close()
  }
  case class propose_suc(
    user_id: Long,
    lover_id: Long,
    prop_id: Int,
    time: Long
  )

}
