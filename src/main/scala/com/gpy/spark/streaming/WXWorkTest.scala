package com.gpy.spark.streaming

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gpy.spark.SparkWeb.doPost
import com.gpy.spark.Utils.DBUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/9/6 下午 3:39
  */
object WXWorkTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()

    val checkPointPath = "file:///./check_path"
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(30))
    ssc.checkpoint(checkPointPath)

    val dstream = getInPutDStream(
      ssc,
      "WXWorkConsumer_test",
      Set("frontend-monitor"),
      "earliest"
    )

    import spark.implicits._
    dstream.foreachRDD { rdd =>
      val df = rdd
        .map { row =>
          val jsonRow = JSON.parseObject(row.value())
          val sv = jsonRow.getString("sv")
          val bn = jsonRow.getString("bn")
          val url = jsonRow.getString("url")
          var errcode = jsonRow.getString("err_code")
          var errmsg = jsonRow.getString("err_msg")
          val timestamp = jsonRow.getLong("@timestamp-log")
          val ip_addr = jsonRow.getString("ip_addr")
          if (errcode == null) errcode = "unkonw"
          if (errmsg == null) errmsg = "unkonw"
          (sv, bn, url, errcode, errmsg, timestamp / 1000, ip_addr)
        }
        .toDF("sv", "bn", "url", "err_code", "err_msg", "time_stamp", "ip_addr")

      val df1 = df
        .groupBy("sv", "bn", "url", "err_code", "err_msg")
        .agg(
          from_unixtime(min("time_stamp"), "yyyy-MM-dd HH:mm:ss")
            .alias("time_stamp"),
          count("ip_addr").alias("num"),
          from_unixtime(max("time_stamp"), "yyyy-MM-dd HH:mm:ss")
            .alias("time_stamp"),
          countDistinct("ip_addr").alias("ip_num")
        )

      val df_res = flushFlagDB(df1, spark, 5)
      df_res.foreach { row =>
        val msg_jo = PareErrLog(row)
        doPost.postResponse(
          "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d4ab1ceb-97d1-4192-aad5-212149acce64",
          msg_jo
        )
      }
    }

    //提交偏移量
    CommitOffsets.saveOffsetsKafka(dstream)
    ssc.start()
    ssc.awaitTermination()
  }

  def getInPutDStream(
    ssc: StreamingContext,
    Group: String,
    Topic: Set[String],
    Reset: String
  ): InputDStream[ConsumerRecord[String, String]] = {

    val KafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.248.15:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Group,
      "auto.offset.reset" -> (Reset: String),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val dstream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Topic, KafkaParams)
      )
    dstream
  }

  /**
    * 刷新数据库中报警标志，开始报警、报警中、报警恢复
    * @param df
    * @param spark
    * @param flag_num
    * @return
    */
  def flushFlagDB(
    df: DataFrame,
    spark: SparkSession,
    flag_num: Int
  ): DataFrame = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    val url =
      "jdbc:mysql://192.168.248.25:3306/state_records?useUnicode=true&characterEncoding=utf-8"
    prop.setProperty("user", "root")
    prop.setProperty("password", "IKWYj3PpM6gADJ0j")
    prop.setProperty("url", url)
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = DBUtil.getConn(prop)
      //conn.setAutoCommit(false)

      val df_flag = spark.read
        .jdbc(url, "alarm_records", prop)
      //将已恢复的告警删除
      ps = conn.prepareStatement(
        s"""delete from alarm_records where status=2 and sv=?
           |""".stripMargin
      )
      df_flag
        .where("status=2")
        .collect()
        .foreach { row =>
          ps.setString(1, row.getString(0))
          ps.addBatch()
        }
      ps.executeBatch()

      //报警次数大于等于阈值，触发，相同时持续触发
      ps = conn.prepareStatement(
        s"""insert into alarm_records(sv, bn, url, err_code, err_msg,time_stamp,num,status,final_time,total_num) values(?,?,?,?,?,?,?,?,?,?)
           | ON DUPLICATE KEY update num=?,status=1,final_time=?,total_num = total_num+?
           |""".stripMargin
      )
      df.where("num >= " + flag_num).collect().foreach { row =>
        writeFlagDB(row, 0)
      }

      ps.executeBatch()

      //前一批次报警，本批次内不再报警，恢复
      ps = conn.prepareStatement(
        s"""insert into alarm_records(sv, bn, url, err_code, err_msg,time_stamp,num,status,final_time,total_num) values(?,?,?,?,?,?,?,?,?,?)
           | ON DUPLICATE KEY update num=?,status=2,final_time=?,total_num = total_num+?
           |""".stripMargin
      )
      df_flag
        .join(
          df,
          df("url") === df_flag("url") and df("err_code") === df_flag("err_code"),
          "left"
        )
        .where(df("url") isNull)
        .select(
          df_flag("sv"),
          df_flag("bn"),
          df_flag("url"),
          df_flag("err_code"),
          df_flag("err_msg"),
          df_flag("time_stamp"),
          coalesce(df("num"), lit(0)),
          df_flag("final_time")
        )
        .collect()
        .foreach(row => writeFlagDB(row, 2))

      ps.executeBatch()

      //本批次内报警次数小于阈值，恢复
      ps = conn.prepareStatement(
        s"""insert into alarm_records(sv, bn, url, err_code, err_msg,time_stamp,num,status,final_time,total_num) values(?,?,?,?,?,?,?,?,?,?)
           | ON DUPLICATE KEY update num=?,status=2,final_time=?,total_num = total_num+?
           |""".stripMargin
      )
      df.where("num < " + flag_num).collect().foreach { row =>
        //触发，但未满足报警条件的，给状态3
        writeFlagDB(row, 3)
      }
      ps.executeBatch()

      //将不足以触发报警的记录删除
      ps = conn.prepareStatement(
        s"""delete from alarm_records where status=3 and sv=?
           |""".stripMargin
      )
      df_flag
        .where("status=3")
        .collect()
        .foreach { row =>
          ps.setString(1, row.getString(0))
          ps.addBatch()
        }
      ps.executeBatch()

      //MySQL语句赋值
      def writeFlagDB(row: Row, status: Int): Unit = {
        val sv = row.getString(0)
        val bn = row.getString(1)
        val url = row.getString(2)
        val errcode = row.getString(3)
        val errmsg = row.getString(4)
        val time = row.getString(5)
        val num = row.getLong(6)
        val final_time = row.getString(7)
        ps.setString(1, sv)
        ps.setString(2, bn)
        ps.setString(3, url)
        ps.setString(4, errcode)
        ps.setString(5, errmsg)
        ps.setString(6, time)
        ps.setLong(7, num)
        ps.setInt(8, status)
        ps.setString(9, final_time)
        ps.setLong(10, num)
        ps.setLong(11, num)
        ps.setString(12, final_time)
        ps.setLong(13, num)
        ps.addBatch()
      }

      //提交数据库修改
      //conn.commit()

    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      if (ps != null) ps.close()
      if (conn != null) DBUtil.releaseConnection(conn)
    }
    spark.read.jdbc(url, "alarm_records", prop)
  }

  /**
    * 解析日志，组装成wx机器人格式
    * @param row
    * @return
    */
  def PareErrLog(
    row: Row
  ): JSONObject = {

    val sv = row.getString(0)
    val bn = row.getString(1)
    val url = row.getString(2)
    val errcode = row.getString(3)
    val errmsg = row.getString(4)
    val time = row.getString(5)
    val num = row.getInt(6)
    val status = row.getInt(7)
    val final_time = row.getString(8)
    val total_num = row.getInt(9)

    val service = sv match {
      case "farm"      => "农场"
      case "tree"      => "爱情树"
      case "lovespace" => "情侣空间"
      case "house"     => "小家"
      case "pet"       => "情侣宝宝"
      case "love_pet"  => "情侣宝宝"
      case _           => sv
    }

    val content =
      status match {
        case 0 =>
          s"""
               |### 项目：<font color=\"warning\">$service</font>接口报警，请相关同学注意。\n
               |>接口分类：<font color=\"comment\">$bn</font>
               |>接口URL：<font color=\"warning\">$url</font>
               |>错误代码：<font color=\"comment\">$errcode</font>
               |>错误信息：<font color=\"comment\">$errmsg</font>
               |>当前报错次数：<font color=\"warning\">$num</font> <font color=\"comment\">(阈值100)</font> 
               |>告警时间：<font color=\"comment\">$time</font> 
               |>备注：<font color=\"comment\">来自埋点报警服务</font>
               |""".stripMargin
        case 1 =>
          s"""
               |### 项目：<font color=\"warning\">$service</font>接口持续报警，请相关同学注意。\n
               |>接口分类：<font color=\"comment\">$bn</font>
               |>接口URL：<font color=\"warning\">$url</font>
               |>错误代码：<font color=\"comment\">$errcode</font>
               |>错误信息：<font color=\"comment\">$errmsg</font>
               |>当前报错次数：<font color=\"warning\">$num</font> <font color=\"comment\">(阈值100)</font> 
               |>告警时间：<font color=\"comment\">$time</font> 
               |>备注：<font color=\"warning\">持续告警中</font>
               |""".stripMargin
        case 2 =>
          s"""
               |### 项目：<font color=\"info\">$service</font>接口报警已恢复。\n
               |>接口分类：<font color=\"comment\">$bn</font>
               |>接口URL：<font color=\"info\">$url</font>
               |>错误代码：<font color=\"comment\">$errcode</font>
               |>错误信息：<font color=\"comment\">$errmsg</font>
               |>当前报错次数：<font color=\"info\">$num</font> <font color=\"comment\">(阈值100)</font>
               |>总报错次数：<font color=\"info\">$total_num</font>
               |>告警时间：<font color=\"comment\">$time</font> 
               |>恢复时间：<font color=\"comment\">$final_time</font>
               |>备注：<font color=\"info\">告警恢复</font>
               |""".stripMargin
        case _ => """未知状态"""
      }

    val outer = new JSONObject()
    val inner = new JSONObject()
    inner.put("content", content)
    outer.put("msgtype", "markdown")
    outer.put("markdown", inner)
    outer
  }
}
