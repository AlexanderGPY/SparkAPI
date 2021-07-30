package com.gpy.spark.batch

import java.sql.DriverManager

import com.gpy.spark.batch.LocalToHiveJdbcTest.driverName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/5/20 下午 5:55
  */
object HiveToMySQL {
  private val driverName = "org.apache.hive.jdbc.HiveDriver"
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .config("hive.metastore.uris", "thrift://192.168.248.32:7004")
        .config("spark.executor.instances", "3")
        .config("spark.executor.cores", "8")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memoryOverhead", "2g")
        .master("local[*]")
        .appName("LocalCalculation")
        .enableHiveSupport()
        .getOrCreate()
    val sc = spark.sparkContext

    val start_time = System.currentTimeMillis()
    val prop       = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://192.168.1.86:3306/decoration"
    prop.setProperty("user", "dw_access")
    prop.setProperty("password", "6e00a81d65bea5c905cb8e8c91d0e8a6")
    val df = spark.sql(
      """select user_id,space_id,unix_timestamp(c_date,'yyyy-MM-dd')*1000 as time from xianqueqiao_dws.dws_user_qqlovespace_new_user_1 where c_date='2019-12-13' """.stripMargin
    )
    val resultDf = df.select(col("user_id"), col("space_id"), col("time"), row_number().over(Window.orderBy("space_id", "user_id")).alias("row_number"))
    resultDf.show(1000)
    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000 + " seconds")
    //df.write.mode("append").jdbc(url, "guide_login_data", prop)
  }
}
