package com.gpy.spark.batch

import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/13 下午 8:57
  */
object LocalToHiveSparkTest {
  System.setProperty("HADOOP_USER_NAME", "hadoop")
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

    val df = spark.read.json("file:///D://qq.txt")

    df.show()
    df.write.format("hive").mode("append").saveAsTable("xianqueqiao_ods.ods_third_log_qq")
    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000)
  }

}
