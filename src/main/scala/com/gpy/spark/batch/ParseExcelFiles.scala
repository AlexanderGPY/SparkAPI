package com.gpy.spark.batch

import org.apache.poi.ss.usermodel.Sheet
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/6/24 下午 4:14
  */
object ParseExcelFiles {
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.metastore.uris", "thrift://192.168.248.32:7004")
      .appName("testDemo")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sqlContext = spark.sqlContext

    def readExcel(file: String, sheet: String): DataFrame =
      sqlContext.read
        .format("com.crealytics.spark.excel")
        .option("Header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .option("addColorColumns", "False")
        .option("dataAddress", sheet + "!A1")
        .load(file)

    val df = readExcel(
      "file:///C:\\Users\\dell\\Downloads\\query-hive-40954.xlsx",
      "Sheet"
    )

    df.show(100)

  }
}
