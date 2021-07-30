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
        .option("dataAddress", sheet + "!A2")
        .load(file)

    val arrGaChaFashion =
      Array(
        "头发",
        "衣服",
        "裤子",
        "全身",
        "鞋子",
        "头顶",
        "面具",
        "耳环",
        "嘴",
        "帽子",
        "眼镜",
        "项链",
        "围巾",
        "双肩包",
        "单肩包",
        "胸包",
        "雨伞",
        "手套",
        "左手",
        "右手",
        "尾巴",
        "前景",
        "背景",
        "勋章",
        "斗篷"
      )

    var dataFrame: DataFrame = null
    for (sheet <- arrGaChaFashion) {
      val df = readExcel(
        "file:///C:\\Users\\dell\\Desktop\\乱七八糟的文件\\小家道具\\gacha-fashion.xlsx",
        sheet
      ).where("goods_id not in ( 'int','string')")
        .selectExpr(
          "goods_id",
          "name",
          "type",
          "bone_type",
          "gender",
          "image",
          "price",
          "desc",
          "priority",
          "gacha_id",
          "weight",
          "rare",
          "recycle_price"
        )
      if (dataFrame == null) {
        dataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)
        dataFrame.show(10)
      }
      dataFrame = dataFrame.union(df)
    }
    //dataFrame.show(1000)

    dataFrame
      .where("goods_id not in ( 'int','string')")
      .write
      .mode(SaveMode.Append)
      .saveAsTable("xianqueqiao_dim.dim_opera_house_items")
  }
}
