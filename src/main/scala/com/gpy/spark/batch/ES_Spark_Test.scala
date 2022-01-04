package com.gpy.spark.batch
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.gpy.spark.SparkWeb.doPost
import org.apache.spark.sql.functions.{col, collect_set, concat, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql._

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/6/8 下午 4:16
  */
object ES_Spark_Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ES读取数据")
      .master("local[*]")
      .config(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      .config(ConfigurationOptions.ES_NODES, "192.168.248.7")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .config("spark.debug.maxToStringFields", 1000)
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "d2zN9WGupgqL4Rhq")
      .getOrCreate()
    val query: String =
      """
        |{
        |    "bool": {
        |      "must": [],
        |      "filter": [
        |        {
        |          "bool": {
        |            "should": [
        |              {
        |                "match": {
        |                  "sv": "wxmp_sweet"
        |                }
        |              }
        |            ],
        |            "minimum_should_match": 1
        |          }
        |        },
        |        {
        |          "range": {
        |            "@timestamp": {
        |              "format": "strict_date_optional_time",
        |              "gte": "2021-12-23T16:00:00.000Z",
        |              "lte": "2021-12-24T05:00:00.000Z"
        |            }
        |          }
        |        }
        |      ],
        |      "should": [],
        |      "must_not": []
        |    }
        |  }
        |""".stripMargin

    //大概耗时三分钟
    val df: DataFrame = spark.esDF("dbay-frontend-2021-12-23", query)

    val df2: DataFrame = df
      .where("space_id is not null and user_id is not null and space_id !=0 and user_id!=0")
      .selectExpr("space_id", "user_id")
      .toDF("space_id", "user_id")
    df2
      .coalesce(1)
      .write
      .text("file:///D:\\ideaProject\\sparkAPI\\result_data\\wxmp_sweet")
  }
}
