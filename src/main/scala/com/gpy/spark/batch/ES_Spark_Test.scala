package com.gpy.spark.batch

import com.alibaba.fastjson.JSONObject
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
      .config(ConfigurationOptions.ES_NODES, "192.168.248.14")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "QA2JB7BoHpbBtTd")
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
        |                  "sv": "farm"
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
        |              "gte": "2021-08-28T16:00:00.000Z",
        |              "lte": "2021-09-04T15:59:59.999Z"
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
    val df: DataFrame = spark.esDF("dbay-frontend-2021-09-0*", query)

    val this_time = udf(MyUdf.turn_timestamp)
    df.groupBy("sv", "bn", "url", "err_code", "err_msg")
      .count()
      .filter("count >= 10")
      .orderBy("sv", "bn", "url")
      .toJSON
      .foreach { row =>
        val jo = new JSONObject()
        val joo = new JSONObject()
        joo.put("content", row)
        jo.put("msgtype", "text")
        jo.put("text", joo)
        println(jo)
        doPost.postResponse(
          "https://qyapi.weixin.qq.com/cgi-bin/webhook/send",
          "d4ab1ceb-97d1-4192-aad5-212149acce64",
          jo
        )
      }
  }
}
