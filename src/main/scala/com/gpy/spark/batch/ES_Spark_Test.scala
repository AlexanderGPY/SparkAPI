package com.gpy.spark.batch

import org.apache.spark.sql.functions.{col, udf}
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
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "d2zN9WGupgqL4Rhq")
      .getOrCreate()
    val query: String =
      """{
        |    "bool": {
        |      "must": [],
        |      "filter": [
        |        {
        |          "bool": {
        |            "filter": [
        |              {
        |                "bool": {
        |                  "should": [
        |                    {
        |                      "match": {
        |                        "bn": "cmd_result"
        |                      }
        |                    }
        |                  ],
        |                  "minimum_should_match": 1
        |                }
        |              },
        |              {
        |                "bool": {
        |                  "filter": [
        |                    {
        |                      "bool": {
        |                        "should": [
        |                          {
        |                            "match": {
        |                              "cmd": "pair_info_get_no_auth"
        |                            }
        |                          }
        |                        ],
        |                        "minimum_should_match": 1
        |                      }
        |                    },
        |                    {
        |                      "bool": {
        |                        "should": [
        |                          {
        |                            "match": {
        |                              "node_code": -90
        |                            }
        |                          }
        |                        ],
        |                        "minimum_should_match": 1
        |                      }
        |                    }
        |                  ]
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "range": {
        |            "@timestamp": {
        |              "format": "strict_date_optional_time",
        |              "gte": "2021-07-27T02:26:27.199Z",
        |              "lte": "2021-07-29T02:26:27.199Z"
        |            }
        |          }
        |        }
        |      ],
        |      "should": [],
        |      "must_not": []
        |    }
        |  }""".stripMargin

    //大概耗时三分钟
    val df: DataFrame = spark.esDF("dbay-backend-2021-07-2*", query)

    val this_time = udf(MyUdf.turn_timestamp)
    df
      .select("open_id", "@timestamp")
      .orderBy("@timestamp")
      .dropDuplicates()
      .coalesce(1)
      .write
      .json(
        "file:///D:\\ideaProject\\sparkAPI\\result_data//es_spark_download7"
      )
  }
}
