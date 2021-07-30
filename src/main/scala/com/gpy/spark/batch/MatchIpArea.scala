package com.gpy.spark.batch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, desc, percent_rank, rank, sum, udf}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/3/30 下午 4:49
  */
object MatchIpArea {

  /**
    * 二分查找
    */
  def binarySearch(
    ipNum: Long,
    broadcastValue: Array[(Long, Long, String, String, String, String, String)]
  ): Int = {
    //开始下标
    var start = 0
    //结束下标
    var end = broadcastValue.length - 1

    while (start <= end) {
      var middle = (start + end) / 2
      if (
        ipNum >= broadcastValue(middle)._1.toLong && ipNum <= broadcastValue(
          middle
        )._2.toLong
      )
        return middle
      if (ipNum < broadcastValue(middle)._1.toLong)
        end = middle
      if (ipNum > broadcastValue(middle)._2.toLong)
        start = middle
    }
    -1
  }

  //将ip地址转化为Long
  def ip2Long(ip: String): Long = {
    //固定的算法
    val ips: Array[String] = ip.split("\\.")
    var ipNum: Long = 0L
    for (i <- ips)
      ipNum = i.toLong | ipNum << 8L
    ipNum
  }

  def MergeIpAddressOfCsv(
    user_info_rdd: RDD[String],
    spark: SparkSession
  ): DataFrame = {
    //1.构建本地测试环境
    //val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("testDemo").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val input = "file:///D:\\ideaProject\\sparkAPI\\data/ip.merge.txt"

    //2.加载维度文件
    val dim_rdd = sc.textFile(input)
    //3.读取维度信息成地区IP信息
    val dim_area_rdd = dim_rdd
      .map(line => line.split("[|]"))
      .map(array =>
        (
          ip2Long(array(0)),
          ip2Long(array(1)),
          array(2),
          array(3),
          array(4),
          array(5),
          array(6)
        )
      )

    //将地区ip信息广播到每一个worker节点
    val cityIpBroadcast = sc.broadcast(dim_area_rdd.collect())

    //4.获取日志数据,获取所有ip地址
    val ip_rdd = user_info_rdd
      .map(line => line.split(","))
      .map(array =>
        (
          array(0),
          ip2Long(array(1))
        )
      )
    //ip_rdd.foreach(println)

    //5.遍历匹配
    val result = ip_rdd.mapPartitions { itr =>
      val bv = cityIpBroadcast.value

      itr.map { ip =>
        val user_id = ip._1
        val ip_a = ip._2

        val index_a = binarySearch(ip_a, bv)

        (
          ip_a,
          user_id,
          bv(index_a)._3,
          bv(index_a)._4,
          bv(index_a)._5,
          bv(index_a)._6
        )
      }
    }
    //result.foreach(println)

    //6.合并处理
    val user_info_df =
      ip_rdd.toDF(
        "user_id",
        "ip"
      )
    //user_info_df.show()
    val ip_area_df =
      result.toDF("ip", "user_id", "country", "area", "province", "city")
    //ip_area_df.show()
    val result_df =
      user_info_df.join(
        ip_area_df,
        user_info_df("ip") === ip_area_df("ip") and user_info_df(
          "user_id"
        ) === ip_area_df("user_id"),
        "left"
      )
    //7.输出一下

    result_df.drop("ip").drop(ip_area_df("user_id"))
  }
  def MergeIpAddressOfJson(
    user_info_rdd: DataFrame,
    spark: SparkSession
  ): DataFrame = {
    //1.构建本地测试环境
    //val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("testDemo").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val input = "file:///D:\\ideaProject\\sparkAPI\\data/ip.merge.txt"

    //2.加载维度文件
    val dim_rdd = sc.textFile(input)
    //3.读取维度信息成地区IP信息
    val dim_area_rdd = dim_rdd
      .map(line => line.split("[|]"))
      .map(array =>
        (
          ip2Long(array(0)),
          ip2Long(array(1)),
          array(2),
          array(3),
          array(4),
          array(5),
          array(6)
        )
      )

    //将地区ip信息广播到每一个worker节点
    val cityIpBroadcast = sc.broadcast(dim_area_rdd.collect())
    //注册UDF
    val ip2long = udf(MyUdf.ip2Long)

    //4.获取日志数据,获取所有ip地址
    val user_ip_df =
      user_info_rdd
        .toDF("ip_addr", "user_id")
        .withColumn("long_ip", ip2long(col("ip_addr")))
        .selectExpr("user_id", "long_ip")
    //ip_rdd.foreach(println)

    //5.遍历匹配
    val result = user_ip_df.mapPartitions { itr =>
      val bv = cityIpBroadcast.value

      itr.map { ip =>
        val user_id = ip.get(0).toString.toLong
        val ip_a = ip.get(1).toString.toLong

        val index_a = binarySearch(ip_a, bv)

        (
          ip_a,
          user_id,
          bv(index_a)._3,
          bv(index_a)._4,
          bv(index_a)._5,
          bv(index_a)._6
        )
      }
    }

    //user_info_df.show()
    val ip_area_df =
      result.toDF("ip", "user_id", "country", "area", "province", "city")
    //ip_area_df.show()
    val result_df =
      user_ip_df
        .join(
          ip_area_df,
          user_ip_df("long_ip") === ip_area_df("ip") and user_ip_df(
            "user_id"
          ) === ip_area_df("user_id"),
          "left"
        )
        .select(
          user_ip_df("user_id"),
          col("country"),
          col("area"),
          col("province"),
          col("city")
        )
    //7.输出一下
    result_df
    //result_df.union(result_df1).union(result_df2).coalesce(1).write.option("header", "true").option("encoding", "UTF-8").csv("result_data/user_info_join_area")

  }
}
