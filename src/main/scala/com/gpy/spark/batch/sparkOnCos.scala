package com.gpy.spark.batch

import com.gpy.spark.Utils.SessionHelper
import org.apache.spark.sql.SparkSession

import scala.Tuple2

/**
  * Created by tencent on 2018/6/28.
  */
object sparkOnCos {
  def main(args: Array[String]): Unit = {
    val spark = SessionHelper.SessionWithLocal("local")

    import spark.implicits._
    val sc = spark.sparkContext
    val rdd =
      sc.textFile("cosn://welove-log-1253362462/logs/2021/welove.log_2021-07-01.log.merge.tar.bz2")
    val rdd1 = rdd.filter(_.contains("Micro")).filter(_.contains(" ")).map { line =>
      val fields = line.split(" ")
      val types = fields(0)
      val date = fields(1)
      val spaceId = fields(7)
      val userId = fields(8)
      (types, date, spaceId, userId)
    }

    val df = rdd1.toDF("types", "date", "spaceId", "userId")

    df.groupBy("types", "date")
      .agg(org.apache.spark.sql.functions.countDistinct("userId").alias("uv"))
      .where("uv >= 1000")
      .orderBy("date")
      .show(50)
  }
}
