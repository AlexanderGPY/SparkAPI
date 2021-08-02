package com.gpy.spark.batch

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/5/12 下午 1:11
  */
object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    val rdd = sc.textFile("file:///home/hadoop/mytest/welove.log_2021-06-09*")

    val rdd1 = rdd.map(line => line.split(" ")(0))
    val df1 = rdd1.toDF("user_id")
    val df = df1.selectExpr("user_id", "'2021-06-09' as c_date")
    df.write.mode("append").saveAsTable("xianqueqiao_mid.mid_sweet_app_user")

    //以下为一次导入多天。
    val input =
      "file:///home/hadoop/mytest/welove.log_2021-0*"
    val res = get_hdfs_dir(input, sc)
    res.foreach { line =>
      val dateL = line.split("[.]")(1).split("_")(1)
      val rdd = sc.textFile(line)
      val rdd1 = rdd.map { line =>
        val uid = line.split(" ")(0)
        val date = dateL
        (uid, date)
      }
      val df1 = rdd1.toDF("user_id", "c_date")
      df1.write.mode("append").saveAsTable("xianqueqiao_mid.mid_sweet_app_user")
    }

  }

  def get_hdfs_dir(input: String, sc: SparkContext): Array[String] = {
    val fileRDD =
      sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](input)
    val hadoopRDD = fileRDD.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
    val fileAdnLine = hadoopRDD.mapPartitionsWithInputSplit {
      (inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) =>
        val file = inputSplit.asInstanceOf[FileSplit]
        iterator
          .take(1)
          .map { x =>
            file.getPath.toString
          }
    }
    val dirOut: Array[String] = fileAdnLine
      .coalesce(1)
      .map { lines =>
        lines
      }
      .collect()
    dirOut
  }
}
