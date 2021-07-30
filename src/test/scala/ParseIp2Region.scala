import java.io.{File, PrintWriter}
import java.sql.DriverManager

import com.gpy.spark.batch.LocalToHiveJdbcTest.driverName
import com.gpy.spark.batch.MatchIpArea
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, countDistinct, desc, sum}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/6/28 下午 7:52
  */
object ParseIp2Region {
  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    //1.构建本地测试环境
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val user_info_file = "file:///D:\\ideaProject\\sparkAPI\\data/user_ip1.json"
    val winSec = Window.partitionBy("country")
    //2.加载维度文件
    val user_info_rdd = spark.read.json(user_info_file)
    // 合并IP和用户信息
    val user_info_df = MatchIpArea.MergeIpAddressOfJson(user_info_rdd, spark)

    val res1 = user_info_df
      .where("province !='0'")
      .dropDuplicates()
      .groupBy("country", "province")
      .agg(countDistinct("user_id").alias("act_number"))
      .select(
        col("country"),
        col("province"),
        col("act_number"),
        (col("act_number") / sum("act_number")
          .over(winSec)).*(100).alias("province_percent")
      )

    res1
      .orderBy(
        asc("country"),
        desc("province_percent")
      )
      .coalesce(1)
      .write
      .option("header", "true")
      .option("encoding", "UTF-8")
      .csv("file:///D:\\ideaProject\\sparkAPI\\result_data/周活跃省市分布")

    //结束时间
    val end_time = System.currentTimeMillis()
    println("time_cost:" + (end_time - start_time) / 1000)
  }
}
