import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/6/9 下午 8:12
  */
object login_os_plat {
  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    //1.构建本地测试环境
    val spark = SparkSession
      .builder()
      .config("hive.metastore.uris", "thrift://192.168.248.32:7004")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[16]")
      .appName("testDemo")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val order = spark
      .sql("""SELECT user_id,
                            |       price
                            |FROM xianqueqiao_dwd.dwd_trade_lovespace_order
                            |WHERE c_date >="2021-05-01"
                            |  AND pay_date IS NOT NULL""".stripMargin)
      .groupBy("user_id")
      .agg(
        sum("price").alias("price")
      )
    val user_info_file =
      "file:///D:\\ideaProject\\sparkAPI\\result\\login_os_plat\\part-00000-a6a02599-9270-4b40-9631-5a5ba8510497-c000.json"
    val df = spark.read
      .json(user_info_file)

    df.selectExpr("user_id", "os", "platform")
      .dropDuplicates()
      .join(order, "user_id")
      .where(order("user_id").isNotNull)
      .groupBy(
        "platform",
        "os"
      )
      .agg(
        sum("price").alias("price")
      )
      .show()

    //程序运行时间
    val end_time = System.currentTimeMillis()
    println("time_cost: " + (end_time - start_time) / 1000 + " sec")
  }
}
