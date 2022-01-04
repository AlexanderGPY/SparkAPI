import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, desc}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/6/22 上午 11:32
  */
object Resorting_guide_data {
  def main(args: Array[String]): Unit = {
    //1.构建本地测试环境
    val spark = SparkSession
      .builder()
      .master("local[16]")
      .appName("testDemo")
      .getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.textFile(
      args(0)
    )
    val regex = """^\d+$"""
    val data = df.map { line =>
      val arr = line.split("[\t]")
      val date = arr(0)
      val dim = arr(1)
      val el = arr(2)
      val num = arr(3)
      (date, dim, el, num)
    }
    val res1 = data.toDF("date", "dim", "el", "num")
//    res1.show(100)
    res1
      .orderBy(asc("el"), asc("dim"), desc("date"))
      .coalesce(1)
      .write
      .csv(args(1))
  }
}
