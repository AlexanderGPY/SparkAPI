import org.apache.spark.sql.execution.streaming.sources.TextSocketReader.DATE_FORMAT

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/7/6 下午 6:07
  */
object MainTest {
  def main(args: Array[String]): Unit = {
    val time = "2021-07-06 08:55:26.659"
    val timeLong: Long = DATE_FORMAT.parse(time).getTime
    println(timeLong)
  }
}
