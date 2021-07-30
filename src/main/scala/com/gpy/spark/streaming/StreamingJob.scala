package com.gpy.spark.streaming

import java.lang

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{
  DStream,
  InputDStream,
  ReceiverInputDStream
}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{
  CanCommitOffsets,
  HasOffsetRanges,
  KafkaUtils
}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/11/11 下午 6:05
  */
object StreamingJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("testDemo")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    //sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))
    //必须设置
    ssc.checkpoint("./check_dir")
    val dStream = getInPutDStream(ssc, "realTimeStat", Set("test"), "earliest")
    //val dataStream: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)

    dStream.foreachRDD(rdd => {
      rdd.foreach(x => {
        val json = x.value()
        val jo = JSON.parseObject(json)
        jo.getString("sv") match {
          case "farm" => {
            if (jo.getString("bn") == "login")
              println("登录" + jo.getString("time"))
          }
          case "tree" => {
            if (jo.getString("el") == "sunshine")
              println("晒太阳" + jo.getString("time"))
            else if (jo.getString("el") == "water")
              println("浇水" + jo.getString("time"))
          }

          case _ =>
        }
      })
    })
    //提交offsets
    saveOffsetsKafka(dStream)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取InputStream
    * @param ssc streamContext
    * @param Group ConsumerGroup
    * @param Topic KafkaTopic
    * @param Reset KafkaOffsets_location
    * @return InputDStream
    */

  def getInPutDStream(
      ssc: StreamingContext,
      Group: String,
      Topic: Set[String],
      Reset: String
  ): InputDStream[ConsumerRecord[String, String]] = {
    val KafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.0.1.6:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Group,
      "auto.offset.reset" -> (Reset: String),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val dstream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Topic, KafkaParams)
      )
    dstream
  }

  def saveOffsetsKafka(
      dstream: InputDStream[ConsumerRecord[String, String]]
  ): Unit = {
    dstream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
  }

  // 实时流量状态更新函数
  val mapFunction =
    (datehour: (String, String), pv: Option[Long], state: State[Long]) => {
      val accuSum = pv.getOrElse(0L) + state.getOption().getOrElse(0L)
      val output = (datehour, accuSum)
      state.update(accuSum)
      output
    }

}
