package com.gpy.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

object CommitOffsets {

  def saveOffsetsKafka(
    dstream: InputDStream[ConsumerRecord[String, String]]
  ): Unit =
    dstream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
}
