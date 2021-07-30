package com.gpy.spark.batch

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/5/11 下午 8:12
  */
object LocalToHbaseSparkTest {

  def main(args: Array[String]): Unit = {
    val spark     = SparkSession.builder().appName("Spark-HBase").master("local[2]").getOrCreate()
    val sc        = spark.sparkContext
    val tablename = "test:test"
    val conf      = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "192.168.248.36,192.168.248.24,192.168.248.64")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.table.sanity.checks", "false")
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    readHbase(spark, conf)
  }

  def readHbase(spark: SparkSession, conf: Configuration): Unit = {
    //从hbase中读取RDD
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    import spark.implicits._
    hbaseRDD
      .map({
        case (_, result) =>
          //通过列族和列名获取列
          val rowKey = Bytes.toString(result.getRow)
          val date   = Bytes.toString(result.getValue("cf".getBytes, "210401".getBytes))
          val hist   = Bytes.toString(result.getValue("cf".getBytes, "hist".getBytes))
          (rowKey, date, hist)
      })
      .toDF("rowKey", "date", "hist")
      .show(200)
  }
}
