package com.gpy.spark.batch
import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2022/1/14 17:58:57
  */
object SparkRedis {

  def main(args: Array[String]): Unit = {
    // 建立连接
    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("myApp")
        // initial redis host - can be any node in cluster mode
        .set("spark.redis.host", "192.168.1.226")
        // initial redis port
        .set("spark.redis.port", "6379")
        // optional redis AUTH password
        .set("spark.redis.auth", "L1DgN31E6qBOrxNAig0Q")
    )
    // 读取set数据
//    val setRDD: RDD[String] = sc.fromRedisSet("stress_test_random_users")
//    setRDD.take(100).foreach(println)

    //读取hash数据
    //val hashRDD: RDD[(String, String)] = sc.fromRedisHash("gpy-*")
    val hashRDD: RDD[(String, String)] = sc.fromRedisHash(Array("gpy-test", "gpy-test-hset"))
    hashRDD.take(100).foreach(println)

  }
}
