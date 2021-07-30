package com.gpy.spark.batch

import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.spark.sql.execution.streaming.sources.TextSocketReader.DATE_FORMAT

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/4/16 下午 1:06
  */
object MyUdf {

  /**
    * 给表打分组标签：按某一列值区间分组
    */
  val add_tag_num: (Int, Int) => Int = (love_days: Int, num: Int) => {
    var cnt = 0
    (1 until num).map { i =>
      if (love_days % num == 0) cnt = love_days
      else if ((love_days - i) % num == 0) cnt = love_days - i
      cnt
    }
    cnt
  }

  /**
    * 分组 90天以内算新用户  365天以上算老用户，中间值取0
    */

  val jug_user_type_new_old: (Int, Int) => String =
    (days: Int, last_act: Int) => {
      if (days <= 365) "new"
      else if (days > 365 && last_act <= 365) "old"
      else "0"
    }

  /**
    * 分组  开通时恋爱时长三十天以上 或者三十天以下 分两组
    */
  val jug_user_type_love_days: (Int) => String = (days: Int) => {
    if (days <= 30) "30days-"
    else "30days+"
  }

  /**
    * 分组 按国家组合分组
    */

  val jug_user_country_type: (String, String) => String =
    (a_c: String, b_c: String) => {
      (a_c, b_c) match {
        case ("中国", "澳大利亚") => "中澳"
        case ("澳大利亚", "中国") => "中澳"
        case ("中国", "美国")   => "中美"
        case ("美国", "中国")   => "中美"
        case ("中国", "新西兰")  => "中新"
        case ("新西兰", "中国")  => "中新"
        case ("中国", "加拿大")  => "中加"
        case ("加拿大", "中国")  => "中加"
        case ("中国", "英国")   => "中英"
        case ("英国", "中国")   => "中英"
        case (_, _)         => "other"
      }
    }

  /**
    * get age  通过生日判断当前的年龄
    */
  val jug_age: String => Int = {
    case "NULL" => -1
    case birthday =>
      val birth = birthday.split("-")
      val year = birth(0).toInt
      val month = birth(1).toInt
      val day = birth(2).toInt

      val curYear = 2021
      val curMonth = 4
      val curDay = 15

      var age = curYear - year
      if (curMonth < month) //当前月份小于出生月份age减一
        age -= 1;
      else if (curDay < day && month == curMonth) //当前月份等于出生月份
        // 并且当前日期小于出生日期age减一
        age -= 1;
      age
  }

  /**
    * 判断是否异国
    */

  val jug_lovers_country_type: (String, String) => String =
    (a_c: String, b_c: String) => {
      if (a_c == b_c) "同国" else "异国"
    }

  val ip2Long: (String) => Long = (ip: String) => {
    //固定的算法
    val ips: Array[String] = ip.split("\\.")
    var ipNum: Long = 0L
    for (i <- ips)
      ipNum = i.toLong | ipNum << 8L
    ipNum
  }

  val turn_timestamp: String => Long = (time: String) => {
    val timeLong: Long = DATE_FORMAT.parse(time).getTime
    timeLong
  }

}
