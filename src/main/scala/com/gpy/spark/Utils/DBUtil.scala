package com.gpy.spark.Utils

import java.io.{PrintWriter, StringWriter}
import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import org.apache.log4j.Logger

/**
  * <p>创建时间: [2021-05-17]</p>
  * <p>描述: [数据库相关操作] </p>
  * @author williamzhang@welove-inc.com
  */
object DBUtil {
  private val log = Logger.getLogger(Thread.currentThread().getClass.getName)
  private var pool: LinkedBlockingQueue[Connection] = _

  /**
    * <p>描述: [从配置文件加载数据库配置，初始化数据库连接池] </p>
    * @param config
    * @author williamzhang
    */
  def initConn(config: Properties): Unit =
    this.synchronized {
      if (pool == null) {
        val PoolSize = 100
        val Driver = config.getProperty("driver")
        val Url = config.getProperty("url")
        val Username = config.getProperty("user")
        val Password = config.getProperty("password")
        Class.forName(Driver)
        pool = new LinkedBlockingQueue[Connection]()
        for (i <- 1 to PoolSize)
          pool.put(DriverManager.getConnection(Url, Username, Password))
        log.info("初始化数据库连接池")
      }
    }

  def getConn(config: Properties): Connection = {
    var conn: Connection = null
    try {
      if (pool == null) initConn(config)
      conn = pool.take()
    } catch {
      case e: Throwable =>
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        log.error(sw.toString())
    }
    conn
  }

  def releaseConnection(connect: Connection) {
    pool.put(connect)
  }

}
