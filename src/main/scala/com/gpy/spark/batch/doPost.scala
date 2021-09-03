package com.gpy.spark.batch

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/9/3 下午 6:34
  */

import java.util
import java.util.{ArrayList, List}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.slf4j.{Logger, LoggerFactory}

object doPost {
  private val logger = LoggerFactory.getLogger("ProviderLog")
  def postResponse(
    url: String,
    params: String = null,
    body: JSONObject = null
  ): Unit = {
    val httpclient = HttpClients.createDefault

    //转化组装json
    val httpPost = new HttpPost(url)
    val formEntity = new util.ArrayList[NameValuePair](0)
    httpPost.addHeader("Content-Type", "application/json;charset=utf-8")
    //参数
    formEntity.add(
      new BasicNameValuePair("key", "d4ab1ceb-97d1-4192-aad5-212149acce64")
    )
    httpPost.setEntity(new UrlEncodedFormEntity(formEntity, "utf-8"))
    //json body实体
    httpPost.setEntity(new StringEntity(body.toJSONString, "utf-8"))
    var response: CloseableHttpResponse = null
    try { // 执行请求
      response = httpclient.execute(httpPost)
      val content = EntityUtils.toString(response.getEntity, "UTF-8")
      // 判断返回状态是否为200
      if (response.getStatusLine.getStatusCode == 200)
        //内容输出
        logger.info("YunWen: " + content)
      else logger.info("YunWen: " + content)
    } finally {
      if (response != null) response.close()
      httpclient.close()
    }
  }

}
