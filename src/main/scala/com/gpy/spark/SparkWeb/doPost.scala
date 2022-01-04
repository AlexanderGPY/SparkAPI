package com.gpy.spark.SparkWeb

import com.alibaba.fastjson.JSONObject
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/9/6 下午 3:39
  */
object doPost {
  private val logger = LoggerFactory.getLogger("ProviderLog")
  def postResponse(
    url: String,
    body: JSONObject = null
  ): Unit = {
    val httpclient = HttpClients.createDefault

    //转化组装json
    val httpPost = new HttpPost(url)
//    val formEntity = new util.ArrayList[NameValuePair](0)
    httpPost.addHeader("Content-Type", "application/json;charset=utf-8")
    //参数
//    formEntity.add(
//      new BasicNameValuePair("key", "d4ab1ceb-97d1-4192-aad5-212149acce64")
//    )
//    httpPost.setEntity(new UrlEncodedFormEntity(formEntity, "utf-8"))
    //json body实体
    httpPost.setEntity(new StringEntity(body.toJSONString, "utf-8"))
    var response: CloseableHttpResponse = null
    try { // 执行请求
      response = httpclient.execute(httpPost)
      val content = EntityUtils.toString(response.getEntity, "UTF-8")
      // 判断返回状态是否为200
      if (response.getStatusLine.getStatusCode == 200)
        //内容输出
        println(content)
      else println(content)
    } finally {
      if (response != null) response.close()
      httpclient.close()
    }
  }

}
