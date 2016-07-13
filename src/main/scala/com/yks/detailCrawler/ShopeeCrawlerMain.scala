package com.yks.detailCrawler

import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import spider.man.parse.JsoupSupport
import spider.man.io.db
import spider.man.io.LocalIO
import spider.man.io.DBIO
import spider.man.fork.ForkJoin
import java.net.URLEncoder
import java.util.Date

object ShopeeCrawlerMain extends App with SpiderFetch with JsoupSupport with HttpSupport with Action with DBIO with LocalIO with ForkJoin {

  var threadnum = 0
  fromDB("allkind", "select thread_num  from  thread_control  where id=3").foreach { x =>
    {
      threadnum = x.getString("thread_num").toInt
    }
  }

  //创建ebean
  var ebean = db("storedetail", threadnum)
  //创建修改状态ebean
  var ebean2 = db("storedetail_in", 20)
  //创建请求头
  val headers = createHeaders.append("Upgrade-Insecure-Requests", "1")
  while (true) {
    fromDB("storedetail", "SELECT `id`, `store_url`, `country`, `act_status`  FROM  shopee_storedetail_in WHERE act_status=1 GROUP BY `store_url` ").foreach(x => {
      var sd = new StoreDetailIn
      sd.setActStatus("2")
      sd.setBeginTime(new Date)
      sd.setId(x.getString("id").toInt)
      sd.setCountry(x.getString("country"))
      sd.setStoreUrl(x.getString("store_url"))
      ebean2.update(sd)
      var country = "" + x.get("country")
      var url = x.getString("store_url")
      var surl = url
      var count = 0
      //改变url，把店铺链接拼接成获取店铺详情json的链接
      url = urlChange(url)
      var allUrl = page(url, x => x, checkBoundary, urlFactory)(x => false)("utf-8")(headers)()
      forkJoin[String](allUrl, u1 => {
        val list = fetch(u1, x => x, parseDocument)(x => false)("utf-8")(headers)()
        childForkJoin[SKU](list, source => {
          source.setCountry(country)
          source.setCrawlerTime(new Date)
          source.setStoreUrl(surl)
          source.setFromId(x.getString("id"))
          ebean.save(source)
        })
      })
      sd.setEndTime(new Date)
      sd.setActStatus("3")
      ebean2.update(sd)
    })
    Thread.sleep(5000)
  }
  //拼接url
  def urlChange(url: String): String = {
    var realurl = url
    var t1 = url.indexOf("shopid")
    if (t1 > (-1)) {
      realurl = url.replace("/#shopid=", "/")
    }
    realurl
  }
}

//💕  🌹
