package com.yks.crawler

import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import spider.man.parse.JsoupSupport
import spider.man.io.db
import spider.man.io.LocalIO
import spider.man.io.DBIO
import spider.man.fork.ForkJoin
import java.net.URLEncoder
import java.util.Date

object ShopeeCrawler extends App with SpiderFetch with JsoupSupport with HttpSupport with Actions with DBIO with LocalIO with ForkJoin {

  var threadnum = 0
  fromDB("allkind", "select thread_num  from  thread_control  where id=1").foreach { x =>
    {
      threadnum = x.getString("thread_num").toInt
    }
  }

  //创建ebean
  var ebean = db("allkind", threadnum)
  //创建修改状态ebean
  var ebean2 = db("allkind_in", 20)
  //创建请求头
  val headers = createHeaders.append("Upgrade-Insecure-Requests", "1")
  //循环，每次间隔5秒执行
  while (true) {
    fromDB("allkind", "select *  from  shopee_kind_in where act_status=1").foreach(x => {
      println("读取数据库" + x.getString("site_url"))
      var kindin = new KindIn
      kindin.setId(x.getString("id").toInt)
      kindin.setSiteUrl(x.getString("site_url"))
      kindin.setActStatus("2")
      kindin.setBeginTime(new Date)
      kindin.setEndTime(null)
      ebean2.update(kindin)
      //从数据库中读取站点链接
      var url = x.getString("site_url")
      var count = 0
      var allitem = 0
      var allUrl = page(url, x => x, checkBoundary, urlFactory)(x => false)("utf-8")(headers)()
      forkJoin[String](allUrl, u1 => {
        //抓取一级分类
        val list = fetch(u1, x => x, parseDocument)(x => false)("utf-8")(headers)()
        childForkJoin[SKU](list, source => {
          println("list循环" + source.getFirstKindUrl())
          //抓取二级分类
          var list2 = fetch(source.getFirstKindUrl(), x => x, h => parseTwoDocument(h, source))(x => false)("utf-8")(headers)()
          println("打印list2长度" + list2.size)
          list2.foreach { source =>
            //拼接出所有的二级分类链接
            var churl = changeUrl(source.getTwoKindUrl(), source.getTwoKindId())
            //抓取二级分类下的商品数量
            var list3 = fetch(churl, x => x, h => parseThirdDocument(h, source))(x => false)("utf-8")(headers)()
            source.setCrawlerTime(new Date)
            source.setCountry(urlcountry(x.getString("site_url")))
            source.setFromId(x.getString("id"))
            ebean.save(source)
            count += 1
            allitem += source.getAllItemNum().toInt
          }
        })
      })
      kindin.setActStatus("3")
      kindin.setEndTime(new Date)
      ebean2.update(kindin)
      println("打印总数" + count)
      println("打印总item" + allitem)
    })
    println("sdds")
    Thread.sleep(5000)
  }

  //http://mall.shopee.tw/search/api/items/?page_type=search&match_id=2164&by=ctime&order=desc&newest=0&limit=20&need_drop_word=false
  //拼接url
  def changeUrl(url: String, twoId: String): String = {
    var t = url.split("search-item")(0) + "search/api/items/?page_type=search&match_id=" + twoId + "&by=ctime&order=desc&need_drop_word=false"
    t
  }
  //截取字符串获得country的标识
  def urlcountry(str: String): String = {
    var con = str.substring(str.length() - 2, str.length())
    con
  }

}
