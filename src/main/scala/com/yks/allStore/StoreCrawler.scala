package com.yks.allStore

import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import spider.man.parse.JsoupSupport
import spider.man.io.db
import spider.man.io.LocalIO
import spider.man.io.DBIO
import spider.man.fork.ForkJoin
import java.net.URLEncoder
import java.util.Date
import scala.util.control.Breaks

/**
 * 根据店铺链接抓取所有商品
 */
object StoreCrawler extends App with SpiderFetch with JsoupSupport with HttpSupport with Action with DBIO with LocalIO with ForkJoin {

  var threadnum = 0
  fromDB("allkind", "select thread_num  from  thread_control  where id=2").foreach { x =>
    {
      threadnum = x.getString("thread_num").toInt
    }
  }

  //创建ebean
  var ebean = db("allstore", threadnum)
  //创建修改状态ebean
  var ebean2 = db("allstore_in", 10)
  //创建请求头
  val headers = createHeaders.append("Upgrade-Insecure-Requests", "1")
  var count = 0
  while (true) {
    fromDB("allstore", "select `id`, `first_kind`, `two_kind`, `two_kind_url`, `two_kind_id`, `all_item_num`  from   shopee_allstore_in  where act_status=1").foreach(x => {
      var storein = new AllStoreIn
      storein.setActStatus("2")
      storein.setBeginTime(new Date)
      storein.setId(x.getString("id").toInt)
      storein.setAllItemNum(x.getString("all_item_num"))
      storein.setTwoKindUrl(x.getString("two_kind_url"))
      storein.setTwoKindId(x.getString("two_kind_id"))
      storein.setFirstKind(x.getString("first_kind"))
      storein.setTwoKind(x.getString("two_kind"))
      storein.setEndTime(null)
      ebean2.update(storein)
      var url = x.getString("two_kind_url")
      var twokindid = x.getString("two_kind_id")
      //店铺商品数量
      var productNum = x.getString("all_item_num").toInt
      if (productNum > 8000) {
        productNum = 8000
      }
      //根据总商品数计算出总页数（每页100条）
      var pNum = pageNum(productNum)
      var turl = realUrl(url, twokindid)
      var kind = x.getString("first_kind") + ">" + x.getString("two_kind")
      //根据数据分页循环
      for (i <- 0 to pNum) {
        val realu = turl + "&newest=" + i * 100 + "&limit=100"
        var allUrl = page(realu, x => x, checkBoundary, urlFactory)(x => false)("utf-8")(headers)()
        forkJoin[String](allUrl, u1 => {
          val list = fetch(u1, x => x, parseDocument)(x => false)("utf-8")(headers)()
          childForkJoin[SKU](list, source => {
            source.setKind(kind)
            source.setCrawlerTime(new Date)
            source.setFromId(x.getString("id"))
            ebean.save(source)
            count += 1
          })
        })
      }
      storein.setEndTime(new Date)
      storein.setActStatus("3")
      ebean2.update(storein)
    })
    Thread.sleep(5000)
  }
  println("打印总数" + count)
  //拼接url
  def realUrl(url: String, fenlei: String): String = {
    var t1 = url.split("search-item")(0) + "search/api/items/?page_type=search&match_id=" + fenlei + "&by=ctime&order=desc&need_drop_word=false"
    t1
  }
  //计算总页数方法
  def pageNum(productnum: Int): Int = {
    var num = 0
    var t1 = productnum % 100
    if (t1 == 0) {
      num = productnum / 100
    } else {
      num = productnum / 100 + 1
    }
    num
  }
}

