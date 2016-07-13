package com.yks.allproduct

import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import spider.man.parse.JsoupSupport
import spider.man.io.db
import spider.man.io.LocalIO
import spider.man.io.DBIO
import spider.man.fork.ForkJoin
import java.net.URLEncoder
import java.util.Date
import java.net.URLDecoder

/**
 * 根据店铺链接抓取所有商品
 */
object AllProductByStoreUrl extends App with SpiderFetch with JsoupSupport with HttpSupport with Action with DBIO with LocalIO with ForkJoin {

  var threadnum = 0
  fromDB("allkind", "select thread_num  from  thread_control  where id=4").foreach { x =>
    {
      threadnum = x.getString("thread_num").toInt
    }
  }

  //创建ebean
  var ebean = db("allitem", threadnum)
  //创建修改状态ebean
  var ebean2 = db("allitem_in", 20)
  //创建请求头
  val headers = createHeaders.append("Upgrade-Insecure-Requests", "1")
  while (true) {
    fromDB("allitem", "select `id`, `store_url`, `store_id`, `store_products_num`, `country`, `act_status`  from   shopee_allproduct_in where act_status=1").foreach(x => {
      var productin = new AllproductIn
      productin.setId(x.getString("id").toInt)
      productin.setActStatus("2")
      productin.setBeginTime(new Date)
      productin.setCountry(x.getString("country"))
      productin.setStoreId(x.getString("store_id"))
      productin.setStoreProductsNum(x.getString("store_products_num"))
      productin.setStoreUrl(x.getString("store_url"))
      ebean2.update(productin)
      var url = x.getString("store_url")
      var storeId = x.getString("store_id")
      var productNum = x.getString("store_products_num").toInt
      //计算总页数
      var pNum = pageNum(productNum)
      var country = x.getString("country")
      var count = 0
      //拼接url
      var turl = realUrl(url, storeId)
      //根据总页数循环
      for (i <- 0 to pNum - 1) {
        //拼接出返回店铺商品json的链接
        val realu = turl + "&newest=" + i * 50 + "&limit=50"
        var allUrl = page(realu, x => x, checkBoundary, urlFactory)(x => false)("utf-8")(headers)()
        forkJoin[String](allUrl, u1 => {
          val list = fetch(u1, x => x, parseDocument)(x => false)("utf-8")(headers)()
          list.foreach { source =>
            var urlState = x.getString("store_url").split("/shop/")(0)
            //根据storeid和itemid拼接出商品详情页链接，抓取商品分类信息
            var newurl = urlState + "/shop/" + storeId + "/item/" + source.getProductId()
            var list2 = fetch(newurl, x => x, h => parseTwoDocument(h, source))(x => false)("utf-8")(headers)()
            println(source.getItemName())
            source.setCrawlerTime(new Date)
            source.setItemUrl(urlState + "/item/?sid=" + storeId + "&iid=" + source.getProductId())
            source.setCountry(country)
            source.setStoreUrl(url)
            source.setFromId(x.getString("id"))
            ebean.save(source)
            count += 1
          }
        })
      }
      println("打印总数" + count)
      productin.setActStatus("3")
      productin.setEndTime(new Date)
      ebean2.update(productin)
    })
    Thread.sleep(5000)
  }
  //拼接url
  def realUrl(url: String, storeid: String): String = {
    var t1 = url.split("shop/")(0) + "search/api/items/?page_type=shop&match_id=" + storeid + "&facet_type=shop_facet&by=ctime&order=desc&need_drop_word=false"
    t1
  }
  //计算分页总数
  def pageNum(productnum: Int): Int = {
    var num = 0
    var t1 = productnum % 50
    if (t1 == 0) {
      num = productnum / 50
    } else {
      num = productnum / 50 + 1
    }
    num
  }

}

//🎀👑CHARZOE👑🎀紫晶手串
//%F0%9F%8E%80%F0%9F%91%91CHARZOE%F0%9F%91%91%F0%9F%8E%80%E7%B4%AB%E6%99%B6%E6%89%8B%E4%B8%B2
//%F0%9F%8E%80%F0%9F%91%91

//👑🎀CHARZOE🎀👑白晶手串
//%F0%9F%91%91%F0%9F%8E%80CHARZOE%F0%9F%8E%80%F0%9F%91%91%E7%99%BD%E6%99%B6%E6%89%8B%E4%B8%B2

