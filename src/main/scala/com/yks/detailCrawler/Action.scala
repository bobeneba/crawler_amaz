package com.yks.detailCrawler

import spider.man.parse.JsoupSupport
import scala.util.parsing.json.JSON
import spider.man.parse.AsJson
import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import scala.math.ScalaNumber
import org.jsoup.select.Evaluator.IsEmpty
import scala.io.Source
import java.util.ArrayList
import com.sun.jndi.ldap.Obj
import java.net.URLEncoder
import java.io.File
import com.sun.beans.decoder.TrueElementHandler
import java.net.URLDecoder

trait Action extends JsoupSupport with AsJson with SpiderFetch {

  /**
   * 解析动态页面列表
   * 抓取的字段：产品名称，产品ID，图片，详情链接，价格，评论数，店铺名称，店铺链接
   *
   */
  def parseDocument(h: String): List[SKU] = {
    var list = List[SKU]()
    var arr = new ArrayList[String]
    h.parseJson("list", arr)("(function(){ var arr=json.shop.name;list.add(arr);var arr2=json.shop.follower_count;list.add(arr2);var arr3=json.shop.item_count;list.add(arr3);  var arr4=json.shop.account.following_count;list.add(arr4);    var arr5=json.shop.total_avg_star;list.add(arr5);  list.add(json.shop.rating_normal+json.shop.rating_bad+json.shop.rating_good);    list.add(json.shop.shopid);             })()")
    var sku = new SKU
    sku.setStoreName(zifuChange(arr.get(0)))
    sku.setStoreFensi("" + arr.get(1))
    sku.setStoreProducts("" + arr.get(2))
    sku.setStoreGuanzhu("" + arr.get(3))
    sku.setCommentStar("" + arr.get(4))
    sku.setCommentNum("" + arr.get(5))
    sku.setStoreId("" + arr.get(6))
    list = list.::(sku)
    list
  }

  //改变特殊字符  新方法
  def zifuChange(str: String): String = {
    var bo = true
    var encode = URLEncoder.encode(str)
    while (bo) {
      var t1 = encode.indexOf("%F0")
      var zifu = ""
      if (t1 > (-1)) {
        try {
          zifu = encode.substring(t1, t1 + 12)
        } catch {
          //case t: Throwable => t.printStackTrace() // TODO: handle error
          case f: StringIndexOutOfBoundsException => {
            zifu = encode.substring(t1, t1 + 3)
          }
        }
        encode = encode.replace(zifu, "")
      } else {
        bo = false
      }
    }
    URLDecoder.decode(encode)
  }
  //改变特殊字符，针对中文无效
  def setCharact(str: String): String = {
    var jieguo = ""
    var strEncod = URLEncoder.encode(str)
    if (strEncod.indexOf("%") == (-1)) {
      jieguo = str
    } else {
      var xunhuan = true
      while (xunhuan) {
        var dw = strEncod.indexOf("%")
        if (dw == (-1)) {
          xunhuan = false
          jieguo = strEncod.replace("+", " ")
        } else {
          //   println("strEncod.subSequence(dw, 2)"+strEncod.substring(dw,dw+3))
          strEncod = strEncod.replace(strEncod.substring(dw, dw + 3), "")
        }
      }
    }
    jieguo
  }

  /**
   * 检查分页边界
   */
  def checkBoundary(h: String): Int = 1

  var currentUrl = ""

  /**
   * 页面链接制造工厂
   */
  def urlFactory(url: String, pageNo: Int) = urlchan(url, pageNo)

  def urlchan(url: String, pageNo: Int): String = {
    currentUrl = url
    url
  }
}


