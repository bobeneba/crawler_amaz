package com.yks.allproduct

import spider.man.parse.JsoupSupport
import scala.util.parsing.json.JSON
import spider.man.parse.AsJson
import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import scala.math.ScalaNumber
import org.jsoup.select.Evaluator.IsEmpty
import scala.io.Source
import java.util.ArrayList
import java.net.URLEncoder
import java.net.URLDecoder
import scala.util.Try

trait Action extends JsoupSupport with AsJson with SpiderFetch {

  /**
   * 解析动态页面列表
   * 抓取的字段：评论数，图片地址
   *
   */
  def parseDocument(h: String): List[SKU] = {
    var list = List[SKU]()
    var arr = new ArrayList
    //   商品id   图片  点赞数   留言    商品名  价格
    h.parseJson("list", arr)("(function(){var arr=json.items;for(var i=0;i<arr.length;i++){list.add(arr[i].itemid+'=='+arr[i].image +'=='+arr[i].liked_count +'=='+arr[i].recommend +'=='+arr[i].name  +'=='+arr[i].price   )   } })()")
    for (i <- 0 to (arr.size() - 1)) {
      var shuzu = ("" + arr.get(i)).split("==")
      var sku = new SKU
      sku.setCommentNum(shuzu(3))
      sku.setImage(currentUrl.replace("mall", "f").split("search")(0) + "file/" + shuzu(1))
      sku.setLikeNum(shuzu(2))
      sku.setProductId(shuzu(0))
      println(currentUrl + "商品名称" + shuzu(4))
      sku.setItemName(zifuChange(shuzu(4))) //""+shuzu(4).replace("\ud83d\udc6f", "")
      var realprice = shuzu(5).toDouble / 100000
      sku.setPrice(realprice + "")
      list = list.::(sku)
    }
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
  //改变字符  对中文无效
  def setCharact(str: String): String = {
    var jieguo = ""
    var strEncod = URLEncoder.encode(str)

    println("urlencode" + strEncod)
    println("urldcode" + URLDecoder.decode(strEncod))
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
          strEncod = strEncod.replace(strEncod.substring(dw, dw + 3), "")
        }
      }
    }
    jieguo
  }

  //链接id,抓取分类信息
  def parseTwoDocument(h: String, sku: SKU): List[SKU] = {
    var list = List[SKU]()
    var arr = new ArrayList[String]
    h.parseJson("list", arr)("(function(){ list.add(json.item_detail.catname) ; list.add(json.item_detail.sub_catname)  })()")
    println("分类" + arr.get(0) + ">" + arr.get(1))
    sku.setItemKind(arr.get(0) + ">" + arr.get(1))
    list
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


