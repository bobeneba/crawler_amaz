package com.yks.allStore

import spider.man.parse.JsoupSupport
import scala.util.parsing.json.JSON
import spider.man.parse.AsJson
import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import scala.math.ScalaNumber
import org.jsoup.select.Evaluator.IsEmpty
import scala.io.Source
import java.util.ArrayList

trait Action extends JsoupSupport with AsJson with SpiderFetch {

  def parseDocument(h: String): List[SKU] = {
    var list = List[SKU]()
    var arr = new ArrayList
    //商品id   图片  点赞数   留言    商品名  价格
    h.parseJson("list", arr)("(function(){var arr=json.items;for(var i=0;i<arr.length;i++){ list.add(arr[i].shopid+'=='+arr[i].country)    }    })()")
    for (i <- 0 to (arr.size() - 1)) {
      var shuzu = ("" + arr.get(i)).split("==")
      var sku = new SKU
      sku.setCountry(shuzu(1))
      sku.setStoreId(shuzu(0))
      var storeurl = currentUrl.split("search")(0) + "shop/#shopid=" + shuzu(0)
      sku.setStoreUrl(storeurl)
      list = list.::(sku)
    }
    list
  }

  //链接id
  def parseTwoDocument(h: String, sku: SKU): List[SKU] = {
    var list = List[SKU]()
    var arr = new ArrayList
    h.parseJson("list", arr)("(function(){ var arr=json.categories;for(var i=0;i<arr.length;i++){ list.add(arr[i].display_name+'_'+arr[i].catid) ;}  })()")
    for (i <- 0 to arr.size() - 1) {
      var realsku = new SKU
      var oneObj = "" + arr.get(i)
      var shuzu = oneObj.split('_')
      list = list.::(realsku)
    }
    list
  }

  /**
   * 检查分页边界
   */
  def checkBoundary(h: String): Int = 1
  //parseHtml(changeto(h)).getOneElement("Class", "select_page_num").$text.replaceAll(".*/", "").toInt

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


