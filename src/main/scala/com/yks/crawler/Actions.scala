package com.yks.crawler

import spider.man.parse.JsoupSupport
import scala.util.parsing.json.JSON
import spider.man.parse.AsJson
import spider.man.misc.HttpSupport
import spider.man.crawler.SpiderFetch
import scala.math.ScalaNumber
import org.jsoup.select.Evaluator.IsEmpty
import scala.io.Source
import java.util.ArrayList

trait Actions extends JsoupSupport with AsJson with SpiderFetch {

  /**
   * 解析动态页面列表
   * 抓取的字段：产品名称，产品ID，图片，详情链接，价格，评论数，店铺名称，店铺链接
   *
   */
  def parseDocument(h: String): List[SKU] = {
    var list = List[SKU]()
    val doc = parseHtml(h)
    var t2 = doc.getElementsByClass("col-xs-4")
    for (i <- 0 to t2.size() - 1) {
      var sku = new SKU
      sku.setFirstKind(t2.get(i).text)
      sku.setFirstKindId(t2.get(i).$attr("data-category-id"))
      //http://mall.shopee.com.my/category/api/?category_id=16
      //var myurl = "http://mall.shopee.com.my"
      sku.setFirstKindUrl(currentUrl + "/category/api/?category_id=" + t2.get(i).$attr("data-category-id"))
      println("firstkind" + sku.getFirstKind() + "firstid" + sku.getFirstKindId())
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
      //var myurl = "http://mall.shopee.com.my"
      ///?utm_source=OrganicA&utm_medium=OrganicA&utm_campaign=lp_home_mall
      var listurl = currentUrl + "/search-item/?sub_categoryid=" + shuzu(1) + "&is_adult=0&categoryname=" + sku.getFirstKind() + "&categoryid=" + sku.getFirstKindId() + "&sub_cate_name=" + shuzu(0)
      realsku.setFirstKind(sku.getFirstKind())
      realsku.setFirstKindId(sku.getFirstKindId())
      realsku.setFirstKindUrl(sku.getFirstKindUrl())
      realsku.setCurrentUrl(currentUrl)
      realsku.setTwoKindUrl(listurl)
      realsku.setTwoKind(shuzu(0))
      realsku.setTwoKindId(shuzu(1))
      list = list.::(realsku)
    }
    list
  }

  def parseThirdDocument(h: String, sku: SKU): List[SKU] = {
    var list = List[SKU]()
    var arr = new ArrayList
    h.parseJson("list", arr)("(function(){ var arr=json.total_count;list.add(arr)})()")
    sku.setAllItemNum("" + arr.get(0))
    list = list.::(sku)
    list
  }
  /**
   * 检查分页边界
   */

  def checkBoundary(h: String): Int = 1
  //changeto(h)
  //parseHtml(changeto(h)).getOneElement("Class", "select_page_num").$text.replaceAll(".*/", "").toInt
  //所有产品数量
  def changeto(h: String): Int = {
    val zh = h.substring(11, h.length() - 2);
    val zzh = zh.replace("\\r", "").replace("\\n", "").replace("\\t", "").replace("\\", "")
    var productCountJson = parseHtml(zzh).getElementById("jsonValue").$attr("value") + ""
    println("productCountJson>>>" + productCountJson)
    var productNo = productCountJson.parseJson("(function(){return json.productCount})()") + ""
    println("productNo>>>" + productNo)
    1
  }

  def parseDetailHtml(h: String, sku: SKU): List[SKU] = {
    var list = List[SKU]()
    val doc = parseHtml(h)
    doc.getListElements("AttributeValue", item => {
      //销量
      var t1 = item.getElementById("mod_salesvolume")
    }, "class", "detail_information")
    list
  }

  var currentUrl = ""

  /**
   * 页面链接制造工厂
   */
  def urlFactory(url: String, pageNo: Int) = urlchan(url, pageNo)

  def urlchan(url: String, pageNo: Int): String = {
    println("url带过来" + url)
    currentUrl = url
    url
  }
}


