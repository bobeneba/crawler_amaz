package com.yks.amazoncategory

import spider.man.crawler.SpiderFetch
import spider.man.io.DBIO
import spider.man.misc.HttpSupport
import spider.man.parse.JsoupSupport
//import spider.man.logger.Logger
import java.io.PrintWriter
import java.io.File
import org.apache.log4j.Logger
import java.io._

object Category extends App with SpiderFetch with DBIO with HttpSupport with JsoupSupport {
  var fetchUrl = ""
  var baseUrl = "https://www.amazon.com"
  var header = createHeaders.append("Upgrade-Insecure-Requests", "1")
  var sqlRow = fromDB("db_amazon", "select category_url from amazon_categroy_url where category_id='10030796011' ")
  val logger = Logger.getLogger(Category.getClass.getName)

  sqlRow.foreach(record => {

    var hh = fetch(record.getString("category_url"), x => x, parseDocument _)(x => false)("utf-8")(header)()
    hh.foreach(pcan => {

      var bseUrl = "https://www.amazon.com/s/ref=sr_pg_" + "2" + "?rh=" + pcan.getRh() + "&page=" + "2" + "&ie=UTF8" + "&qid=" + pcan.getQid() + "&spIA=" + pcan.getSpIA()
      var listUrls =page(bseUrl, x => x, checkBoundary, urlFactory)(x => false)("utf-8")(header)()
      var n=0
      println(listUrls.size)
      listUrls.foreach(listUrl=>{
        n+=1
       println( "[factoryURL crawler start]::"+"["+n+"] ::"+listUrl+" di "+n+"ci")
        val producturl = fetch(listUrl,x=>x,parseDocumentProduct _)(x => false)("utf-8")(header)()
        
      })
      
    })

  })

  def urlFactory(h: String, num: Int): String = {
    var src = h
    var t1 = src.substring(0, src.indexOf("ref=sr_pg_"))
    var t2 = src.substring(src.indexOf("?rh=") + 4, src.indexOf("&page="))
    var t3 = src.substring(src.indexOf("&qid=") + 5, src.indexOf("&spIA="))
    var t4 = src.substring(src.indexOf("&spIA=") + 6, src.length)
    var urls = t1 + "ref=sr_pg_" + num + "?rh=" + t2 + "&page=" + num + "&ie=UTF8&qid=" + t3 + "&spIA=" + t4
    urls
  }
  def checkBoundary(h: String): Int = {
//    var writer = new PrintWriter(new File("page.html"))
//    writer.write(h)
//    writer.flush()
//    writer.close()
    var document = parseHtml(h)
    var num = document.getElementsByAttributeValue("class", "pagnDisabled").text.toInt
    num
  }
  /**
   * *
   * 获取拼接url的参数，三个。分类名称， 分类ID
   */
  def parseDocument(h: String): List[AmazonCategoryModel] = {

    var lsit = List[AmazonCategoryModel]()
    var doc = parseHtml(h)
    var tt = doc.getElementsByAttributeValue("class", "pagnLink").get(0).getElementsByTag("a").attr("href")
    var rh = tt.substring(tt.indexOf("rh=") + 3, tt.indexOf("&page"))
    var qid = tt.substring(tt.indexOf("qid=") + 4, tt.indexOf("&spIA"))
    var spIA = tt.substring(tt.lastIndexOf("spIA=") + 5, tt.length())

    var data = new AmazonCategoryModel()
    data.setRh(rh)
    data.setQid(qid)
    data.setSpIA(spIA)
    lsit = lsit.::(data)
    lsit
  }
  def parseDocumentProduct(h: String): List[AmazonProductModel] = {
      var lsit = List[AmazonProductModel]()
      /***
       * item 唯一表识别
       */
      var item = ""
      var title = ""
      var product_url= ""
      var Brands = ""
      var image_url = ""
      var price = ""
      var shipping = ""
      var review = ""
      var review_rate = ""
      var follow_seller =""
      var category = ""
      var catch_time = ""
      
  //  val from = h.indexOf("""<script>P.when""")
   //  val to = h.indexOf("""2});});</script>"""")
      var doc = parseHtml(h)
   // println(doc)
      println("*************************************************")
    // var htm=h
   // htm= htm.replace("<script>P.when", "").replace("2});});</script>", "")
    // println(htm)
  
      //var ko = parseHtml(htm)
    //btfResults  s-result-list
    //s-results-list-atf
   
    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx") 
    val baseUl = doc.getElementById("atfResults")
    val baseLiArray= baseUl.children()
    val baseLi= baseLiArray.get(0)
    val baseDivArray = baseLi.children()
    val baseDiv = baseDivArray.get(0)
//    println("[base]:"+baseUL.html())
//    println("[base1]:"+baseLI.html())
//    println("[base2]:"+base2.html())
//    println("[base3]:"+base3.html())
//    println("[base4]:"+base4.html())
    //println("[base5]:"+base5.html())
    //var product = doc.getElementById("atfResults").children().get(0).children().get(0).html()
    val content = baseDiv.getElementsByAttributeValue("class", "a-link-normal a-text-normal").attr("href")
     
     
 


    //var product1 = doc.getElementById("btfResults").html()
   // println(product)
     println("******************product***************") 
     println(content)
      //println(product1)
    var list = List[AmazonProductModel]()
   // <li id="result_0"
    //(?<=<node>)((.|[^.])*?)(?=</node>)
    //<div class="img_header hdr noborder" id="bottomBar">
   // val a = h.mkString
   // println(a)
//    val from = a.indexOf("""<li id="result_""")
//    val to = a.indexOf("""<li id="result_9602"""")
//   println(from+"::"+to)
//   println(a.substring(from+15 , to))
   
//     val a = """<li id="123">content0</li><li id="234">content1/ncontent2</li>"""
//    println(a)
    
   // val indexRege = """(?<=<li id=)((.|[^.])*?)(?=</li)""".r
    
   // println("rege**************************************")
  //  println( (indexRege findAllIn a).mkString(","))
   

 
 //   println(product)
        println("test 222")
    //Thread.sleep(90000)
    list
  }
}