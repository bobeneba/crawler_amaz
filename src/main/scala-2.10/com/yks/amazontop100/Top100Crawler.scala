package com.yks.amazontop100

import spider.man.parse.JsoupSupport
import spider.man.crawler.SpiderFetch
import spider.man.misc.HttpSupport
import spider.man.io.DBIO
import spider.man.fork.ForkJoin
import spider.man.io.db
import turbo.crawler.misc.ADSLUtils


object Top100Crawler extends App with SpiderFetch with DBIO with HttpSupport with JsoupSupport with ForkJoin {
  var ebean=db("db_amazon_top100",30)

  var header = createHeaders.append("Upgrade-Insecure-Requests", "1").append("User-Agent","Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36").append("Connection","keep-alive").append("host", "www.amazon.com").append("cookie","session-id=175-4489133-1558941; session-id-time=2082787201l; ubid-main=183-5766437-1916459; x-wl-uid=12RlB++jZczyxOyHktCwuV9J3AcbKZYd1+ZJlaobtJRTCE+3GGV2cV9YeoCuU+lj3s6bO6ur+OE4=; session-token=/FsKzbhRK7j9m1kz1GJTut2gi9vrknhA/bQhizC73KYl3aT3pqCNCgIUIXtkEAnovOp7p9NdCki1hp89B/JupLOu0tFkPV5SkPgljuoIJXEhtC0PEvaefhcZheC9R/MMwSQe4NSbfLI2a4F+S+C9XP0+UyO4spWPBD3QeBU4uxkNbeo7CI7YOWw+yUAdTPGa+v0SGuplzYqJxl39d4B8k0uhG4hto02EK3xKUUrgg7Tyd1f20mhwlHChqWl8pkrk; csm-hit=1WTMG8DXC1H3GRQ12JVN+s-1WTMG8DXC1H3GRQ12JVN|1468321387749; skin=noskin")
  var sqlRow = fromDB("db_amazon_top100", "select category_id ,category_top100_url from amazon_top100_url limit 0,500 ")
//  sqlRow.foreach(x=>{
//    println(x)
//    fromDB("db_amazon_top100", "update amazon_top100_url SET status = 1 where category_id = '"+x.getString("category_id")+"'")
//UPDATE persondata SET age=age*2, age=age+1;
//  
//  })
  
  
  sqlRow.foreach(record => {
    var baseurl = record.getString("category_top100_url")
    var tempurl = record.getString("category_top100_url").substring(0, baseurl.length())
//    for (i <- 1 to 5) {
//      var urlFactory = tempurl + "#" + i
      var hh = fetch(tempurl, x => x, parseDocument _)(x => false)("utf-8")(header)()
   //  var hh = fetchX(urlFactory, x=>x, parseDocument _)(hasRejected)(howToContinue)("utf-8")(header)(1)
      hh.foreach(db=>{
        ebean.save(db)
      })
      //      childForkJoin[Amazontop100Model](hh, dr=>{
      //        
      //        
      //      })
//    }
  })
//  def howToContinue(fetchUrl: String) {
////    println("continueFetch start")
////    var hh = fetch(fetchUrl, x => x, parseDocument _)(x => false)("utf-8")(header)()
////        println("continueFetch end")
////    println("sleep1")
////      Thread.sleep(6000)
////       println("sleep2")
//     println("屏蔽，在次抓取")
//
//  }
// def continueFetch(fetchUrl: String) = ADSLUtils.reconnect(fetchUrl)
  
//  def hasRejected(html:String):Boolean={
//       println("判断是否屏蔽")
//        var doc = parseHtml(html)
//        var flag= doc.getElementById("zg_centerListWrapper")
//        if(flag == null){
//          true
//        }else{
//          false
//        }
//
//   
//  }

  def parseDocument(h: String): List[Amazontop100Model] = {
    /**
     * *
     * top100 分类下的产品 ebean 字段对应
     * 排名，item,标题，产品链接，图片链接，售价价格，评论，评论分数，
     * 跟卖数量，分类名，分类id,抓取时间，评论链接,原始售价,跟卖,跟卖价格
     */
    var list = List[Amazontop100Model]()
    

    var zg_rankNumber = 0
    var item = ""
    var title = ""
    var product_url = ""
    var image_url = ""
    var price = ""
    var review = ""
    var review_rate = ""
    var follow_seller = ""
    var category = ""
    var category_id = ""
    var catch_time: Long = 0
    var review_url = ""
    var price_original = ""
    var follow_seller_price = ""
    var doc = parseHtml(h)
    /**
     * *
     * 分类名抓取
     */
    var categorydom = doc.getElementById("zg_listTitle")
    if (categorydom != null) {
      category = categorydom.children().get(0).text()
    } else {
      println("[Warning]:: fuck crawler: 类目名字屏蔽 ")
    }

    /**
     * *
     *  页面抓取基础dom
     */
    val baseZG = doc.getElementById("zg_centerListWrapper")
    if (baseZG == null) {
      println("[Error]::fuck crawler: 20个整个产品列表屏蔽")
    } 
    else 
    {
      val baseZGArray = baseZG.children()
      /**
       * *
       * 计算抓取的dom元素个数
       */
      val num = baseZGArray.size()
      /**
       * *
       * 循环20个产品页面，去除js标签文件
       */
      for (i <- 0 to num - 4) {
        if (i != 0 && i != 4 && i != 5 && i != 6) 
        {
          var temp = baseZGArray.get(i).getAllElements.size()
          var basedom = baseZGArray.get(i);
          println("*********amazon crawler start ***********")
          println("抓取" + i + "个产品")
          /**
           * *
           * 排名抓取
           */
          if (basedom == null) {
            println("[Warning]:: fuck crawler: 屏蔽产品排名 ")
          } else {
             zg_rankNumber = basedom.child(0).text().replace(".", "").toInt
            //println("zg_rankNumber" + zg_rankNumber)
           
           // println("aaa"+zg_rankNumber)
          }
          /**
           * *
           * 图片链接和产品链接，标题抓取，
           */
          var temp_base_url = basedom.child(1)
          var temp_image_url = temp_base_url.child(0)
          product_url = temp_image_url.getElementsByTag("a").attr("href").trim()
          if(product_url !=""){
         //   println(product_url)
           var  temp_category_id = product_url.substring(product_url.indexOf("ref=zg_bs_")+10,product_url.length)
         //  println("temp_category_id"+temp_category_id)
           category_id = temp_category_id.substring(0,temp_category_id.indexOf("_"))
           //println(category_id)
          }
          image_url = temp_image_url.getElementsByTag("img").attr("src")
          title = temp_image_url.getElementsByTag("img").attr("title")
          /**
           * *
           * 评论数，评分，评论链接，item ,原始售价，售价，跟卖数抓取
           */

          var temp_review = temp_base_url.getElementsByAttributeValue("class", "a-icon-alt").html()
          review_rate = temp_review

          review = temp_base_url.getElementsByAttributeValue("class", "a-link-normal").html()
          review = review.replace(",", "")
          review_url = temp_base_url.getElementsByAttributeValue("class", "a-link-normal a-text-normal").attr("href")
          if(review_url!="")
          {
           // println("item"+item)
            item = review_url.substring(38, 48)
          }
          // 
          price = temp_base_url.getElementsByAttributeValue("class", "zg_price").get(0).getElementsByAttributeValue("class", "price").html()
          price = price.replace("$", "")
          if(price.size>7) {
            price = price.substring(0,price.indexOf("-"))
          }
          price_original = temp_base_url.getElementsByAttributeValue("class", "listprice").html()
          price_original = price_original.replace("$", "")
          price_original = price_original.replace(",", "")

          var temp_foller = temp_base_url.getElementsByAttributeValue("class", "zg_usedPrice")
          /**
           * *
           * 判断是否存在跟卖标签存在
           */
          if (temp_foller.html() != "") {

            follow_seller_price = temp_foller.get(0).getElementsByAttributeValue("class", "price").html()
            follow_seller = temp_foller.get(0).child(0).html()

            follow_seller_price = follow_seller_price.replace("$", "")
            follow_seller_price = follow_seller_price.replace(",", "")

            follow_seller = follow_seller.replace("&nbsp;new", "")
            follow_seller = follow_seller.replace("used &amp; new", "")
             follow_seller = follow_seller.replace("&nbsp;used","")
            follow_seller = follow_seller.trim()

          }
          catch_time = System.currentTimeMillis().toLong

          if (follow_seller_price == "") {
            println("follow_seller_price is null" + follow_seller_price)
            follow_seller_price = "0"
          }
          if (price_original == "") {
            println("price_original is null" + price_original)
            price_original = "0"
          }
          if (follow_seller == "") {
            println("follow_seller is null" + follow_seller)
            follow_seller = "0"
          }
           if (price == "") {
            println("follow_seller is null" + price)
            price = "0"
          }
           if(review == ""){
             println("follow_seller is null" + price)
            review = "0"
           }

          println("----------------------------")
          println("[info]"+"catch_time="+System.currentTimeMillis())
          println("[info]::" + "zg_rankNumber=" + zg_rankNumber)
          println("[info]::" + "item=" + item)
          println("[info]::" + "title=" + title)
          println("[info]::" + "product_url=" + product_url)
          println("[info]::" + "image_url=" + image_url)
          println("[info]::" + "price=" + price)
          println("[info]::" + "review=" + review)
          println("[info]::" + "review_rate=" + review_rate)
          println("[info]::" + "follow_seller=" + follow_seller)
          println("[info]::" + "category=" + category)
          println("[info]::" + "category_id=" + category_id)
          println("[info]::" + "catch_time=" + catch_time)
          println("[info]::" + "review_url=" + review_url)
          println("[info]::" + "price_original=" + price_original)
          println("[info]::" + "follow_seller_price=" + follow_seller_price)
          var data = new Amazontop100Model()
          data.setTitle(title)
          data.setZg_rankNumber(zg_rankNumber)
          data.setImage_url(image_url)
          data.setCatch_time(catch_time)
          data.setCategory(category)
          data.setCategory_id(category_id)
          data.setFollow_seller(follow_seller.toInt)
          data.setFollow_seller_price(follow_seller_price.toDouble)
          data.setItem(item)
          data.setPrice(price.toDouble)
          data.setPrice_original(price_original.toDouble)
          data.setProduct_url(product_url)
          data.setReview(review.toInt)
          data.setReview_rate(review_rate)
          data.setReview_url(review_url)
          list = list.::(data)
          // data.set

          /**
           * *
           *
           */
          println("----------------------------")
          // println(basedom.html())
          println("*********end******************")
        }
      }
    }

    // println(baseZGArray)
    //    var tt = doc.getElementsByAttributeValue("class", "pagnLink").get(0).getElementsByTag("a").attr("href")
    //    var rh = tt.substring(tt.indexOf("rh=") + 3, tt.indexOf("&page"))
    //    var qid = tt.substring(tt.indexOf("qid=") + 4, tt.indexOf("&spIA"))
    //    var spIA = tt.substring(tt.lastIndexOf("spIA=") + 5, tt.length())
    //
    //    var data = new Amazontop100Model()
    ////    data.setRh(rh)
    ////    data.setQid(qid)
    ////    data.setSpIA(spIA)
    // lsit = lsit.::(data)
    // list = list.::()
   
    println(list)
    list
  }

}
