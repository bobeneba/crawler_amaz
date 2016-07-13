package com.yks.detailCrawler

import scala.beans.BeanProperty
import javax.persistence.Entity
import javax.persistence.Table
import javax.persistence.Column
import sun.security.util.Length
import spider.man.parse.AsJson
import javax.persistence.Lob
import javax.persistence.Id
import javax.persistence.GeneratedValue
import javax.persistence.GenerationType
import java.util.Date

@Entity
@Table(name = "shopee_storedetail_out")
class SKU extends AsJson {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  var id: Int = 0
  @Column(name = "from_id")
  @BeanProperty
  var fromId: String = ""
  @transient
  @Column(name = "json_url")
  @BeanProperty
  var jsonUrl: String = ""
  @Column(name = "store_name")
  @BeanProperty
  var storeName: String = ""
  @Column(name = "store_url")
  @BeanProperty
  var storeUrl: String = ""
  @Column(name = "country")
  @BeanProperty
  var country: String = ""
  @Column(name = "store_fensi")
  @BeanProperty
  var storeFensi: String = ""
  @Column(name = "store_guanzhu")
  @BeanProperty
  var storeGuanzhu: String = ""
  @Column(name = "store_products_num")
  @BeanProperty
  var storeProducts: String = ""
  @Column(name = "comment_num")
  @BeanProperty
  var commentNum: String = ""
  @Column(name = "comment_star")
  @BeanProperty
  var commentStar: String = ""
  @Column(name = "store_id")
  @BeanProperty
  var storeId: String = ""
  @Column(name = "crawler_time")
  @BeanProperty
  var crawlerTime: Date = null

}
