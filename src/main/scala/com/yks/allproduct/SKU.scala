package com.yks.allproduct

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
@Table(name = "shopee_allproduct_out")
class SKU extends AsJson {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  var id: Int = 0
  @Column(name = "from_id")
  @BeanProperty
  var fromId: String = ""
  @Column(name = "item_name")
  @BeanProperty
  var itemName: String = ""
  @Column(name = "product_id")
  @BeanProperty
  var productId: String = ""
  @Column(name = "image")
  @BeanProperty
  var image: String = ""
  @Column(name = "like_num")
  @BeanProperty
  var likeNum: String = ""
  @Column(name = "comment_num")
  @BeanProperty
  var commentNum: String = ""
  @Column(name = "price")
  @BeanProperty
  var price = ""
  @Column(name = "item_url")
  @BeanProperty
  var itemUrl: String = ""
  //@transient
  @Column(name = "item_kind")
  @BeanProperty
  var itemKind: String = ""
  @Column(name = "store_url")
  @BeanProperty
  var storeUrl: String = ""
  @Column(name = "country")
  @BeanProperty
  var country: String = ""
  @Column(name = "crawler_time")
  @BeanProperty
  var crawlerTime: Date = null

}
