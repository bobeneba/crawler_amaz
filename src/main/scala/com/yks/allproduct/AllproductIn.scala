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
@Table(name = "shopee_allproduct_in")
class AllproductIn extends AsJson {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  var id: Int = 0
  @Column(name = "store_url")
  @BeanProperty
  var storeUrl: String = ""
  @Column(name = "store_id")
  @BeanProperty
  var storeId: String = ""
  @Column(name = "store_products_num")
  @BeanProperty
  var storeProductsNum: String = ""
  @Column(name = "country")
  @BeanProperty
  var country: String = ""
  @Column(name = "act_status")
  @BeanProperty
  var actStatus: String = ""
  @Column(name = "begin_time")
  @BeanProperty
  var beginTime: Date = null
  @Column(name = "end_time")
  @BeanProperty
  var endTime: Date = null

}
