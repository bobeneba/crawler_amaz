package com.yks.allStore

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
@Table(name = "shopee_allstore_out")
class SKU extends AsJson {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  var id: Int = 0
  @Column(name = "store_id")
  @BeanProperty
  var storeId: String = ""
  @Column(name = "from_id")
  @BeanProperty
  var fromId: String = ""
  @Column(name = "store_url")
  @BeanProperty
  var storeUrl = ""
  @Column(name = "kind")
  @BeanProperty
  var kind = ""
  @Column(name = "country")
  @BeanProperty
  var country: String = ""
  //@transient
  @Column(name = "crawler_time")
  @BeanProperty
  var crawlerTime: Date = null
  @Column(name = "begin_time")
  @BeanProperty
  var beginTime: Date = null
  @Column(name = "end_time")
  @BeanProperty
  var EndTime: Date = null
  @Column(name = "act_status")
  @BeanProperty
  var actStatus: String = ""

}
