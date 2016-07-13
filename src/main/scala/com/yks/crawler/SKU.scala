package com.yks.crawler

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
@Table(name = "shopee_kind_out")
class SKU extends AsJson {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  var id: Int = 0
  @Column(name = "from_id")
  @BeanProperty
  var fromId: String = ""
  @Column(name = "first_kind")
  @BeanProperty
  var firstKind = ""
  @Column(name = "first_kind_url")
  @BeanProperty
  var firstKindUrl = ""
  @Column(name = "first_kind_id")
  @BeanProperty
  var firstKindId = ""
  @Column(name = "two_kind")
  @BeanProperty
  var twoKind: String = ""
  @Column(name = "two_kind_url")
  @BeanProperty
  var twoKindUrl: String = ""
  @Column(name = "two_kind_id")
  @BeanProperty
  var twoKindId: String = ""
  @transient
  @Column(name = "json_url")
  @BeanProperty
  var jsonUrl: String = ""
  @Column(name = "current_url")
  @BeanProperty
  var currentUrl = ""
  @Column(name = "country")
  @BeanProperty
  var country: String = ""
  @Column(name = "all_item_num")
  @BeanProperty
  var allItemNum: String = ""
  @Column(name = "crawler_time")
  @BeanProperty
  var crawlerTime: Date = null

}       
