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
import java.util.Formatter.DateTime

@Entity
@Table(name = "shopee_allstore_in")
class AllStoreIn extends AsJson {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  var id: Int = 0
  @Column(name = "first_kind")
  @BeanProperty
  var firstKind: String = ""
  @Column(name = "two_kind")
  @BeanProperty
  var twoKind: String = ""
  @Column(name = "two_kind_url")
  @BeanProperty
  var twoKindUrl: String = ""
  @Column(name = "two_kind_id")
  @BeanProperty
  var twoKindId: String = ""
  @Column(name = "all_item_num")
  @BeanProperty
  var allItemNum: String = ""
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
