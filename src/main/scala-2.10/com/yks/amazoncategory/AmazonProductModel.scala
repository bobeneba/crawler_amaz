package com.yks.amazoncategory
import scala.beans.BeanProperty
import javax.persistence.{ Transient, Lob, Column, Table, Entity, Id, GeneratedValue, GenerationType }
import java.util.Date

@Entity
@Table(name = "amazon_product")
class AmazonProductModel {
   /**
   * 自增ID
   */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  @Column(name = "id", nullable = false, length = 20)
  var id = 0;

  @BeanProperty
  @Column(name = "url", nullable = false)
  var url = "";

  @Transient
  @BeanProperty
  var rh = ""

  @Transient
  @BeanProperty
  var qid = ""

  @Transient
  @BeanProperty
  var spIA = ""
}