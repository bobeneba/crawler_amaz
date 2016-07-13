package com.yks.amazontop100

import scala.beans.BeanProperty
import javax.persistence.{ Transient, Lob, Column, Table, Entity, Id, GeneratedValue, GenerationType }
import java.util.Date

@Entity
@Table(name = "amazon_category_urlfactory")
class Amazontop100Model {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @BeanProperty
  @Column(name = "id", nullable = false, length = 20)
  var id = 0;

  @BeanProperty
  @Column(name = "zg_rankNumber", nullable = false)
  var zg_rankNumber = 0;

  @BeanProperty
  @Column(name = "item", nullable = false)
  var item = "";

  @BeanProperty
  @Column(name = "title", nullable = false)
  var title = "";

  @BeanProperty
  @Column(name = "product_url", nullable = false)
  var product_url = "";

  @BeanProperty
  @Column(name = "image_url", nullable = false)
  var image_url = "";

  @BeanProperty
  @Column(name = "price", nullable = false)
  var price = 0.0;

  @BeanProperty
  @Column(name = "review", nullable = true)
  var review = 0;
  @BeanProperty
  @Column(name = "review_rate", nullable = false)
  var review_rate = "";

  @BeanProperty
  @Column(name = "follow_seller", nullable = true)
  var follow_seller = 0;

  @BeanProperty
  @Column(name = "category", nullable = false)
  var category = "";

  @BeanProperty
  @Column(name = "category_id", nullable = false)
  var category_id = "";

  @BeanProperty
  @Column(name = "catch_time", nullable = false)
  var catch_time: Long = _;

  @BeanProperty
  @Column(name = "review_url", nullable = false)
  var review_url = "";
  
  @BeanProperty
  @Column(name = "price_original", nullable = false)
  var price_original = 0.00;
  
  @BeanProperty
  @Column(name = "follow_seller_price", nullable = false)
  var follow_seller_price = 0.00;
}