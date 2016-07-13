create table amazon_category_urlfactory (
  id                        integer(20) auto_increment not null,
  zg_rankNumber             integer not null,
  item                      varchar(255) not null,
  title                     varchar(255) not null,
  product_url               varchar(255) not null,
  image_url                 varchar(255) not null,
  price                     double not null,
  review                    integer,
  review_rate               varchar(255) not null,
  follow_seller             integer,
  category                  varchar(255) not null,
  category_id               varchar(255) not null,
  catch_time                bigint not null,
  review_url                varchar(255) not null,
  price_original            double not null,
  follow_seller_price       double not null,
  constraint pk_amazon_category_urlfactory primary key (id))
;



