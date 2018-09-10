-- set the table format
-- supported are TEXTFILE, RCFILE, ORC, SEQUENCEFILE, PARQUET, AVRO
set tableFormat=TEXTFILE;

-- set additional variables
set temporaryTableSuffix=_temporary;
set hdfsDataPath=/bigbenchv2/data;
set fieldDelimiter=|;

-- set database name
set dbName=BigBenchV2;

-- set table names as variables
set customersTableName=customers;
set itemsTableName=items;
set reviewsTableName=product_reviews;
set web_pagesTableName=web_pages;
set web_salesTableName=web_sales;
set store_salesTableName=store_sales;
set storesTableName=stores;
set web_logsTableName=web_logs;

-- create database
CREATE DATABASE IF NOT EXISTS ${hiveconf:dbName};
USE ${hiveconf:dbName};

-- create table customer
!echo Create temporary table: ${hiveconf:customersTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:customersTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:customersTableName}${hiveconf:temporaryTableSuffix}
  ( c_customer_id             bigint              --not null
  , c_name              	  string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:customersTableName}'
;

!echo Load text data into ${hiveconf:tableFormat} table: ${hiveconf:customersTableName};
CREATE TABLE IF NOT EXISTS ${hiveconf:customersTableName}
STORED AS ${hiveconf:tableFormat}
AS
SELECT * FROM ${hiveconf:customersTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:customersTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:customersTableName}${hiveconf:temporaryTableSuffix};


-- create table items
!echo Create temporary table: ${hiveconf:itemsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:itemsTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:itemsTableName}${hiveconf:temporaryTableSuffix}
  ( i_item_id             	  bigint              --not null
  , i_name              	  string
  , i_category_id     	  	  int
  , i_category_name           string
  , i_price         		  decimal(7,2)
  , i_comp_price			  decimal(7,2)
  , i_class_id				  bigint
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:itemsTableName}'
;

!echo Load text data into ${hiveconf:tableFormat} table: ${hiveconf:itemsTableName};
CREATE TABLE IF NOT EXISTS ${hiveconf:itemsTableName}
STORED AS ${hiveconf:tableFormat}
AS
SELECT * FROM ${hiveconf:itemsTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:itemsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:itemsTableName}${hiveconf:temporaryTableSuffix};


-- create table product_reviews
!echo Create temporary table: ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix}
  ( pr_review_id              bigint              --not null
  , pr_item_id     	  	  	  bigint
  , pr_ts           		  string
  , pr_rating				  int
  , pr_content			      string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:reviewsTableName}'
;

!echo Load text data into ${hiveconf:tableFormat} table: ${hiveconf:reviewsTableName};
CREATE TABLE IF NOT EXISTS ${hiveconf:reviewsTableName}
STORED AS ${hiveconf:tableFormat}
AS
SELECT * FROM ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};

-- create table web_pages
!echo Create temporary table: ${hiveconf:web_pagesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:web_pagesTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:web_pagesTableName}${hiveconf:temporaryTableSuffix}
  ( w_web_page_id              bigint              --not null
  , w_web_page_name            string
  , w_web_page_type     	   string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:web_pagesTableName}'
;

!echo Load text data into ${hiveconf:tableFormat} table: ${hiveconf:web_pagesTableName};
CREATE TABLE IF NOT EXISTS ${hiveconf:web_pagesTableName}
STORED AS ${hiveconf:tableFormat}
AS
SELECT * FROM ${hiveconf:web_pagesTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:web_pagesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:web_pagesTableName}${hiveconf:temporaryTableSuffix};


-- create table web_sales
!echo Create temporary table: ${hiveconf:web_salesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:web_salesTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:web_salesTableName}${hiveconf:temporaryTableSuffix}
  ( ws_transaction_id          bigint              --not null
  , ws_customer_id             bigint
  , ws_item_id     	   		   bigint
  , ws_quantity				   int
  , ws_ts					   string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:web_salesTableName}'
;

!echo Load text data into ${hiveconf:tableFormat} table: ${hiveconf:web_salesTableName};
CREATE TABLE IF NOT EXISTS ${hiveconf:web_salesTableName}
STORED AS ${hiveconf:tableFormat}
AS
SELECT * FROM ${hiveconf:web_salesTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:web_salesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:web_salesTableName}${hiveconf:temporaryTableSuffix};

-- create table store_sales
!echo Create temporary table: ${hiveconf:store_salesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:store_salesTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:store_salesTableName}${hiveconf:temporaryTableSuffix}
  ( ss_transaction_id          bigint              --not null
  , ss_customer_id             bigint
  , ss_store_id				   bigint
  , ss_item_id     	   		   bigint
  , ss_quantity				   int
  , ss_ts					   string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:store_salesTableName}'
;

!echo Load text data into ${hiveconf:tableFormat} table: ${hiveconf:store_salesTableName};
CREATE TABLE IF NOT EXISTS ${hiveconf:store_salesTableName}
STORED AS ${hiveconf:tableFormat}
AS
SELECT * FROM ${hiveconf:store_salesTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:store_salesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:store_salesTableName}${hiveconf:temporaryTableSuffix};

-- create table stores
!echo Create temporary table: ${hiveconf:storesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:storesTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:storesTableName}${hiveconf:temporaryTableSuffix}
  ( s_store_id          bigint              --not null
  , s_store_name        string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:storesTableName}'
;

!echo Load text data into ${hiveconf:tableFormat} table: ${hiveconf:storesTableName};
CREATE TABLE IF NOT EXISTS ${hiveconf:storesTableName}
STORED AS ${hiveconf:tableFormat}
AS
SELECT * FROM ${hiveconf:storesTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:storesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:storesTableName}${hiveconf:temporaryTableSuffix};


-- create table web_logs
!echo Create external table: ${hiveconf:web_logsTableName};
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:web_logsTableName} (line string)
 ROW FORMAT DELIMITED LINES TERMINATED BY '\n' 
STORED AS ${hiveconf:tableFormat} LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:web_logsTableName}'
;