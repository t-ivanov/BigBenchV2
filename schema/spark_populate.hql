use bigbenchv2; 
CREATE TABLE spark_logs 
USING org.apache.spark.sql.json 
OPTIONS (
  path "/bigbenchv2/data/web_logs/clicks.json"
)
;
