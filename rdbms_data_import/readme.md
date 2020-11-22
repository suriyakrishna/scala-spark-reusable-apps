## ***RDBMS Data Import***

*Spark Scala Application to import data from RDBMS tables to HDFS.*

#### ***Applicaton Options***


|Short Option|Long Option|Description|Has Argument|Is Required|
|------|------|------|:------:|:------:|
|-a|--append|Flag to set append|||
|-c|--columns|List of columns to import separated by comma|Yes||
|-d|--driver|JDBC/ODBC Driver Path|Yes|Yes|
|-f|--format|Output file format. By default parquet. Can be CSV/PARQUET/JSON|Yes||
|-inc|--incremental|Flag to set incremental import|||
|-incColumn|--incremental-column|Column to be used for incremental import|Yes||
|-incEnd|--incremental-end-time|End timestamp Value. To be used when incremental-type is 'TIMESTAMP'|Yes||
|-incFormat|--incremental-time-format|Timestamp format representation in string. To be used when incremental-type is 'TIMESTAMP'|Yes||
|-incId|--incremental-id|Value of Start ID for incremental import. To be used when incremental-type is 'ID'|Yes||
|-incStart|--incremental-start-time|Start timestamp value. To be used when incremental-type is 'TIMESTAMP'|Yes||
|-incType|--incremental-type |Type of incremental import ID/TIMESTAMP|Yes||
|-n|--num-partitions|Number of partitions for parallelism. By default 4|Yes||
|-o|--overwrite|Flag to set overwrite|||
|-pass|--password|JDBC/ODBC password|Yes|Yes|
|-s|--split-by|splitBy Column for parallelism|Yes|Yes|
|-t|--table|JDBC/ODBC table name|Yes|Yes|
|-target|--target-dir|Target directory to import data|Yes|Yes|
|-uName|--username|JDBC/ODBC username|Yes|Yes|
|-url|--url|JDBC/ODBC url|Yes|Yes|


### ***Log4j Properties File***

```properties
# Define Appender for rootLogger
log4j.rootLogger=ERROR, Y
log4j.appender.Y=org.apache.log4j.ConsoleAppender
log4j.appender.Y.target=System.err
log4j.appender.Y.layout=org.apache.log4j.PatternLayout
log4j.appender.Y.layout.conversionPattern=%d{MM-dd-yyyy HH:mm:ss.SSS} %c{1} -%5p - %m%n

# Application Logger Configurations
log4j.logger.com.github.suriyakrishna=INFO, app
log4j.appender.app=org.apache.log4j.ConsoleAppender
log4j.appender.app.layout=org.apache.log4j.PatternLayout
log4j.appender.app.layout.conversionPattern=%d{MM-dd-yyyy HH:mm:ss.SSS} %10c{1} -%5p - %m%n
log4j.additivity.com.github.suriyakrishna=false
```

### ***spark-submit Invocation/Usage***
```bash
# ENVIRONMENTAL VARIABLES
APPLICATION_DIRECTORY="/home/hadoop/spark_jdbc_import"
JDBC_URL="jdbc:mysql://localhost:3306"
JDBC_USERNAME="MY_USERNAME"
JDBC_PASSWORD="MY_PASSWORD"
JDBC_DRIVER="com.mysql.jdbc.Driver"

# IMPORT ALL DATA FROM SQL TABLE
spark-submit --master yarn \
--deploy-mode cluster \
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties \
--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties \
--class com.github.suriyakrishna.Import \
${APPLICATION_DIRECTORY}/rdbms_data_import_1.0.0.jar \
--driver ${JDBC_DRIVER} \
--url ${JDBC_URL} \
--username ${JDBC_USERNAME} \
--password ${JDBC_PASSWORD} \
--table "employees.salaries" \
--split-by emp_no \
--target-dir "/user/hadoop/spark_jdbc_import_out/employees/salaries" \
--format "json" \
--overwrite

# IMPORT USING SQL QUERY
spark-submit --master yarn \
--deploy-mode cluster \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties" \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties" \
--class com.github.suriyakrishna.Import \
${APPLICATION_DIRECTORY}/rdbms_data_import_1.0.0.jar \
--driver ${JDBC_DRIVER} \
--url ${JDBC_URL} \
--username ${JDBC_USERNAME} \
--password ${JDBC_PASSWORD} \
--table "(SELECT * FROM sakila.rental WHERE rental_id<=200) rental" \
--split-by rental_id \
--num-partitions 1 \
--target-dir "/user/hadoop/spark_jdbc_import_out/sakila/rental" \
--format "parquet" \
--overwrite

# INCREMENTAL IMPORT USING PRIMARY KEY COLUMN
spark-submit --master yarn \
--deploy-mode cluster \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties" \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties" \
--class com.github.suriyakrishna.Import \
${APPLICATION_DIRECTORY}/rdbms_data_import_1.0.0.jar \
--driver ${JDBC_DRIVER} \
--url ${JDBC_URL} \
--username ${JDBC_USERNAME} \
--password ${JDBC_PASSWORD} \
--table sakila.rental \
--split-by rental_id \
--num-partitions 1 \
--target-dir "/user/hadoop/spark_jdbc_import_out/sakila/rental" \
--format "parquet" \
--incremental \
--incremental-type ID \
--incremental-column rental_id \
--incremental-id 200 \
--append

# INCREMENTAL IMPORT USING LAST UPDATE TIMESTAMP COLUMN
spark-submit --master yarn \
--deploy-mode cluster \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties" \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://${APPLICATION_DIRECTORY}/log4j.properties" \
--class com.github.suriyakrishna.Import \
${APPLICATION_DIRECTORY}/rdbms_data_import_1.0.0.jar \
--driver ${JDBC_DRIVER} \
--url ${JDBC_URL} \
--username ${JDBC_USERNAME} \
--password ${JDBC_PASSWORD} \
--table sakila.payment \
--split-by payment_id \
--target-dir "/user/hadoop/spark_jdbc_import_out/sakila/payment" \
--format "parquet" \
--incremental \
--incremental-type TIMESTAMP \
--incremental-column last_update \
--incremental-time-format "yyyy-MM-dd HH:mm:ss" \
--incremental-start-time "1900-01-01 00:00:00" \
--incremental-end-time "2006-02-15 22:19:54" \
--append
```   