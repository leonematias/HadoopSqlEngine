===============================================
Hadoop SQL Engine
===============================================
Author: Matias Leone


Installation
============
1) Install Java JDK 1.7 and set JAVA_HOME

2) Download Apache Hadoop latest stable version 2: http://hadoop.apache.org/releases.html#Download
   I'm using version 2.5.1
	
3) Unzip hadoop files and set HADOOP_HOME variable to the extracted dir. 
Example:
export HADOOP_HOME=myHomeDir/hadoop-2.5.1
	
4) Set all the following variables:
export HADOOP_PREFIX=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop



Create Test Data
=================
1) Go to hadoop home
cd $HADOOP_HOME

2) Generate some test data with one million rows in $HADOOP_HOME/input
bin/hadoop jar HadoopSqlEngine.jar sqlEngine.TestDataGenerator "input" 1000000


Examples
========
#Go to hadoop home
cd $HADOOP_HOME

#Simple query
bin/hadoop jar HadoopSqlEngine.jar sqlEngine.SqlEngine -showResults -sql "SELECT user.0, user.1 FROM user"

#Simple query with order by
bin/hadoop jar HadoopSqlEngine.jar sqlEngine.SqlEngine -showResults -sql "SELECT user.0, user.1 FROM user ORDER BY 1"

#Join two tables and use Where
bin/hadoop jar HadoopSqlEngine.jar sqlEngine.SqlEngine -showResults -sql "SELECT product.0, product.1, product.2 FROM sale JOIN product ON product.0 = sale.1 WHERE product.2 > '20' AND product.1 LIKE 'Shadow'"

#Join 3 tables and use Group By
bin/hadoop jar HadoopSqlEngine.jar sqlEngine.SqlEngine -showResults -sql "SELECT user.1, COUNT(product.1) FROM sale JOIN user ON sale.0 = user.0 JOIN product ON product.0 = sale.1 GROUP BY user.1 ORDER BY 0 ASC"

#Join 4 tables and apply complex Where filter
bin/hadoop jar HadoopSqlEngine.jar sqlEngine.SqlEngine -showResults -sql "SELECT user.1, product.1, store.1 FROM sale JOIN user ON sale.0 = user.0 JOIN product ON product.0 = sale.1 JOIN store ON store.0 = sale.2 WHERE user.1 LIKE 'Robert' AND (store.1 = 'Houston' OR product.2 > '40')"


















