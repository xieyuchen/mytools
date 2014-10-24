dfsusage
========

scala jetty hdfs usage tool

url has the same structure with hdfs directory
port: 8989

make:
mvn clean compile assembly:single

run:
$HADOOP_HOME/bin/hadoop jar dfsusage-1.0-SNAPSHOT.jar com.meituan.mthdp.dfsusage.DfsUsage

