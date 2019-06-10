How to submit the jar locally

E:/spark/bin/spark-submit --class "main.scala.CNP_ARCS" --master local C:\Users\teoto\workspace\MetaBlocking-Spark\target\SimpleApp-0.0.1-SNAPSHOT.jar

E:/spark/bin/spark-submit --class "main.scala.Comparison_Based_Preprocessing" --master local C:\Users\teoto\workspace\MetaBlocking-Spark\target\SimpleApp-0.0.1-SNAPSHOT.jar 


E:/spark/bin/spark-submit --class "main.scala.Block_Filtering" --master local C:\Users\teoto\workspace\MetaBlocking-Spark\target\SimpleApp-0.0.1-SNAPSHOT.jar --files C:\Users\teoto\workspace\MetaBlocking-Spark\input.txt
--conf "spark.executor.extraClassPath=./
E:/spark/myInput/input.txt E:/spark/myInput/input.txt

E:/spark/bin/spark-submit --properties-file mypropsfile.conf --class "main.scala.Block_Filtering" --master local C:\Users\teoto\workspace\MetaBlocking-Spark\target\SimpleApp-0.0.1-SNAPSHOT.jar

## How to add config.properties file to the classpath so as to write file paths (NOT to have hardcoded file paths)
E:/spark/bin/spark-submit --jars $SPARK_HOME/lib/protobuf-java-2.5.0.jar --class MainClass /home/myapplication/my-application-fat.jar -appconf /home/myapplication/application-prop.properties -conf /home/myapplication/application-configuration.conf --files /home/myapplication/external-prop.properties --conf "spark.executor.extraClassPath=./"


E:/spark/bin/spark-submit --jars $SPARK_HOME/lib/protobuf-java-2.5.0.jar --class MainClass /home/myapplication/my-application-fat.jar -appconf /home/myapplication/application-prop.properties -conf /home/myapplication/application-configuration.conf --files /home/myapplication/external-prop.properties --properties-file  config.properties

--properties-file  mypropsfile.conf


MISC notes
=========================
1) The project needs scala 2.11 to execute. Right click to the Project > Properties > Scala compiler > Use Project Settings > scala 2.11 > Rebuild project

2) In Spark 2.3 SparkContext has been replaced by SparkSession

3) Input file doesn't exist when loading from local file system. Thus, I neede to setup HDFS, following this guide: https://opensourceforu.com/2015/03/getting-started-with-hadoop-on-windows/ 



Installation Instructions
=====================================
To launch the jar using spark-submit, give the following command in cmd. Use --conf option to pass the path to the input file at runtime (to avoid any hard-coded paths within the code). 

spark-submit --class "main.scala.Block_Filtering" --master local --conf "spark.myapp.input=E:\spark\myInput\input.txt" --conf "spark.myapp.output=E:\spark\MYoutput\logData" C:\Users\teoto\workspace\MetaBlocking-Spark\target\SimpleApp-0.0.1-SNAPSHOT.jar                                                                                                                         