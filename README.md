# MetaBlocking-Spark
This source code implements the following parts 
from this work XXXX

It is written in Scala XX using Apache Spark XX 

## Instructions for execution, execute locally (1 core)

Run the four classes in *THIS* order: 
1) Block_Filtering
2) Comparison_Based_Preprocessing
3) CNP_ARCS
4) Overlapping_Index

To launch the jars using spark-submit, run the following commands in cmd.

####### Step1:: Block Filtering
spark-submit --class "main.scala.Block_Filtering" --master local <MetaBlocking-Spark-home>\target\SimpleApp-0.0.1-SNAPSHOT.jar                                                                                                                         

####### Step2:: Comparison-based preprocessing
spark-submit --class "main.scala.Comparison_Based_Preprocessing" --master local <MetaBlocking-Spark-home>\target\SimpleApp-0.0.1-SNAPSHOT.jar                                                                                                                         

####### Step3:: Cardinality Node pruning (CNP - ARCS)
spark-submit --class "main.scala.CNP_ARCS" --master local <MetaBlocking-Spark-home>\target\SimpleApp-0.0.1-SNAPSHOT.jar                                                                                                                         

########## Calculate overlapping index 
spark-submit --class "main.scala.Overlapping_Index" --master local <MetaBlocking-Spark-home>\target\SimpleApp-0.0.1-SNAPSHOT.jar 
