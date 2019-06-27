# MetaBlocking-Spark
This source code implements the following parts from this work

Efthymiou, Vasilis, et al. "Parallel meta-blocking for scaling entity resolution over big heterogeneous data." Information Systems 65 (2017): 137-157.

1. Block filtering
2. Comparison-based preprocessing
3. Cardinality Node Pruning (CNP) using the Aggregate Reciprocal Comparisons Scheme (ARCS) weighting scheme

It also introduces a source code which calculates the difference between (i) the input overlapping index BEFORE the pipeline (Block Filtering | Comparison based preprocessing | CNP_ARCS), and (ii) the output overlapping index, where overlapping index is defined as the number of entities in all the blocks of the Block collection divided by the number of entities in the initial dataset.

The code is written in Scala 2.11 using Apache Spark 2.3 (https://spark.apache.org/releases/spark-release-2-3-0.html).  

## Instructions for execution, execute locally (1 core)

Run the four classes in *THIS* order: 
1) Block_Filtering
2) Comparison_Based_Preprocessing
3) CNP_ARCS
4) Overlapping_Index

To launch the jar locally (on localhost, with 1 core), using spark-submit, run the following commands in cmd.

1.Step1:: Block Filtering
spark-submit --class "main.scala.Block_Filtering" --master local [MetaBlocking-Spark-home]\target\SimpleApp-0.0.1-SNAPSHOT.jar                                                                                                                         

2.Step2:: Comparison-based preprocessing
spark-submit --class "main.scala.Comparison_Based_Preprocessing" --master local [MetaBlocking-Spark-home]\target\SimpleApp-0.0.1-SNAPSHOT.jar                                                                                                                         

3.Step3:: Cardinality Node pruning (CNP - ARCS)
spark-submit --class "main.scala.CNP_ARCS" --master local [MetaBlocking-Spark-home]\target\SimpleApp-0.0.1-SNAPSHOT.jar                                                                                                                         

4.Calculate overlapping index 
spark-submit --class "main.scala.Overlapping_Index" --master local [MetaBlocking-Spark-home]\target\SimpleApp-0.0.1-SNAPSHOT.jar 
