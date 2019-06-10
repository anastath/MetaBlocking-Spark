
package main.scala

/* Comparison_Based_Preprocessing.scala
 * 
 * Implements Fig.8 in Eythimiou et.al
 * 
 *  */

import org.apache.spark.sql.SparkSession
import java.io.File
import scala.io.Source._
import org.apache.spark.sql.Dataset
import java.io.PrintWriter
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.IOException
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.util.Properties


/*
 * @author Natassa Theodouli
 */

object Comparison_Based_Preprocessing {
  
   
   def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

    
  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }
  
// function to calculate block cardinalities from block sizes  
//cardinality refers to number of comparisons within blocks in DIRTY-ER
def blockCardins(blockCards: Array[Int]):  Array[Int] = {
  var count=0;
    for (elem <- blockCards) {        
          blockCards(count) = elem*(elem-1)/2 ;   
       count+=1;
        }
    blockCards
    }
 
 
// Implementation of Stage 2: Comparison Based Preprocessing
  
  def main(args: Array[String]) {      
    
   //val file = "E:/spark/input/BF-output.txt" // ei,(<bi,||bi||>)
   //val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
   
    val conf = new SparkConf().setAppName("Simple Application")//.setMaster(master)
    val sc = new SparkContext(conf) 
    //val file = sc.getConf.get("spark.myapp.input") // read input data as a runtime parameter    format input is --> entity_id,(<block_id,block_cardinality>)
    
    val prop = new Properties();    
    val input = new FileInputStream("config.properties"); 
    try {
        prop.load(input) //load properties file
		    val file=prop.getProperty("spark.myapp.input.step2") // read input data file path parameter      
        val dataRDD = sc.textFile(file).cache()
        //val output =  sc.getConf.get("spark.myapp.output") // read output file as a runtime parameter
        val output=prop.getProperty("spark.myapp.output.step2") // read output data file path parameter
        // necessary parsing of the input file
        val dataRDDparsed =  dataRDD.map{ line =>    
          //val index2=line.indexOfSlice(line.takeRight(1)) 
         line.slice(1, line.length-1)}.map{line => line.replaceFirst(",", " ")}    
       
          
       // combine the key (entity_id), with EACH of the values (block_id, block_cardinality) :: entity_id, (block_id, block_cardinality) 
        val tmp=dataRDDparsed.flatMap { case line =>
        val Array(head, other) = line.split(" ")
        other.split('#').map(o => head -> o).map(line=>line.toString)
        }
        
    // tmp.saveAsTextFile("E:/spark/2019/tmp"); // TO-DO:: delete this line !!!!
   
  
  // ##################  sort Bi in ascending order of block ids  
 
    val tmp2=tmp.map{ line =>
    val tokens = line.split(',')
    (tokens(1),(tokens(0),tokens(2)))}.sortByKey()//.map{case(a,(b,c))=>(b,a,c)}    // block_id, (entity_id, block_cardinality), where block_id: sorted in ascending order
     
     // tmp2.saveAsTextFile("E:/spark/2019/tmp2"); // TO-DO:: delete this line !!!!
    
    // necessary parsing of the file to remove unnecessary parentheses
    val tmp3=tmp2.map{line => line.toString}.map{line => line.slice(2, line.length-3)}.map{line => line.replaceFirst("\\(", "")} // block_id, (entity_id, block_cardinality)
    
    
    val tmp4=tmp3.map{ line =>   //   entity_id, block_id, block_cardinality
    val tokens = line.split(',')
    (tokens(1),(tokens(0),tokens(2)))}.map{line => line.toString}.map{line => line.slice(2, line.length-3)}.map{line => line.replaceFirst("\\(", "")}   // entity_id, (block_id, block_cardinality)
    
    
    
    // Filter only blocks with block_cardinality >=1, i.e. at least one comparison should occur in the block
    // entity_id, block_id, block_cardinality, where block_cardinality>=1 
    val threshold=1;
    val tmp5=tmp4.map{line =>
    val tokens = line.split(',')
    (tokens(0),tokens(1),tokens(2).toInt)
    }.filter(x => x._3>=threshold)
    
        
    val tmp8=tmp5.map{case(a,b,c)=>(a,(b,c))}.groupByKey.map(x=>(x._1,x._2.toList.mkString("#")))  // entity_id,(<block_id,block_cardinality>), where bi in ascending order
     
   // tmp8.saveAsTextFile("E:/spark/2019/tmp8"); // TO-DO:: delete this line !!!! 
   
    //  ###################   END sort Bi in ascending order of block ids 
    
    // emit (k, i.Bi) --> emit (block_id,<entity_id,<block_id, block_card>>
    
    val tmp9 = tmp5.map{case(a,b,c)=>(a,b)}  //entity_id, block_id
    
   // tmp9.saveAsTextFile("E:/spark/2019/tmp9"); // TO-DO:: delete this line !!!! 
    
    val tmp10=tmp9.join(tmp8)   // (entity_id,(block_id,<block_id, block_card>))
    
    // tmp10.saveAsTextFile("E:/spark/2019/tmp10"); // TO-DO:: delete this line !!!!
    
    val tmp11 = tmp10.map{case(a,(b,c))=>(b,(a,c))}  // (block_id,(entity_id,<block_id, block_card>))
      
        
    val tmp12=tmp11.groupByKey.map(x=>(x._1,x._2.toList.mkString("||")))
    
    val writer1 = new PrintWriter(output)  //(block_id,<entity_id,<block_id, block_card>>)
    tmp12.collect.foreach(line=>writer1.println(line))
    writer1.close()
    } // end try
    
     catch {
      case e: IOException => e.printStackTrace()
    } 
    finally {
        sc.stop()
    }
  }
}