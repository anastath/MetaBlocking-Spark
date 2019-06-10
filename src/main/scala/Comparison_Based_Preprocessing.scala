
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
    val conf = new SparkConf().setAppName("Simple Application")//.setMaster(master)
    val sc = new SparkContext(conf)      
    val prop = new Properties();    
    val input = new FileInputStream("config.properties"); 
    try {
        prop.load(input) //load properties file
        val file=prop.getProperty("spark.myapp.input.step2") // read input data file path parameter  // The format is: ei,(<bi,||bi||>)    
        val dataRDD = sc.textFile(file).cache()        
        val output=prop.getProperty("spark.myapp.output.step2") // read output data file path parameter
        // necessary parsing of the input file
        val dataRDDparsed =  dataRDD.map{ line =>   
        line.slice(1, line.length-1)}.map{line => line.replaceFirst(",", " ")}    
       
          
       // combine the key (entity_id), with EACH of the values (block_id, block_cardinality) :: entity_id, (block_id, block_cardinality) 
        val tmp=dataRDDparsed.flatMap { case line =>
        val Array(head, other) = line.split(" ")
        other.split('#').map(o => head -> o).map(line=>line.toString)
        }   
   
  // ##################  sort Bi in ascending order of block ids  
 
    val tmp2=tmp.map{ line =>
    val tokens = line.split(',')
    (tokens(1),(tokens(0),tokens(2)))}.sortByKey()//.map{case(a,(b,c))=>(b,a,c)}    // block_id, (entity_id, block_cardinality), where block_id: sorted in ascending order
        
    
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
       
   
    //  ###################   END sort Bi in ascending order of block ids 
    
    // emit (k, i.Bi) --> emit (block_id,<entity_id,<block_id, block_card>>
    
    val tmp9 = tmp5.map{case(a,b,c)=>(a,b)}  //entity_id, block_id      
    val tmp10=tmp9.join(tmp8)   // (entity_id,(block_id,<block_id, block_card>))     
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
