
package main.scala


import org.apache.spark.sql.SparkSession
import java.io.File
import scala.io.Source._
import org.apache.spark.sql.Dataset
import java.io.PrintWriter
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/*
 * @author Natassa Theodouli
 */


object Block_Filtering {
  
     
// function to calculate block cardinalities from block sizes  
//cardinality refers to number of comparisons within blocks in DIRTY-ER
def blockCardins(blockCards: Array[Int]):  Array[Int] = {
  var count=0;
    for (elem <- blockCards) {        
          blockCards(count) = elem*(elem-1)/2 ;   // DIRTY-ER
          count+=1;
        }
    blockCards
    }
 
  
//Implementation of Stage 1: Block Filtering 
  
  def main(args: Array[String]) {          
    
    val conf = new SparkConf().setAppName("Simple Application")//.setMaster(master)
    val sc = new SparkContext(conf)  
	  
    val prop = new Properties();    
    val input = new FileInputStream("config.properties");
	  
    try {    
    // load a properties file
    prop.load(input)//.toString()
    val logFile=prop.getProperty("spark.myapp.input.step1") // read input data file path parameter
    val logData = sc.textFile(logFile).cache()    
    val output=prop.getProperty("spark.myapp.output.step1") // read output data file path parameter    
    val filtering = prop.getProperty("spark.myapp.filtering").toBoolean // read filtering option parameter
    
    // ::::::::::::::::::::::::::::::::::::::   1. Split blocks and entities

     val blockIDRDD= logData.map(line => line.split(" ")(0)) // blockIDs
     val entitiesRDD= logData.map(line => line.split(" ")(1)) // entities
       
  // Get the distinct entities 
     val entitiesDistinct=entitiesRDD.flatMap(_.split(",")).distinct.collect.toSeq
     
     val entitiesDistinctRDD=sc.parallelize(entitiesDistinct)
 
 // Sort the distinct entities 
     val entitiesDistinct_RDD=entitiesDistinctRDD.zipWithIndex.sortByKey().map{case(a,b)=>a}
  
 
   // :::::::::::::::::::::::::::::::::::::::   2. Get block sizes
    
     val blocksizesRDD=entitiesRDD.map(_.split(",")).map(_.size) //.count()//.foreach(println(_)) 


      
 // ::::::::::::::::::::::::::::::::::::::  3. Get block cardinalities
    
   val blockCardinalities= blockCardins(blocksizesRDD.collect()).toSeq//.foreach(blockCardins)
   val blockCardinalitiesRDD=sc.parallelize(blockCardinalities)     
   val blockID_cards=blockIDRDD.zip(blockCardinalitiesRDD) //   (blockId, blockCardinality)
       
  
  // :::::::::::::::::::::::::::::::::::  4. Sort block cardinalities
   
  val cards_blockIDs= blockID_cards.map{case (a,b)=>(b,a)}.sortByKey() // (blockCardinality,blockID) sorted by cardinalities, ascending 
  val card_blockIDs2=cards_blockIDs.map{case (a,b)=>(a,b.toInt)}
  
   
  
  val test = logData.map(_.split(" ")).map(x=>(x(0),x(1))).mapValues(_.split(",")).flatMapValues(line=>line) // (blockid, entityid)
  val entityid_blockid=test.map{case(a,b)=>(b.toInt,a.toInt)} // (entityid, blockid)
    
     
  
 // :::::::::::::::::::::::::::::::::::::::::     5. Get top N blocks (by ascending cardinality for each entity)
  
  val threshold=3;
  val entity_blocks2=entityid_blockid.keyBy(t => t._2)
  val card_blockIDs3=card_blockIDs2.keyBy(t => t._2)
  val tmpJoined=entity_blocks2.join(card_blockIDs3)
 
  
  val entity_block_card=tmpJoined.map{case(a,((b,c),(d,e)))=>(b,a,d)}  //(entity_id, block_id, cardinality)
  
  
 //  Based on a runtime parameter, filtering for block cardinalities could be on-off
 val entity_block_card_Filtered = entity_block_card.filter(x => x._3<threshold) //(entity_id, <block_id, block_cardinality>) ONLY for remaining blocks after filtering

    
 if (filtering){ 
  val writer1 = new PrintWriter(output) 
  entity_block_card_Filtered.map{case(a,b,c)=>(a,(b,c))}.groupByKey.map(x=>(x._1,x._2.toList.mkString("#"))).collect.foreach(line=>writer1.println(line)) // convert to list and mkstring to remove CompactBuffer from printed output
  writer1.close()
 }
else {
  val writer1 = new PrintWriter(output)  
  entity_block_card.map{case(a,b,c)=>(a,(b,c))}.groupByKey.map(x=>(x._1,x._2.toList.mkString("#"))).collect.foreach(line=>writer1.println(line)) // convert to list and mkstring to remove CompactBuffer from printed output
  writer1.close()
}     
    
    } // end try
 catch {
      case e: IOException => e.printStackTrace()
 } 
 
finally {
 sc.stop()
}
}
}
