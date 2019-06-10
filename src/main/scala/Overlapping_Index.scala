
package main.scala

/* Overlapping_Index
 * 
 * Calculates overlapping index between input to Block_Filtering and output from CNP_ARCS
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
import java.io.PrintWriter
import java.io.InputStream
import java.io.FileInputStream
import java.util.Properties
import java.io._
/*
 * @author Natassa Theodouli
 */

object Overlapping_Index{ 
 
 
/* Calculation of difference between input overlapping index BEFORE the pipeline (Block Filtering | Comparison based preprocessing | CNP_ARCS)
  and output overlapping index */
  
  def main(args: Array[String]) {
     
   val conf = new SparkConf().setAppName("Simple Application")//.setMaster(master)
   val sc = new SparkContext(conf)
   val prop = new Properties();    
   val input = new FileInputStream("config.properties"); 

   try {
        prop.load(input) //load properties file

        // Calculate overlapping index in the initial block collection BEFORE the pipeline (Block Filtering | Comparison based preprocessing | CNP_ARCS)  
        val fileInput=prop.getProperty("spark.myapp.input.step1") // read pipeline input data file 
        val data1 = sc.textFile(fileInput).cache() 

        val numEntities = data1.map{line=>line.split(" ")(1)}.flatMap{line=>line.split(",")}.distinct.reduce{(a,b)=> if(a>b) a else b}.toInt // number of entities in input data equals the maximum entity index 
        System.out.println("ENTITIES "+numEntities)

        val numEntitiesInBlocks=data1.map{line=>line.split(" ")(1)}.map{line=>line.split(",")}.map{line=>line.size}.reduce{(a,b)=>a+b}
        val inputOverlappingIndex=(numEntitiesInBlocks.toFloat/numEntities.toFloat).toDouble //toFloat


        // Calculate overlapping index in the initial block collection AFTER the pipeline (Block Filtering | Comparison based preprocessing | CNP_ARCS) 
        val fileOutput=prop.getProperty("spark.myapp.output.step3") // read pipeline output data file 
        val data2=sc.textFile(fileOutput).cache()                                       
        val tmp1=data2.map{line=>line.replaceFirst(",","#")}.map{line=>line.slice(2,line.length-2)}.map{line=>line.replaceAll("\\),",",")}//.map(_.split("#")).map(line=>(line(0),line(1)))//.flatMapValues{line=>line}

        val numEntitiesInBlocksOutput=tmp1.map(_.split("#")).map{line=>(line(0),line(1))}.flatMapValues{_.split("\\),\\(")}.map{line=>line.toString.replaceAll(",\\(",",")}.map{line=>line.replaceAll("\\)\\)","\\)")}
        .map{line=>line.slice(1,line.length-1)}.map{line => val tokens=line.split(",") 
         (tokens(0),tokens(1),tokens(2))}.map{case(a,b,c)=>(a,b)}.map{line=>line.toString.split(",")}.map{line=>line.size}.reduce{(a,b)=>a+b}
         val outputOverlappingIndex=(numEntitiesInBlocksOutput.toFloat/numEntities.toFloat).toDouble

        // Calculate Difference between inputOverlappingIndex and outputOverlappingIndex as a Percentage
        val dOverlappingIndex = (outputOverlappingIndex-inputOverlappingIndex)/inputOverlappingIndex *100;
        val nums = Seq(numEntities, inputOverlappingIndex, numEntitiesInBlocksOutput, outputOverlappingIndex, dOverlappingIndex) 
        val numsrdd =sc.parallelize(nums)
   } 

  catch {
        case e: IOException => e.printStackTrace()
  } 

  finally {
   sc.stop()
  }
}
}
