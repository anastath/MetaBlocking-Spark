
package main.scala

/* CNP_ARCS
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
import java.io.PrintWriter
import java.io.InputStream
import java.io.FileInputStream
import java.util.Properties

/*
 * @author Natassa Theodouli
 */

object CNP_ARCS{
   
  
 def block_comparisons(entities: List[String]) : List[(String,String)] = {
  val entities1=entities   
  val comparisons= for {
    x <- entities
    y <- entities1    
    if ( x!=y && x<y)       
   } yield (x,y)    
   comparisons          
  }
 
 def LeCoBIfunc(BlockList1: List[Int],BlockList2: List[Int]) : List[Int] = {     
    val list= BlockList1 intersect BlockList2  
    list          
  }
 

 def arcs(blokcCards: String): Double = {
   val blockCardsArray = blokcCards.split(",")
   var sum=0
   for (elem <- blockCardsArray) { 
    val elem2 = elem.toInt
   sum = sum+1/elem2    
   }
   sum
 }
 
 
 def getEntity(line: String): Array[String] = {  
   var arr = line.split("\\|\\|")
   val len=arr.length
   var z = new Array[String](len)
   var count=0;
   for (elem <- arr){  
      z(count)=elem(1).toString
      count+=1;   
   }
   
    for (elem <- z) {    
     println("elem ######################"+elem)     
        }  
    z
    }
 
 
// Implementation of Stage 3: Cardinality_Node_pruning (Comparison based strategy, with ARCS weighting scheme)
  
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Simple Application")//.setMaster(master)
      val sc = new SparkContext(conf) 
      val prop = new Properties();    
      val input = new FileInputStream("config.properties"); 

     try  {
      prop.load(input) //load properties file
      val file=prop.getProperty("spark.myapp.input.step3") // read input data file path parameter 
      val data = sc.textFile(file).cache()   
      val output = prop.getProperty("spark.myapp.output.step3") // read output data file path parameter
      val bi=data.map{line=>line.replaceFirst(",", " ")}.map{line => line.split(" ")}.map(x=>(x(0),x(1))).keys.map{line=>line.slice(1,line.length)} //block_id
      val Ei=data.map{line=>line.replaceFirst(",", " ")}.map{line => line.split(" ")}.map(x=>(x(0),x(1))).values.map{line=>getEntity(line)}.map{line=>line.toList.mkString(",")} //<entity_id>
      val bi_Ei=bi.zip(Ei) // bi, <ei>
      val  ei_Bi = bi_Ei.map{line=>line.toString}.map{line=>line.slice(1,line.length-1)}.map{line=>line.replaceFirst(",", " ")}.map{line =>
          val tokens = line.split(" ")
          (tokens(0),tokens(1))}.mapValues{line=>line.split(",")}.flatMapValues(line=>line).map{case(a,b)=>(b,a)}.groupByKey.map(x=>(x._1,x._2.toList.mkString(","))).map{line=>line.toString}//.collect

      //######################## Step1::  Calculating pair-wise COMPARISONS of entities for each Block

      val blockComparisons = Ei.map(_.split(",")).map(line=>(block_comparisons(line.toList)))//.map {case Seq(a,b) => (a,b)}//.map { set => set.toList(0) -> set.toList(1) }//.values
      val blockcomparisonsStr=blockComparisons.map(_.mkString(",")) // convert RDD[List] to RDD[string]
      val blockCombs = bi.zip(blockcomparisonsStr) // bi, <list of pair-wise comparisons between entities>

      //####################### Step2:: Check if a comparison between two entities is NON REDUNDANT, i.e. find the LeCoBI (Least Common Block Index) for the two entities
      // ONLY in the LeCoBI block, should the two entities be compared 
      val ei_Bi2=ei_Bi.map{line =>line.slice(1, line.length-1)}.map{line => line.replaceFirst(",", "#")}
      val combinations = ei_Bi2.cartesian(ei_Bi2).filter{ case (a,b) => (a < b && a!=b) }

      val tmp = combinations.map(x=>x._1+"#"+x._2).map{line =>line.slice(0, line.length)}.map{line => line.replaceAll(" ", ",")} 

      val tmp2 = tmp.map{line =>
      val tokens = line.split('#')
      ((tokens(0),tokens(2)),tokens(1),tokens(3))
      }

      val LeCoBI = tmp2.map{case((a,b),c,d)=>(c,d)}.map(x=>(x._1.split(',').toList.map((s: String) => s.toInt).sorted, x._2.split(',').toList.map((s: String) => s.toInt).sorted))
      val entitiesCombins=tmp2.map{case((a,b),c,d)=>(a,b)} 
      val LeCoBIList = LeCoBI.map(line =>(LeCoBIfunc(line._1,line._2))).map(_.take(1)) // the first block_id is the minimum (common) block_id, since the lists were sorted by block_id, in ascending order
      val entitiesCombsLeCoBI = entitiesCombins.zip(LeCoBIList)
      val entitiesCombsLeCoBI2 = entitiesCombsLeCoBI.filter(x => x._2.nonEmpty)
      val entitiesCombsLeCoBI3 = entitiesCombsLeCoBI2.map(line => ("(" +line._1._1 + "," + line._1._2 +")", line._2.mkString))//.map{case(a,b)=>((a),b)}
      val blockCombsforjoin=blockCombs.map{case(a,b)=>(b,a)} //.keyBy(t => t._1)
      val tmp4 = blockCombsforjoin.join(entitiesCombsLeCoBI3).map{case(a,(b,c))=>(a,b,c)}.filter(x=>(x._2==x._3)).map{case(a,b,c)=>(a,b)} // WHERE DO WE NEED THIS ???? Isn't entitiesCombsLeCoBI3 enough ???

      //########################### Step3:: Find blockIDs and their block cardinalities
      val file4=prop.getProperty("spark.myapp.output.step1") // (entity_id, block_id, block_cardinality) :: filtered by block_cardinality threshold as per Block Filtering technique
      val data2 = sc.textFile(file4).cache().map(line=>line.toString.replaceFirst(","," "))
      .map{line =>
      val tokens = line.split(' ')
      (tokens(0),tokens(1))
      }.flatMapValues(_.split("#")).map{line=>line.toString}.map(line=>line.slice(1,line.length-1).replaceAll(",\\(",",").replaceAll("\\)\\)","\\)")) 
      
      val tmp5 = data2.map{line =>
      val tokens = line.split(',')
      (tokens(1),tokens(2))
      }.distinct 
      val blockID_blockCard = tmp5.map(line => (line._1 + "," + line._2)).map{line => line.dropRight(1)} // block_id, block_cardinality


      // Step4:: Find common blocks of all entities combinations, need for the ARCS weight calculation, where ARCS(ei,ej,Block collection) = sum(1/block_cardinality) for all common blocks of ei, ej
      val commonBlocksList = LeCoBI.map(line =>(LeCoBIfunc(line._1,line._2))) // same as LeCoBIList, line 187 !!!! WHY is this needed ? To compute the ARCS weight ???
      val entitiesCombs_CommonBlocks = entitiesCombins.zip(commonBlocksList).filter(x => x._2.nonEmpty).map(line => ("(" +line._1._1 + "," + line._1._2 +")", line._2.mkString(",")))


      // All pairs of entities who have common blocks, will have also a least common block, i.e. they will be involved in a non-redundant comparison and so, an ARCS weight should be calculated for this pair
      val tmp7 = entitiesCombs_CommonBlocks.flatMapValues(_.split(",")).map{case(a,b)=>(b,a)}.map(line => (line._1 + "," + line._2)).map{line =>line.slice(0, line.length)}
      val tmp8=tmp7.map{line=>line.replaceFirst(",", "#")}.map{line =>
      val tokens = line.split('#')
      (tokens(0),tokens(1))
      }


      // Step5:: Calculate ARCS weights for all the pairwise combinations of the entities
      val tmp9=blockID_blockCard.map{line =>
      val tokens = line.split(',')
      (tokens(0),tokens(1))
      }

      val tmp10 = tmp8.join(tmp9) // (blockID, entitiesCombs) JOIN (blockID, blockCard) => blockID, (entitiesComb, blockCard)
      val tmp11 = tmp10.map{case(a, (b,c)) => (b,c)}.groupByKey().map(x=>(x._1,x._2.toList.mkString(","))) // <entitiesCombs>, <blockCard> a.k.a (ei.ej), <blockCards>
      val tmp12 = tmp11.map(line=>(line._1,arcs(line._2)))

      val tmp13=tmp12.keys.map{line =>line.slice(1, line.length-1)}.map(line=>("("+line.split(",")(1)+','+line.split(",")(0)+")"))  // (ej,ei)
      val tmp14 = tmp12.values // wij
      val tmp15 = tmp13.zip(tmp14) // (ej.ei), wij

      // Step6:: Get only top-K weights, PER ENTITY since it is CNP

      val tmp16 = tmp12.keys.map{line =>line.slice(1, line.length-1)}.map(line=>(line.split(",")(1)+','+"("+line.split(",")(0))) // ej,ei
      val tmp17=tmp16.zip(tmp14) 
      // tmp17.saveAsTextFile("E:/spark/2019/tmp17") // (ej,(ei,wij)
      val tmp18 = tmp17.map{line=>line.toString}.map{line =>line.slice(1, line.length-1)}.map{line=>line.replaceFirst(",\\(", "#")}.map{line =>
      val tokens = line.split('#')
      (tokens(0),(tokens(1)))
      } //(ej,ei,wij)

      val tmp19 = tmp18.map{line=>line.toString}.map{line=> line.split(",")}.map{x=>((x(0),x(1)),x(2))}.sortBy(_._2,false)//map{case(a,b,c)=>((a,b),c)} // (((ej.ei),wij)) , sorted by wij, DESC

      // CNP uses a cardinality criterion, which retains the top k weighted edges within every node neighborhood. This is a LOCAL, not a GLOBAL threshold
      //val k=1;
      val k=prop.getProperty("spark.myapp.cardinality.threshold").toInt 
      val result = tmp19.map{case((a,b),c)=>(a,(b,c))}.groupByKey.mapValues(_.take(k)).mapValues{_.mkString(",")} //ej,List(ei,wij)

      val writer = new PrintWriter(output)
      result.collect.foreach(line=>writer.println(line))
      writer.close()
      } // end try

catch {
      case e: IOException => e.printStackTrace()
} 

finally {
      sc.stop()
}
}
}
