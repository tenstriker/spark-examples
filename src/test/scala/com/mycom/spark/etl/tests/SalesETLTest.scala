package com.mycom.spark.etl.tests

import com.mycom.spark.etl.SalesETL
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.scalatest._

abstract class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors
  
class SalesETLTest extends UnitSpec {
  
  val master = "local"
  val sparkConf = new SparkConf().setAppName("spark-test").setMaster(master)
  val sparkContext = new SparkContext(sparkConf)
  
  val expectedRddRaw = sparkContext.textFile("src/test/resources/expected.txt")
  val expectedRdd = expectedRddRaw.map(str => {
    val lastidx = str.lastIndexOf('#')
    (str.substring(0, lastidx + 1), str.substring(lastidx + 1).toDouble)
  })
  val expectedcnt = expectedRdd.count()
    
  "SalesETL with broadcast join and serial execution" should "match expected result" in {
    
     val salesRdd = SalesETL.processSalesData(sparkContext)
     val salesRddRaw = salesRdd.map(t => t._1+"#"+t._2)
     val salescnt = salesRdd.count()
     
     val intersectedcnt = salesRddRaw.intersection(expectedRddRaw).count()
     assert(salescnt == expectedcnt)
     assert(intersectedcnt == salescnt)
     assert(intersectedcnt == expectedcnt)
  }
  
  "SalesETL with sortmerge join and serial execution" should "match expected result" in {
    
      val salesRdd2 = SalesETL.processSalesData(sparkContext, false)
      val salesRddRaw2 = salesRdd2.map(t => t._1+"#"+t._2)
      
      val salescnt2 = salesRdd2.count()
      val intersectedcnt2 = salesRddRaw2.intersection(expectedRddRaw).count()
      
      assert(salescnt2 == expectedcnt)
      assert(intersectedcnt2 == salescnt2)
      assert(intersectedcnt2 == expectedcnt)
  }
  
  "SalesETL with broadcast join and parallel execution" should "match expected result" in {
    
      val salesRdd2 = SalesETL.processSalesData(sparkContext, true, true)
      val salesRddRaw2 = salesRdd2.map(t => t._1+"#"+t._2)
      
      val salescnt2 = salesRdd2.count()
      val intersectedcnt2 = salesRddRaw2.intersection(expectedRddRaw).count()
      
      assert(salescnt2 == expectedcnt)
      assert(intersectedcnt2 == salescnt2)
      assert(intersectedcnt2 == expectedcnt)
  }  
  

}