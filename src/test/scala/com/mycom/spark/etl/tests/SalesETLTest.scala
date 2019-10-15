package com.mycom.spark.etl.tests

import com.mycom.spark.etl.SalesETL
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SalesETLTest {
  
  def main(args: Array[String]) {
    
    val master = "local"
    val sparkConf = new SparkConf().setAppName("spark-test").setMaster(master)
    val sparkContext = new SparkContext(sparkConf)
        
    val salesRdd = SalesETL.processSalesData(sparkContext)
    val salesRddRaw = salesRdd.map(t => t._1+"#"+t._2)
    val expectedRddRaw = sparkContext.textFile("src/test/resources/expected.txt")
    val expectedRdd = expectedRddRaw.map(str => {
      val lastidx = str.lastIndexOf('#')
      (str.substring(0, lastidx + 1), str.substring(lastidx + 1).toDouble)
    })

    val salescnt = salesRdd.count()
    val expectedcnt = expectedRdd.count()
    val intersectedcnt = salesRddRaw.intersection(expectedRddRaw).count()
    
    assert(salescnt == expectedcnt)
    assert(intersectedcnt == salescnt)
    assert(intersectedcnt == expectedcnt)
    

    
  }
}