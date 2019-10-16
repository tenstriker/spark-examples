package com.mycom.spark.etl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.time.ZonedDateTime
import java.util.Date
import java.util.Calendar
import java.text.DecimalFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import java.util.TimeZone
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext


case class Customer(customerId: Long, name: String, street: String, city: String, state: String, zip: Int) 

object Customer {  
  
  def parse(line: String, headerMap: Map[String, Int]): Option[Customer] = {
    
    try {
      val arr = line.split("#")
      val customerId = arr(headerMap("customer_id"))
      val name = arr(headerMap("name"))
      val street = arr(headerMap("street"))
      val city = arr(headerMap("city"))
      val state = arr(headerMap("state"))
      val zip = arr(headerMap("zip"))
      Some(Customer(customerId.toLong, name, street, city, state, zip.toInt))
      
    } catch {
      case t: Throwable => {
        println("Invalid customer record: " + line)
        None
      }
    }
    
  }
}


case class Sales(timestamp: Long, year: String, month: String, day: String, hour: String, customerId: Long, price: Long)

object Sales {
  
  def parse(line: String, headerMap: Map[String, Int]): Option[Sales] = {
    
    try {
      
      val arr = line.split("#")
      
      val timestamp = arr(headerMap("timestamp")).toLong 
      val calendar = Calendar.getInstance
      calendar.setTime(new Date(timestamp * 1000))
      calendar.setTimeZone(TimeZone.getTimeZone("UTC"))
      val year = calendar.get(Calendar.YEAR)
      val month = calendar.get(Calendar.MONTH) + 1
      val day = calendar.get(Calendar.DAY_OF_MONTH)
      val hour = calendar.get(Calendar.HOUR)
      
      val customerId = arr(headerMap("customer_id")).toLong
      val price = arr(headerMap("sales_price")).toLong
      
      val mformat = new DecimalFormat("00")
      
      Some(Sales(timestamp, mformat.format(year), mformat.format(month), mformat.format(day), mformat.format(hour), customerId, price))
      
    } catch {
      case t: Throwable => {
        println("Invalid sales record: " + line)
        None
      }
    }
    
  }
}

object SalesETL {
  
  def processSalesData(sparkContext: SparkContext, 
      bcastDimension: Boolean = true,
      processParallel: Boolean = false): RDD[(String, Long)] = {
    
    val customerHeaderRdd = sparkContext.textFile("src/test/resources/customerHeader.txt")
    val customerHeaderMap = customerHeaderRdd.collect().head.split("#").zipWithIndex.toMap
    
    val salesHeaderRdd = sparkContext.textFile("src/test/resources/salesHeader.txt")
    val salesHeaderMap = salesHeaderRdd.collect().head.split("#").zipWithIndex.toMap
    
    
    val customerRdd = sparkContext.textFile("src/test/resources/customer.txt")
      .flatMap(line => {
        val customerOpt = Customer.parse(line, customerHeaderMap)
        if(customerOpt.isDefined) Iterable(customerOpt.get) else Iterable()
      })
      .map(cust => cust.customerId -> cust)
      
      
    
    
    val salesRdd = sparkContext.textFile("src/test/resources/sales.txt", 10)
      .flatMap(line => {
        val salesOpt = Sales.parse(line, salesHeaderMap)
        if(salesOpt.isDefined) Iterable(salesOpt.get) else Iterable()
      })
      
    //Assuming customer data is small dataset
    def broadcastJoin() = {
      val customerBcast = sparkContext.broadcast(customerRdd.collectAsMap())
      salesRdd  
        .map(sales => {
          val cust = customerBcast.value(sales.customerId)
          (cust.state, sales.year, sales.month, sales.day, sales.hour, sales.price)      
        })
    }
    
    //Assuming both rdds are large
    def mergeJoin() = {
      val partitioner = new HashPartitioner(10)
      val salesRddMapped = salesRdd.map(sales => sales.customerId -> sales)  
         //.partitionBy(partitioner)//.cache()
      salesRddMapped
        .join(customerRdd)//.partitionBy(salesRddMapped.partitioner.get))
        .map(t => (t._2._2.state, t._2._1.year, t._2._1.month, t._2._1.day, t._2._1.hour, t._2._1.price))
    }
    
    val joinedRdd = if(bcastDimension) broadcastJoin() else mergeJoin()
    
    println(joinedRdd.toDebugString)
    
    import org.apache.spark.rdd.PairRDDFunctions
    
    def processAggSerially() = {
      
      val hourly = joinedRdd.map(t => (t._1, t._2, t._3, t._4, t._5) -> t._6)
        .reduceByKey(_+_)
    
      val daily = hourly.map(t => (t._1._1, t._1._2, t._1._3, t._1._4, "") -> t._2)
        .reduceByKey(_+_)
      
      val monthly = daily.map(t => (t._1._1, t._1._2, t._1._3, "", "") -> t._2)
        .reduceByKey(_+_)
      
      val yearly = monthly.map(t => (t._1._1, t._1._2, "", "", "") -> t._2)
        .reduceByKey(_+_)  
      
      val statewide = yearly.map(t => (t._1._1, "", "", "", "") -> t._2)
        .reduceByKey(_+_)  
      
      val unionRdd = hourly.union(daily).union(monthly).union(yearly).union(statewide)
        .sortByKey(true, 10)
        .map(t => t._1.productIterator.mkString("#") -> t._2)
        
      unionRdd  
      
    }
    
    
    
    def processAggParallely() = {

      // Set number of threads via a configuration property
      val pool = Executors.newFixedThreadPool(5)
      // create the implicit ExecutionContext based on our thread pool
      implicit val xc = ExecutionContext.fromExecutorService(pool)
      
      try {
        
        joinedRdd.persist()
        
        val hourlyF = Future(joinedRdd.map(t => (t._1, t._2, t._3, t._4, t._5) -> t._6)
          .reduceByKey(_+_))
      
        val dailyF = Future(joinedRdd.map(t => (t._1, t._2, t._3, t._4, "") -> t._6)
          .reduceByKey(_+_))
        
        val monthlyF = Future(joinedRdd.map(t => (t._1, t._2, t._3, "", "") -> t._6)
          .reduceByKey(_+_))
        
        val yearlyF = Future(joinedRdd.map(t => (t._1, t._2, "", "", "") -> t._6)
          .reduceByKey(_+_) ) 
        
        val statewideF = Future(joinedRdd.map(t => (t._1, "", "", "", "") -> t._6)
          .reduceByKey(_+_) ) 
        
        val unionF = for {
          
          hourly <- hourlyF
          daily <- dailyF
          monthly <- monthlyF
          yearly <- yearlyF
          statewide <- statewideF
          
        } yield {
          
          val u1 = hourly.union(daily).union(monthly).union(yearly).union(statewide)
          println(u1.toDebugString)
            u1
            .sortByKey(true, 10)
            .map(t => t._1.productIterator.mkString("#") -> t._2)
            
        } 
        val unionRdd = Await.result(unionF, Duration.Inf)
        unionRdd
      
      } finally {
        xc.shutdown()
      }

    }
 
    
    val unionRdd = if(processParallel) processAggParallely() else processAggSerially()
      
    unionRdd
  }
  
  def main(args: Array[String]) {
    
    println("inside SalesETL-main")
    
    val bcast = if(args.length > 0) args(0).toBoolean else true
    val runparallel = if(args.length > 1) args(1).toBoolean else false
    
    val master = "local"
    val sparkConf = new SparkConf().setAppName("spark-test").setMaster(master)
    val sparkContext = new SparkContext(sparkConf)
    
    val salesrdd = processSalesData(sparkContext, bcast, runparallel)
    salesrdd.map(t => t._1+"#"+t._2).collect().foreach(println)
    
    println("exiting SalesETL-main")
    
  }
  
}