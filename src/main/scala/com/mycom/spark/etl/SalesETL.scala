package com.mycom.spark.etl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.time.ZonedDateTime
import java.util.Date
import java.util.Calendar
import java.text.DecimalFormat
import org.apache.spark.rdd.RDD


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


case class Sales(timestamp: Long, year: String, month: String, day: String, hour: String, customerId: Long, price: Double)

object Sales {
  
  def parse(line: String, headerMap: Map[String, Int]): Option[Sales] = {
    
    try {
      
      val arr = line.split("#")
      
      val timestamp = arr(headerMap("timestamp")).toLong 
      val calendar = Calendar.getInstance
      calendar.setTime(new Date(timestamp * 1000))
      val year = calendar.get(Calendar.YEAR)
      val month = calendar.get(Calendar.MONTH) + 1
      val day = calendar.get(Calendar.DAY_OF_MONTH)
      val hour = calendar.get(Calendar.HOUR_OF_DAY)
      
      val customerId = arr(headerMap("customer_id")).toLong
      val price = arr(headerMap("sales_price")).toDouble
      
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
  
  def processSalesData(sparkContext: SparkContext): RDD[(String, Double)] = {
    
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
      
      
    
    
    val salesRdd = sparkContext.textFile("src/test/resources/sales.txt")
      .flatMap(line => {
        val salesOpt = Sales.parse(line, salesHeaderMap)
        if(salesOpt.isDefined) Iterable(salesOpt.get) else Iterable()
      })
      
    salesRdd.foreach(println)
    
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
      
    }
    
    val joinedRdd = broadcastJoin()
    joinedRdd.foreach(println)
    
    import org.apache.spark.rdd.PairRDDFunctions
    
    val hourly = joinedRdd.map(t => (t._1, t._2, t._3, t._4, t._5) -> t._6)
    //.map(t => (t._1 +"#"+ t._2 +"#"+ t._3 +"#" + t._4 +"#"+ t._5 -> t._6))
      .reduceByKey(_+_)
    hourly.foreach(println)
    
    val daily = hourly.map(t => (t._1._1, t._1._2, t._1._3, t._1._4, "") -> t._2)
      .reduceByKey(_+_)
    daily.foreach(println)
    
    val monthly = daily.map(t => (t._1._1, t._1._2, t._1._3, "", "") -> t._2)
      .reduceByKey(_+_)
    monthly.foreach(println)
    
    val yearly = monthly.map(t => (t._1._1, t._1._2, "", "", "") -> t._2)
      .reduceByKey(_+_)  
    yearly.foreach(println)
    
    val statewide = yearly.map(t => (t._1._1, "", "", "", "") -> t._2)
      .reduceByKey(_+_)  
    statewide.foreach(println)
    
    val unionRdd = hourly.union(daily).union(monthly).union(yearly).union(statewide)
      .sortByKey(false, 10)
      .map(t => t._1.productIterator.mkString("#") -> t._2)
      
    unionRdd
  }
  
  def main(args: Array[String]) {
    
    println("inside SalesETL-main")
    
    val master = "local"
    val sparkConf = new SparkConf().setAppName("spark-test").setMaster(master)
    val sparkContext = new SparkContext(sparkConf)
    
    val salesrdd = processSalesData(sparkContext)
    salesrdd.map(t => t._1+"#"+t._2).collect().foreach(println)
    
  }
  
}