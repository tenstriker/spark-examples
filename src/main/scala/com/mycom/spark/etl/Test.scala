package com.mycom.spark.etl

import java.util.Date
import java.util.Calendar
import java.text.DecimalFormat

object Test {
  
  def main(args: Array[String]) {
  
    val epoch = "1470045600".toLong
    val dt = new Date(epoch * 1000 )
    println(dt)
    val calendar = Calendar.getInstance
    calendar.setTime(dt)
    val year = calendar.get(Calendar.YEAR)
    val month = calendar.get(Calendar.MONTH) + 1
    val day = calendar.get(Calendar.DAY_OF_MONTH)
    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    
    val mformat = new DecimalFormat("00")
    println(mformat.format(month))
    println(mformat.format(day))
    println(mformat.format(hour))
    
    println(s"$year $month $day $hour")
    
    
    val d1 = "AL#2017#08#01#09"
    val d2 = "AL#2017#08#01#"
    
    println(d1.substring(0, d1.lastIndexOf('#')))
    
    println(("AL","","","","246916").productIterator.mkString("#"))

  }
}