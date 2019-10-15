Big Data Craft Demonstration

 You have given two data sets in HDFS as mentioned below.
o Customer information &lt;customer_id,name,street,city,state,zip&gt;
o Sales information &lt;timestamp,customer_id,sales_price&gt;
Implement a Spark application using Spark Core (not with Spark SQL) to
get &lt;state,total_sales&gt; for (year, month, day and hour) granularities. Be
prepared to present your code and demonstrate it running with appropriate
input and output.
Notes:
1. You can consider input/output data set in any one of the below format
a. Text(with any DELIMITER)
b. AVRO
c. PARQUET
2. Consider timestamp in epoch (for example 1500674713)
3. We encourage you to consider all possible cases of datasets like
number of states are small(finitely known set) OR huge(unknown set)
and come with appropriate solutions.
4. You can you use any of PYTHON/SCALA/JAVA API’s of your
choice.

Example Input: (Assume input as text format with &#39;#&#39; as delimiter)

customers:
123#AAA Inc#1 First Ave Mountain View CA#94040
456#ABC Inc#2 First Ave Fayetteville AK#72703
789#DEF Inc#3 First Ave Mobile AL#36571
101112#GHI Inc#4 First Ave Portland OR#97205

Sales:
1454313600#123#123456
1501578000#789#123457
1470045600#456#123458
1470049200#789#123459

Example Output:
state#year#month#day#hour#sales

AL#2017#08#01#09#123457
AL#2017#08#01##123457
AL#2017#08###123457
AL#2017####123457
AL#2016#08#01#11#123459
AL#2016#08#01##123459
AL#2016#08###123459
AL#2016####123459
AL#####246916
AK#2016#08#01#10#123458
AK#2016#08#01##123458
AK#2016#08###123458
AK#2016####123458
AK#####123458
CA#2016#02#01#08#123456
CA#2016#02#01##123456
CA#2016#02###123456
CA#2016####123456
CA#####123456