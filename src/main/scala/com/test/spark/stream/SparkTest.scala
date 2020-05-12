package com.test.spark.stream

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object SparkTest extends App {

  val logFile = "/Users/sushilkotnala/Documents/tech/docker/test1.txt"
  val spark = SparkSession.builder()
    .appName("Spark2")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.addFile(logFile)
  spark.sparkContext.files

  import spark.implicits._

  val data = spark.read.textFile(SparkFiles.get("test1.txt"))
  println(data.take(10).mkString(" "))
  println(data.count())


}
