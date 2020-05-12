package com.test.spark.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

/**
 * This stream reads from Kafka topic and writes it to console
 */
object SparkStreamingConsumer extends App {

  val spark = SparkSession.builder()
    .appName("SparkStreamingConsumer")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "wordcount")
    .option("startingOffsets", "latest")
    .option("timestamp", true)
    .load()

  val data = df.selectExpr("CAST( value as STRING)", "CAST(timestamp as LONG)").as[(String, Timestamp)]

  val wordsDf = data.toDF("word", "time")


  val query = wordsDf.writeStream
    .format("console")
    .option("truncate","false")
    .outputMode("append")
    .start()

  query.awaitTermination()
}
