package com.test.spark.stream

import org.apache.spark.sql.SparkSession

/**
 * This stream reads from Kafka topic and does count
 */

object WordCountSparkStreamingConsumer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "wordcount")
      .option("startingOffsets", """{"wordcount":{"0":1728}}""")
      .load()

    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val results = data
      .map(_._2)
      .flatMap(value => value.split("\\s+"))
      .groupByKey(_.toLowerCase)
      .count()

    val query = results.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()
  }

}
