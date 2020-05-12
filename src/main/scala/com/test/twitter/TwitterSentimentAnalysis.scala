package com.test.twitter

import twitter4j.Status
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object TwitterSentimentAnalysis extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("TwitterSentimentAnalysis")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  import spark.implicits._

  // Creating a stream from Twitter
  val tweets: DStream[Status] = TwitterUtils.createStream(ssc, None, Array("Modi"))


  val textAndSentences: DStream[String] =
    tweets.
      map(_.getText)

  textAndSentences.print()

  ssc.start()
  ssc.awaitTermination()


//  // To compute the sentiment of a tweet we'll use different set of words used to
//  // filter and score each word of a sentence.
//  // Since these lists are pretty small it can be worthwhile to broadcast those across the cluster so that every
//  // executor can access them locally
//  val uselessWords = spark.sparkContext.broadcast(load("/stop-words.dat"))
//  val positiveWords = spark.sparkContext.broadcast(load("/pos-words.dat"))
//  val negativeWords = spark.sparkContext.broadcast(load("/neg-words.dat"))
//
//
//  def load(resourcePath: String): Set[String] = {
//    val source = Source.fromInputStream(getClass.getResourceAsStream(resourcePath))
//    val words = source.getLines.toSet
//    source.close()
//    words
//  }

}
