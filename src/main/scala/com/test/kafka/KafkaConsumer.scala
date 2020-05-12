package com.test.kafka

import java.time.Duration
import java.util.{Collections, Properties}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object KafkaConsumer extends App {

  val props:Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer(props)
  val topics = "wordcount"

  try {
    consumer.subscribe(Collections.singletonList(topics))

    while(true) {
      val records = consumer.poll(Duration.ofSeconds(10))
      println(s"Records recived ${records.count()}")

      for(r <- records.asScala){
        println(s"r.key() ${r.key()}")
        println(s"r.partition() ${r.partition()}")
        println(s"r.timestamp() ${r.timestamp()}")
     }

    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
