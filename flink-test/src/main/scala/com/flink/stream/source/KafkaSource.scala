package com.flink.stream.source

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}

object KafkaSource {

  def getKafkaSource  : FlinkKafkaConsumer[String] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "test_flink")

     val source: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("test_flink", new SimpleStringSchema(), properties)
    source
  }
}
