package com.flink.stream.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSink {

  def getKafkaSink : FlinkKafkaProducer[String] = {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "hadoop102:9092")

    new FlinkKafkaProducer[String]("sink_flink",new SimpleStringSchema(),properties)
  }
}
