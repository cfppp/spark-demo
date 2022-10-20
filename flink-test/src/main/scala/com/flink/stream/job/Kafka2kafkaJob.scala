package com.flink.stream.job

import com.flink.stream.sink.KafkaSink
import com.flink.stream.source.KafkaSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Kafka2kafkaJob {
  def main(args: Array[String]): Unit = {
    //todo get env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //todo get source

    val stream = env
      .addSource(KafkaSource.getKafkaSource)


    //todo transformation


    //todo sink
    stream.addSink(KafkaSink.getKafkaSink)

    //todo execute
    env.execute()

  }
}
