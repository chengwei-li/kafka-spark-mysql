package com.studysai

import java.util.Properties

import kafka.producer.ProducerConfig
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage

import scala.util.Random

/**
  * 模拟kafka producer发送消息
  * Created by giuseppe on 2017/3/30.
  */
object TestProducer {

  private val random = new Random()

  def main(args: Array[String]): Unit = {
    val brokers = "hadoopMaster.studysai.com:9092,hadoopSlave1.studysai.com:9092,hadoopSlave2.studysai.com:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    while (true){
      val userName = "giuseppe" + random.nextInt(10)
      val age = random.nextInt(100) + ""
      val height = random.nextFloat() + ""
      val message = userName + " " + age + " " + "man" + " " + height

      producer.send(new KeyedMessage[String, String]("userTopic", message))
      Thread.sleep(500)
    }
  }
}
