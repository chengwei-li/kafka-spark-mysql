package com.studysai.lamda.realTime

import java.util.Properties

import com.studysai.lamda.proto.Spark.NewClickEvent
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
  * Created by lj on 2017/5/22.
  */
object BehaviorProducer {

  val newClickEvents = Seq(
    (1000000L, 123L),
    (1000001L, 400L),
    (1000002L, 500L),
    (1000003L, 278L),
    (1000004L, 681L)
  )

  def run(topic : String): Unit ={
    val props : Properties = new Properties
    props.put("metadata.broker.list","hadoopMaster@studysai.com:9092")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val conf : ProducerConfig = new ProducerConfig(props)
    var producer : Producer[String, Array[Byte]] = null
    try {
      producer = new Producer[String, Array[Byte]](conf)
      for (event <- newClickEvents) {
        val eventProto = NewClickEvent.newBuilder().setUserId(event._1).setItemId(event._2).build()
        producer.send(new KeyedMessage[String, Array[Byte]](topic, eventProto.toByteArray))
        println("event is :" + eventProto.toString)
      }
    } catch {
      case ex : Exception => println("exception is :" + ex.getMessage)
    } finally {
      if (producer != null)
        producer.close
    }
  }

  def main(args: Array[String]): Unit = {
    BehaviorProducer.run("behaviorAnalyzer")
  }
}
