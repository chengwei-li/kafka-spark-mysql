package com.studysai.lamda.realTime

import com.studysai.lamda.proto.Spark.NewClickEvent
import com.studysai.lamda.webService.RedisClient
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lj on 2017/5/22.
  */
object BehaviorConsumer {

  def main(args: Array[String]): Unit = {
    val Array(broker, topic) = Array("", "")

    val sparkConf = new SparkConf().setMaster("local").setAppName("BehaviorAnalyzer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "auto.offset.reset" -> "smallest"
    )
    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicSet)

    messages.map(_._2).map {
      event => NewClickEvent.parseFrom(event)
    }.mapPartitions{
      iter =>
        val jedis = RedisClient.pool.getResource
        iter.map{
          event =>
            val userId = event.getUserId
            val itemId = event.getItemId
            val key = "II:" + itemId
            val value = jedis.get(key)
            if (value != null)
              jedis.set("RUI:" + userId, value)
        }
    }.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
