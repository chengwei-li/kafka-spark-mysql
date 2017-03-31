package com.studysai

import java.sql.DriverManager

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SQLContext, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * kafka-spark-mysql实例
  * Created by giuseppe on 2017/3/30.
  */
object TestKSM {

  case class UserInfo (name: String, age : Int, sex : String, height : Float)

  def main(args: Array[String]): Unit = {
    /**
      * 获取mysql链接
      */
    val url = ""
    val userName = ""
    val password = ""
    

    /**
      * 创建sparkContext与SparkStreamingContext
      */
    val conf = new SparkConf().setAppName("KSM")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    /**
      * kafka 部分
      */
    val topic = Set("userTopic")
    val brokers = "hadoopMaster.studysai.com:9092,hadoopSlave1.studysai.com:9092,hadoopSlave2.studysai.com:9092"
    val kafkaParam = Map("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    //使用direct方式创建，还有另一种为reciever，较复杂且低稳定性，突然挂掉会丢失数据，所以不使用
    val kafkaStream = KafkaUtils.createDirectStream(ssc, kafkaParam, topic)

    //获取topic中的数据,每条消息tostring
    val kafkaData = kafkaStream.flatMap(line =>{
      Some(line.toString())
    })

    //数据处理:拆分，过滤,映射
    val tempDS = kafkaData.map(_.split(" ")).filter(_.length != 4).map(s => UserInfo(s(0).trim, s(1).toInt, s(2).trim, s(3).toFloat))

    //
    tempDS.foreachRDD{ rdd =>{
        rdd.foreachPartition(records => {
          val conn = DriverManager.getConnection(url, userName, password)
          val psta = conn.prepareStatement("INSERT INTO  UserInfo (name, age, sex, height) VALUES (?,?,?,?)")
          records.foreach(userInfo =>{
            psta.setString(1, userInfo.name)
            psta.setInt(2, userInfo.age)
            psta.setString(3, userInfo.sex)
            psta.setFloat(4, userInfo.height)
            psta.execute()
          })
          psta.close()
          conn.close()
        })
      }
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}
