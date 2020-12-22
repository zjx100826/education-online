package com.atguigu.qzpoint.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object RegisterProducer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    ssc.textFile("file://"+this.getClass.getResource("/register.log").getPath, 10)
//    ssc.textFile("/user/atguigu/kafka/register.log", 10)
      .foreachPartition(partition => {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        props.put("acks", "1") //0最快（不应答，丢东西），-1最慢（所有都需要应答），取中间
        props.put("batch.size", "16384") //批次吞吐的大小
        props.put("linger.ms", "10") //批次的延迟最大时间
        props.put("buffer.memory", "33554432") //缓冲区的大小
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item => {
          val msg = new ProducerRecord[String, String]("register_topic",item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
