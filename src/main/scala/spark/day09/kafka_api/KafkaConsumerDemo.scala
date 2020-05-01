package spark.day09.kafka_api
import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.{Collections, Properties}

import scala.collection.mutable.ArrayBuffer

//消费者API允许应用程序从Kafka集群中的主题读取数据流
object KafkaConsumerDemo {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "172.16.109.132:9092")
    props.put("group.id", "test") //消费者组id
    props.put("enable.auto.commit", "false")  //手动提交偏移量
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("test0"))
    val minBatchSize = 2

    val buffer = new ArrayBuffer[ConsumerRecord[String, String]]

    while(true){
      val records: ConsumerRecords[String, String] = consumer.poll(100)
      val it: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
      while(it.hasNext){
        val result: ConsumerRecord[String, String] = it.next()
        println(s"收到数据：${result.value()}")
        buffer.append(result)
        if (buffer.size >= minBatchSize) {
          //偏移量保存数据库
          //insertIntoDb(buffer)
          println(s"偏移量:${result.offset()}写入数据库")
          //提交偏移量
          consumer.commitSync()
          buffer.clear()
        }
      }

    }
  }
}
