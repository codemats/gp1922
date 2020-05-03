package spark.day09

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//练习--消费kafka消息,单词统计
object SparkStreamingByKafkaWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))

    //创建读取kafka数据流
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.109.132:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //消费kafka的主题
    val topics = Array("test0")

    //创建直接流的方式
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //业务数据处理(单词统计)
    val result: DStream[(String, Int)] = stream.flatMap(_.value().split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    //启动
    ssc.start()
    //停止
    ssc.awaitTermination()
  }
}
