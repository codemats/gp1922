package spark.day09

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 统计单词个数（包含历史单词个数），打印单词全部个数(含以前批次的统计结果)
  */
object SparkStreamingUpdateStateByKeyWC {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    val ssc = new StreamingContext(conf,Seconds(2))

    ssc.checkpoint("/Users/a10.12/Documents/study/bigdata/spark/gp1922/file/checkpoint")
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
      //实际中, 大多数情况下使用如上所示的 LocationStrategies.PreferConsistent, 将在可用的 executors 上均匀分布分区
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //更新单词状态
    val result: DStream[(String, Int)] = stream.flatMap(_.value().split(" ")).map((_,1))
      .updateStateByKey[Int](updateFunc _,new HashPartitioner(2))

    //统计个数，并打印
    result.reduceByKey(_+_).print()

    //启动
    ssc.start()
    //停止
    ssc.awaitTermination()
  }


  //更新key的状态
  def updateFunc(newValues: Seq[Int], buffer: Option[Int]): Option[Int] = {
    //初始值
    var keySatas:Int = 0 ;

    //变量用于更新，加上上一个状态的值，这里隐含一个判断，如果有上一个状态就获取，如果没有就赋值为0
    keySatas = buffer.getOrElse(0)

    //遍历当前的序列，序列里面每一个元素都是当前批次的数据计算结果，累加上一次的计算结果
    newValues.foreach(value=>{
      keySatas+=value
    })

    //返回值
    Some(keySatas)
  }
}
