package spark.day09.kafka_api

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

//生产者API允许应用程序将数据流发送到Kafka集群中的主题
object KafkaProduceDemo {
    def main(args: Array[String]): Unit = {

        val props = new Properties
        props.put("bootstrap.servers", "172.16.109.132:9092")
        props.put("acks","1")  //全部[master,salves]收到才算接受到消息
        props.put("retries","4")   //失败重试4次
        props.put("linger.ms","1") //发送之前在等1ms
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //key序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //value 序列化

        //创建生产者
        val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](props)
        (1 to 100).foreach(count=>{
            //创建消息内容【主题,key值,value值】
            val message = new ProducerRecord[String,String]("test0",count.toString,s"这是发送消息第${count.toString}条")
            //发送消息,Callback回调，证明服务器已确认记录的元数据
            producer.send(message,new Callback {
                override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
                        if(recordMetadata.hasOffset){
                            println(s"消息已被保存在，偏移量为${recordMetadata.offset()}")
                        }else if(e != null){
                            throw e
                        }
                }
            })
            println(s"这是发送消息第${count.toString}条")
            TimeUnit.SECONDS.sleep(1)
        })
        //资源释放
        producer.close()
    }
}
