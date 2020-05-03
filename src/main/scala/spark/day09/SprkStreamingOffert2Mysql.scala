package spark.day09

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.UUID
import javax.security.auth.callback.Callback

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 需求：
  * 1.kafka的偏移量写入mysql
  * 2.保存kafka的数据只被消费一次(策略：读取mysql中kafka的消息偏移量)
  */
object SprkStreamingOffert2Mysql {

  def main(args: Array[String]): Unit = {
    //初始化配置环境
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    //初始化批次时间间隔：2秒
    val ssc = new StreamingContext(conf, Seconds(2))

    //初始化操作mysql
    val consumerKafkaOffset2Mysql = ConsumerKafkaOffset2Mysql()
    val mysqlUtils = consumerKafkaOffset2Mysql.mysqlUtils


    //创建读取kafka数据流
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.109.132:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //消费kafka的主题
    val topics = Array("test0")

    //获取mysql中kafka的偏移量读取kafka中的数据，（已下功能没有实现）并判断数据库中偏移量是否已经处理，没有处理则正常处理数据，有处理则跳过
    val offsetRanges = null
    val resultSet: ResultSet = mysqlUtils.getObjectData(consumerKafkaOffset2Mysql.queryKafkaOffset, List("test0"))
    while (resultSet.next()) {

      val offsetRange = Array(OffsetRange(
        resultSet.getString("topic"),
        resultSet.getString("partitions").toInt,
        resultSet.getString("start_offset").toLong,
        resultSet.getString("end_offsert").toLong
      ))

      val kafkaParamsJU = new util.HashMap[String, Object]()
      kafkaParamsJU.put("bootstrap.servers", "172.16.109.132:9092")
      kafkaParamsJU.put("key.deserializer", classOf[StringDeserializer])
      kafkaParamsJU.put("value.deserializer", classOf[StringDeserializer])

      val rdd: RDD[ConsumerRecord[String, String]] = KafkaUtils.createRDD[String, String](ssc.sparkContext,
        kafkaParamsJU, offsetRange, LocationStrategies.PreferConsistent)

      rdd.foreachPartition { iter =>
        while (iter.hasNext) {
          println("根据偏移量获取的数据：" + iter.next().value())
          //写业务---code
        }
      }
      //置空，防止到executor端造成task没有序列化
      mysqlUtils.setNull()
    }

    //创建直接流的方式
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //获取偏移量
    try {
      stream.foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
          try {
            while (iter.hasNext) {
              //如果没有连接就启动连接
              if (mysqlUtils.con == null) {
                mysqlUtils.getConnect()
              }
              mysqlUtils.con.setAutoCommit(false) //开启事务
              //获取kafka的topic中的消息
              val message: String = iter.next().value()

              //获取kafka的topic的消息偏移量
              val offset: OffsetRange = offsetRanges(TaskContext.get.partitionId)

              println(s"topic:${offset.topic},partition: ${offset.partition}" +
                s",fromOffset: ${offset.fromOffset},untilOffset: ${offset.untilOffset}" +
                s"，消息：${message}")

              val queryList = List(offset.fromOffset.toString, offset.untilOffset.toString, message, offset.partition.toString, offset.topic)
              //更新结果为0，表示数据库没有
              if (mysqlUtils.update(consumerKafkaOffset2Mysql.updateKafkaOffset2Mysql, queryList) == 0) {
                //kafka偏移量保存到mysql
                val id: String = UUID.randomUUID().toString.replaceAll("-", "")
                //保存
                mysqlUtils.save(consumerKafkaOffset2Mysql.saveKafkaOffset2Mysql,
                  List(id, offset.topic.trim, offset.fromOffset.toString.trim, offset.untilOffset.toString.trim, offset.partition.toString, message))
              }

              //以下可以实现业务逻辑，mysql可新加事务机制，防止数据丢失或者修复消费
              //业务

              //模拟业务异常，重新启动后拿到之前处理失败的数据，重新执行
              val a: Int = 1 / 0 //业务异常

              mysqlUtils.con.commit() //事务提交
            }
          } catch {
            case e: Exception => {
              mysqlUtils.con.rollback() //事务回滚
              e.printStackTrace()
              throw new RuntimeException
            }
          }
        }
        //手动提交偏移量
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges, new OffsetCommitCallback {
          override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = {
            if (e == null) {
              println("kafka偏移量已经提交...")
              //已下操作可以更新数据库中偏移量是否已经提交，这里代表前面的的业务没有出错，数据已经消费完成
            }
          }
        })
      }
    } catch {
      case  e: Exception =>{
        e.printStackTrace()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

//获取mysql中存储的kafka的偏移量工具
case class SparkStreamingOffsetUtils(url: String, user: String, password: String) {

  //初始值-设置mysql参数的循环初值
  val initSqlArgsValue: Int = 1
  //连接数据库对象
  var con: Connection = _
  //操作数据库预操作对象
  var statement: PreparedStatement = _

  //初始化操作，加载驱动,这里操作是在Driver端
  {
    Class.forName("com.mysql.jdbc.Driver")
  }

  //获取连接
  def getConnect(): Unit = {
    con = DriverManager.getConnection(url, user, password)
  }

  //sql设置占位符值
  def setQueryStatment(sql: String, sqlArsg: List[String]): Unit = {
    //如果连接是null，初始化，注意放在Driver端写会报错：task没有序列化
    if (con == null) {
      getConnect()
    }
    var offset: Int = initSqlArgsValue
    statement = con.prepareStatement(sql)
    println(s"执行sql：${sql},参数：${sqlArsg.toBuffer}")
    sqlArsg.foreach(args => {
      statement.setObject(offset, args)
      offset += 1
    })
  }

  //数据保存
  def save(sql: String, sqlArsg: List[String]): Int = {
    if (sqlArsg.isEmpty) throw new NullPointerException("参数sqlArsg不能为空")
    setQueryStatment(sql, sqlArsg)
    executorSql("saveOrUpdate").asInstanceOf[Int]
  }

  //获取数据
  def getObjectData(sql: String, sqlArsg: List[String]): ResultSet = {
    setQueryStatment(sql, sqlArsg)
    executorSql("query").asInstanceOf[ResultSet]
  }

  //数据更新
  def update(sql: String, sqlArsg: List[String]): Int = {
    setQueryStatment(sql, sqlArsg)
    executorSql("saveOrUpdate").asInstanceOf[Int]
  }

  //执行sql
  def executorSql(typeModel: String): Any = {
    typeModel match {
      case "saveOrUpdate" => statement.executeUpdate()
      case "query" => statement.executeQuery()
    }
  }

  //关闭数据库资源
  def close(): Unit = {
    if (this.statement != null) this.statement.close()
    if (this.con != null) this.con.close()
  }

  def setNull(): Unit = {
    this.con = null
    this.statement = null
  }
}


//保存kafka偏移量到mysql
case class ConsumerKafkaOffset2Mysql() {

  //mysql登录url，用户，密码
  val mysqlInfo = Map[String, String](
    "url" -> "jdbc:mysql://localhost/spark_user?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC",
    "user" -> "root",
    "password" -> "root"
  )

  //初始化mysql
  val mysqlUtils = SparkStreamingOffsetUtils(mysqlInfo("url"), mysqlInfo("user"), mysqlInfo("password"))

  //保存偏移量sql
  val saveKafkaOffset2Mysql = "insert into kafka_offset(id,topic,start_offset,end_offsert,partitions,value) values(?,?,?,?,?,?)"

  //更新偏移量
  val updateKafkaOffset2Mysql = "update kafka_offset set start_offset= ? , end_offsert= ? , value= ? , partitions= ? where topic= ?"

  //查找偏移量
  val queryKafkaOffset = "select topic,start_offset,end_offsert,partitions from kafka_offset where topic= ?"

}
