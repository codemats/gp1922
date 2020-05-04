package spark.Structed_Streaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

//数据写入kafka
object WriteData2KafkaDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    //数据写入kafka必须设置checkpoint
    spark.conf.set("spark.sql.streaming.checkpointLocation", "./")

    //产生测试数据
    val data: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .option("rampUpTime", 2)
      .option("numPartitions", 1)
      .load()


    //数据写到Kafka
    val query: StreamingQuery = data.toJSON.selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.16.109.137:9092")
      .option("topic", "test0")
      .start()

    query.awaitTermination()
  }
}

