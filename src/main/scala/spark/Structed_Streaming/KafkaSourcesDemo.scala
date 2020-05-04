package spark.Structed_Streaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object KafkaSourcesDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    //Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("group.id","test")
      .option("kafka.bootstrap.servers", "172.16.109.137:9092")
      //主题
      .option("subscribe", "test0")
      .load()

    //转换为String，否则是二进制格式
    val data: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]

    //单词统计
     val result = data.flatMap(_.split(" ")).map((_,1)).toDF("word","count").groupBy($"word").count()
    val query: StreamingQuery = result.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    query.awaitTermination()

  }
}
