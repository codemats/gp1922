package spark.Structed_Streaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//单词统计
object WordCountDemo {
  def main(args: Array[String]): Unit = {

    //初始化环境
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[2]")
      .getOrCreate()

    //从NC读取数据
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._

    //数据处理
    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts: DataFrame = words.groupBy("value").count()

    //输出
    val query: StreamingQuery = wordCounts.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    //等待停止
    query.awaitTermination()
  }
}
