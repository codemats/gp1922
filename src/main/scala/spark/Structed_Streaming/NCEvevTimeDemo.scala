package spark.Structed_Streaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

//NC的事件时间
object NCEvevTimeDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //从NC读取数据
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()

    val result: DataFrame = lines.as[(String,Timestamp)].flatMap {
      case (words, ts) => words.split(" ").map((_, ts))
    }.toDF("word", "ts")
      .groupBy(
        window($"ts", "4 minutes", "2 minutes")
        , $"word"
      ).count()


    val query: StreamingQuery = result.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("truncate", "false")
      .start()

    query.awaitTermination()


  }
}
