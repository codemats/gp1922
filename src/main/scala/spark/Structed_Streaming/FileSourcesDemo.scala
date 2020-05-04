package spark.Structed_Streaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object FileSourcesDemo {
  def main(args: Array[String]): Unit = {

    val filePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/file/person"

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val field:List[StructField] = List(
      StructField("name",StringType),
      StructField("age",IntegerType),
      StructField("sex",StringType)
    )

    ////从文件读取
    val lines: DataFrame = spark.readStream
      .format("csv")
      .schema(StructType(field))
      .load(filePath)

    val query: StreamingQuery = lines.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    query.awaitTermination()

  }
}
