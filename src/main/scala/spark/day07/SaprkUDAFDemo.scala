package spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//自定义用户聚合函数
//统计文件【/Users/a10.12/Documents/study/bigdata/spark/gp1922/testFile/people.json】中
//用户的平均年龄
object SaprkUDAFDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取文件
    val peopleFilePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/testFile/people.json"
    val peopleFrame: DataFrame = spark.read.json(peopleFilePath)

    //创建临时表
    peopleFrame.createOrReplaceTempView("people")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //注册临时表
    spark.udf.register("myAgeAvg",new MyAgeAvg)

    //处理sql---使用自定义临时表
    spark.sql("select myAgeAvg(age) from people").show()

    /**
      * +-------------+
        |myageavg(age)|
        +-------------+
        |           20|
        +-------------+
      */

    spark.stop()
  }
}


//用户在定义聚合函数---计算平均年龄
class MyAgeAvg extends UserDefinedAggregateFunction{

  //定义数据字段类型
  override def inputSchema: StructType = StructType(List(StructField("age",LongType,nullable = true)))

  //用于计算缓存字段类型
  override def bufferSchema: StructType = {
    StructType(List(StructField("sum",LongType,nullable = true),
      StructField("count",LongType,nullable = true)))
  }

  //返回值类型
  override def dataType: DataType = LongType

  //确定性的，一般设置为true
  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L //sum
    buffer(1) = 0L //count
  }

  //更新--局部聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0)+input.getLong(0)
    buffer(1) = buffer.getLong(1)+1
  }

  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0) / buffer.getLong(1)
  }
}
