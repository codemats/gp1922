package spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

//窗口操作
//需求：每批次2秒钟，每10秒展示一次，每次滑动长度10秒
object SparkWindowsOperationDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    //获取NC数据
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //业务：单词统计
    val temp = inputStream.flatMap(_.split(" ")).map((_, 1))
    //按照窗口统计，窗口长度10秒（展示结果），窗口滑动10秒
    //方式1：val result = temp.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(10), Seconds(10))

    //方式2：
    val result = temp.window(Seconds(10),Seconds(10)).reduceByKey(_+_)

    //结果打印
    result.print()
    //启动
    ssc.start()
    //停止
    ssc.awaitTermination()
  }
}
