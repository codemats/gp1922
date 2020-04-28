package spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

//使用spark提供的累加器实现NotAccumulatorDemoERR中的功能
//需求：求List(1,2,3,4,5,6)的和
object SparkAccumulatorDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //spark提供的累加器
    val accLong: LongAccumulator = sc.longAccumulator("addList")
    //acc初始化
    accLong.reset()

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    //循环相加
    rdd.foreach(x=>{
      accLong.add(x)
    })

    println(s"sum:${accLong.value}")
    //结果：sum:21

    sc.stop()
  }
}
