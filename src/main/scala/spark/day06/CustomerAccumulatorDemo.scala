package spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

//在定义累加器实现List(1,2,3,4,5,6)相加----结果：21
object CustomerAccumulatorDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //注册累加器
    val myAccumulator =  new MyAccumulator
    sc.register(myAccumulator,"MyAccumulator")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))

    //循环累加
    rdd.foreach(x=>{
      myAccumulator.add(x)
    })

    //打印结果 sum:21
    println(s"sum:${myAccumulator.value}")
    sc.stop()
  }
}

//在定义累加器
class MyAccumulator extends AccumulatorV2[Int, Int] {

  //设定初始值
  private var sum: Int = _

  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new MyAccumulator
    acc.sum = this.sum
    acc
  }

  override def reset(): Unit = this.sum = 0

  override def add(v: Int): Unit = {
    this.sum += v
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
    case x: MyAccumulator => this.sum += x.sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Int = this.sum
}
