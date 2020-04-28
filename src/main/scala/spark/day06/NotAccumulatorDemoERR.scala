package spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//常见错误---没有使用累加器
//需求：求List(1,2,3,4,5,6)的和
object NotAccumulatorDemoERR {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    var sum = 0 ;  //在 drive中执行

    val listData: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    //一下在excutor端执行
    //循环求和
    listData.foreach(x=>{
      sum += x  //这里的sum确实21，但是无法返回给Drive端中，使用sum还是初始值0
    })

    println(s"sum:${sum}")
    //结果：sum = 0
    sc.stop()
  }
}
