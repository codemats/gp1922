package spark.day06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//广播变量----把Drive端的数据广播到每个worker上，在计算时worker可以获取该数据进行计算，不需要
//从Drive端拷贝一份(否侧只要计算就要从Drive端拷贝，消耗大量的网络IO)
object BoradcastDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //Drive端的数据
     val userNameList: ListBuffer[String] = ListBuffer("李梅","王晓路","李晓燕","王麻子","王茶月")
    //广播
    val userNameBroadcast: Broadcast[ListBuffer[String]] = sc.broadcast(userNameList)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

    rdd.foreach(line=>{
      //拿到广播的值
      val userNameList: ListBuffer[String] = userNameBroadcast.value
      val str = userNameList(line-1)
      println(str)
    })

    sc.stop()
  }
}
