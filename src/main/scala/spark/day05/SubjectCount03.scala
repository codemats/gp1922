package spark.day05

import java.net.URL
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable

//要求：统计所有用户对每个学科的各个模块，保存到hdfs[可本地磁盘]，
//1.定义分区
//2.保存到hdfs上，每个文件放相同的数据

//默认：使用默认的2个分区，保存数据，会出现数倾斜（1文件3条数据，2文件2条数据）
//result.distinct().saveAsTextFile(SparkUtils.hdfsFile+"/001")
//
//使用在定义分区，每个文件只存相同的数据
object SubjectCount03 {

  def main(args: Array[String]): Unit = {
    val conf = SparkUtils.getConf(this.getClass.getName, "local[2]")
    val sc = SparkUtils.getSparkContext(conf)

    val lines: RDD[String] = sc.textFile(SparkUtils.filePath)

    val result: RDD[(String, Int)] = lines.map(x => {
      val arr: Array[String] = x.split(",")
      (new URL(arr(1)).getHost, 1)
    }).reduceByKey(_+_)

    val value: RDD[String] = result.keys.distinct()
    //使用默认的2个分区，保存数据，会出现数倾斜（1文件3条数据，2文件2条数据）
    //value.saveAsTextFile(SparkUtils.hdfsFile+"/001")

    val collect: Array[String] = value.collect

    val rePartitionSubjectResult: RDD[(String, Int)] = result.partitionBy(new MyPartition(collect))

    rePartitionSubjectResult.mapPartitions((x: Iterator[(String, Int)]) => {
      //排序获取最高前3者
      val tuples: List[(String, Int)] = x.toList.sortWith(_._2 > _._2).take(3)
      tuples.iterator
    }).saveAsTextFile(SparkUtils.hdfsFile + "/005")

    SparkUtils.stop(sc)
  }
}

class MyPartition(value: Array[String]) extends Partitioner {

  private[this] var parMap = new mutable.HashMap[String, Int]()

  //分区记录器(分区从0开始启)，使用AtomicInteger防止多线程操作
  private val atomInt = new AtomicInteger(-1)

  private var parNum = 0

  //初始化这个map
  value.map(x => {
    parMap.put(x, atomInt.incrementAndGet())
  })
  println("MyPartition 已经初始化了...")

  override def numPartitions: Int = {
    println("numPartitions...:" + this.parMap.size)
    this.parMap.size
  }

  override def getPartition(key: Any): Int = {

    val partitionStr: String = key.toString

    parMap.get(partitionStr) match {
      case Some(x) => this.parNum = x
      case None => println("没有匹配到....")
    }
    println("getPartition...:" + parNum)
    this.parNum
  }

}


