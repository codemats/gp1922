package spark.day04

import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


//检查点---作用：提高数据安全性，减少RDD依赖
//  checkPint   和     Catch
// 数据安全性高        数据安全性低
// 减少RDD依赖        减少RDD依赖
// 提高数据安全性      提高数据安全性

//checkPoint的使用步骤：1.sc.checkPoint(文件目录)  2.有shuffle后面.catche()  3.rdd.checkPoint()
//必须是action算子才会起作用
object CheckPointDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    //1,sc.checkPoint(文件目录)
    sc.setCheckpointDir("hdfs://mac-yangheng:9000/file/study/checkpoint")

    val lines = sc.textFile(FilePathUtils.wordCountFilePath)

    //2,reduceByKey有shuffle则加上catch
    val rdd = lines.flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK)

    //使用：rdd.checkpoint()
    rdd.checkpoint()

    rdd.collect.foreach(println)

    TimeUnit.SECONDS.sleep(Int.MaxValue)
    sc.stop()
  }
}
