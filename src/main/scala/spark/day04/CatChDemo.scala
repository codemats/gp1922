package spark.day04

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//缓存的目的
//1.减少缩短依赖
//2.提高计算效率
object CatChDemo extends Logging{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CatChDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)


    //catch需要在会发生shuffle之后使用
    val lines: RDD[String] = sc.textFile(FilePathUtils.wordCountFilePath)
    logWarning("文件的内容读取...")
    val rdd:RDD[(String,Int)]=lines.flatMap(_.split("\t"))
      .map((_, 1))
      .reduceByKey(_ + _).cache() //底层是：def cache(): this.type = persist() default storage level (`MEMORY_ONLY`)

    //取消缓存
    //rdd.unpersist()
    rdd.collect.foreach(println)

    TimeUnit.SECONDS.sleep(Int.MaxValue)
    sc.stop()
  }
}
