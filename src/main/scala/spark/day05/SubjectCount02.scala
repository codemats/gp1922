package spark.day05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

//要求：统计所有用户对每个学科的各个模块的访问次数，再取Top3--使用缓存（catch）
//打印包含subjects内容开始的数据
object SubjectCount02 {

  def main(args: Array[String]): Unit = {

    val subjects = Array("http://bigdata.learn.com", "http://ui.learn.com/ui",
      "http://java.learn.com", "http://h5.learn.com", "http://android.learn.com")

    val conf = SparkUtils.getConf(this.getClass.getName,"local[2]")
    val sc = SparkUtils.getSparkContext(conf)

    //广播
    val subjectBroadcast: Broadcast[Array[String]] = sc.broadcast(subjects)
    //读取文件
    val lines: RDD[String] = sc.textFile(SparkUtils.filePath)

    //获取数据并缓存
    val resultCatch: RDD[(String, Int)] = lines.map(_.split(","))
      .map(x => (x(1), 1))
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_ONLY)

    resultCatch.collect.foreach(println)

    println("-------------------------------------")

    subjectBroadcast.value.foreach(x=>{

      val fliterValue: RDD[(String, Int)] = resultCatch.filter(_._1.startsWith(x))

      val value: RDD[(String, Int)] = fliterValue.sortBy(_._1)

      val result: Array[(String, Int)] = value.take(3)

      println(result.toBuffer)
    })


    SparkUtils.stop(sc)
  }
}
