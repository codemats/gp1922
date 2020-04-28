package spark.day05

import java.net.URL

import org.apache.spark.rdd.RDD

//要求：统计所有用户对每个学科的各个模块的访问次数，再取Top3
//使用常规的算子
object SubjectCount01 {

  def main(args: Array[String]): Unit = {
    val conf = SparkUtils.getConf(this.getClass.getName,"local[2]")
    val sc = SparkUtils.getSparkContext(conf)
    val filePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/src/main/scala/spark/day05/file/access.log"

    val userSubjectRdd: RDD[(String, Int)] = sc.textFile(filePath).map(lines => {
      val arrUserInfo = lines.split(",")
      val subjectUrl = arrUserInfo(1)
      val subject = new URL(subjectUrl).getHost()
      (subjectUrl,1)
    })

    val userSubjectReduceRdd: RDD[(String, Int)] = userSubjectRdd.reduceByKey(_+_)

    val userSubjectReduceRddGroup: RDD[(String, Iterable[(String, String, Int)])] = userSubjectReduceRdd.map(x => {
      (new URL(x._1).getHost, x._1, x._2)
    }).groupBy(_._1)

    val resultRdd: RDD[(String, List[(String, String, Int)])] = userSubjectReduceRddGroup.mapValues(_.toList.sortWith(_._3 > _._3).take(3))

    resultRdd.collect.foreach(println)

  }
}

/**
  * 结果：
  * (android.learn.com,List((android.learn.com,http://android.learn.com/android/video.shtml,1)))
  (h5.learn.com,List((h5.learn.com,http://h5.learn.com/h5/course.shtml,2), (h5.learn.com,http://h5.learn.com/h5/teacher.shtml,1)))
  (ui.learn.com,List((ui.learn.com,http://ui.learn.com/ui/course.shtml,1), (ui.learn.com,http://ui.learn.com/ui/video.shtml,1)))
  (java.learn.com,List((java.learn.com,http://java.learn.com/java/video.shtml,2), (java.learn.com,http://java.learn.com/java/javaee.shtml,2)))
  (bigdata.learn.com,List((bigdata.learn.com,http://bigdata.learn.com/bigdata/teacher.shtml,4)))
  */
