package spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

//需求二：统计每个省份每个小时的广告ID的top3
//时间戳         省份      城市      用户id  广告id
//1562085629599,Hebei,Shijiazhuang,564,1

/**
    (Hebei_0,List((9,1), (1,1)))
    (Jiangsu_0,List((6,1)))
    (Hubei_0,List((2,1)))
    (Hunan_0,List((6,1), (4,1)))
  */
object Advert02 {

  def main(args: Array[String]): Unit = {

    val filePaht = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/src/main/scala/spark/day03/Advert.log"

    val conf = new SparkConf().setAppName("Advert02").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines: RDD[Array[String]] = sc.textFile(filePaht).map(_.split(","))
    val proTimeAdvIdCount: RDD[(String, Int)] = lines.map(item => {
      (item(1) + "_" + getTimeStr(item(0)) + "_" + item(4), 1)
    })
    val proTimeAdvIdCountSum: RDD[(String, Int)] = proTimeAdvIdCount.reduceByKey(_+_)

    val groupValues: RDD[(String, Iterable[(String, Int)])] = proTimeAdvIdCountSum.map(x => {
      val arr: Array[String] = x._1.split("_")
      (arr(0) + "_" + arr(1), (arr(2), x._2))
    }).groupByKey()

    val result: RDD[(String, List[(String, Int)])] = groupValues.mapValues(x=>x.toList.sortWith(_._2 > _._2).take(3))

    result.collect.foreach(println)


    sc.stop()
  }

  /**
    * 获取时间小时（通过时间戳）
    * @param time
    * @return
    */
  def getTimeStr(time:String):String={
    val longTime: Long = time.toLong
    new DateTime(longTime).getHourOfDay.toString
  }
}


