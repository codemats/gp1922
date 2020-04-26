package spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//求所有用户经过的所有基站停留时长最长的top2
object Advert03 {

  //连接状态:连接
  val statusConnect:String = "1"
  val UserStationFilePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/src/main/scala/spark/day03/lacduration.txt"
  val stationFilePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/src/main/scala/spark/day03/lac_info.txt"



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advert03").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //用户停留基站信息
    val userStationResult: RDD[(String, (String, List[Long]))] = getUserStationInfo(sc)
    //基站信息
    val stationInfo:RDD[(String,(String,String,Int))] = getStationInfo(sc)
    //关联用户和基站信息
    val result: RDD[(String, ((String, List[Long]), (String, String, Int)))] = userStationResult.join(stationInfo)

    result.collect.foreach(item=>{
      val station = item._1
      val userPhoto = item._2._1._1
      val timeLong = item._2._1._2.last
      val long = item._2._2._1
      val lat = item._2._2._2
      val otherValue = item._2._2._3

      println(s"基站:${station},用户电话：${userPhoto},连接时长(ms):${timeLong},经度:${long},纬度:${lat},其他值:${otherValue}")

      //基站:9F36407EAD8829FC166F14DDE7970F68,用户电话：18101056888,连接时长(ms):6000,经度:116.304864,纬度:40.050645,其他值:6
      //基站:9F36407EAD8829FC166F14DDE7970F68,用户电话：18688888888,连接时长(ms):6200,经度:116.304864,纬度:40.050645,其他值:6
    })

    sc.stop()
  }


  //基站信息
  def getStationInfo(sc: SparkContext):RDD[(String,(String,String,Int))] = {

    val stationLines: RDD[String] = sc.textFile(stationFilePath)
    stationLines.map(x=>{
      val arr: Array[String] = x.split(",")
      val station = arr(0)
      val long = arr(1)
      val lat = arr(2)
      val value = arr(3).toInt

      (station,(long,lat,value))
    })

  }

  //用户停留基站信息
  def getUserStationInfo(sc:SparkContext):RDD[(String, (String, List[Long]))]={
    val lines: RDD[String] = sc.textFile(UserStationFilePath)
    val userStation: RDD[(String, Long)] = lines.map(item => {

      var arr = item.split(",")
      val userPhotot = arr(0)
      var time = arr(1).toLong
      val baseStation = arr(2)
      val status = arr(3)

      if (status.equals(statusConnect)) {
        time = -time
      }
      (userPhotot + "_" + baseStation, time)
    })

    val groupUserStation: RDD[(String, Iterable[Long])] = userStation.reduceByKey(_+_).groupByKey()

    val userStationResult: RDD[(String, List[Long])] = groupUserStation.mapValues(x=>x.toList.sortWith(_ > _).take(3))
    userStationResult.map(x=>{
      val arrUserSatationInfo = x._1.split("_")
      (arrUserSatationInfo(1),(arrUserSatationInfo(0),x._2))
    })
  }
}
