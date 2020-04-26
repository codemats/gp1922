package spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//统计每个省份点击广告ID的top3
//1562085629599,Hebei,Shijiazhuang,564,1
object Advert01 {

  def main(args: Array[String]): Unit = {
    val filePaht = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/src/main/scala/spark/day03/Advert.log"

    val conf = new SparkConf().setMaster("local[2]").setAppName("Advert01")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(filePaht)
    val lineArrs: RDD[Array[String]] = lines.map(x=>x.split(","))
    val values: RDD[(String, Int)] = lineArrs.map(x=>(x(1)+"_"+x(4),1))

    val proAdvCount: RDD[(String, (String,String, Int))] = values.reduceByKey(_ + _).map(x => {
      val proAdvId: Array[String] = x._1.split("_")
      (proAdvId(0), (proAdvId(0),proAdvId(1), x._2))
    })
    val result: RDD[(String, List[(String, String, Int)])] = proAdvCount.groupByKey().mapValues(x=>x.toList.sortWith(_._3 > _._3).take(3))
    result.collect.foreach(println)




    sc.stop()


  }

}
