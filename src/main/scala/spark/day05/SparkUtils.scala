package spark.day05

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

  /**
    * 获取sparkconf的配置
    * @return SparkConf
    */
  def getConf(appName:String, master: String):SparkConf={
    new SparkConf().setAppName(appName).setMaster(master)
  }


  /**
    * 获取spark的context
    * @param conf 配置
    * @return SparkContext
    */
  def getSparkContext(conf:SparkConf):SparkContext={
    new SparkContext(conf)
  }

  /**
    * 停止
    * @param sc
    */
  def stop(sc:SparkContext):Unit={
    sc.stop()
  }

  val hdfsFile = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/file"
  val filePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/src/main/scala/spark/day05/file/access.log"
}
