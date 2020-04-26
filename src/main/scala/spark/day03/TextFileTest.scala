package spark.day03

import org.apache.spark.{SparkConf, SparkContext}

object TextFileTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("textFile").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.textFile("文件路径")//分片数： def defaultMinPartitions: Int = math.min(defaultParallelism(local[这里数值]), 2)
    sc.stop()


  }
}
