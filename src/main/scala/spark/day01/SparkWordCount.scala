package spark.day01

import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

//单词统计--读取hdfs文件
object SparkWordCount {


  def main(args: Array[String]): Unit = {

    val hdfsFilePath = "hdfs://mac-yangheng:9000/file/study/word.txt"
    val appName = "SparkWordCount"
    val conf = new SparkConf().setAppName(appName)
    .setMaster("local[2]")  //集群模式需要注释掉，否则已本地模式运行
    val sc = new SparkContext(conf)

    //读取hdfs文件
    val lines: RDD[String] = sc.textFile(hdfsFilePath)
    //数据压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //组装数据（word,1）
    val wordTuple: RDD[(String, Int)] = words.map((_, 1))
    //按照key统计单词个数
    val value: RDD[(String, Int)] = wordTuple.reduceByKey(_ + _).sortBy(_._2,false)
    //保存到hdfs
    //value.saveAsTextFile("hdfs://mac-yangheng:9000/file/study/word1.txt")
    //收集统计的结果
    val tuples: Array[(String, Int)] = value.collect()
    //打印结果
    for (elem <- tuples) {
      println(elem)
    }
  }
}
