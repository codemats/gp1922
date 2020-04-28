package spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

//在定义累加器统计单词个数
object CustomerAccumulatorWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //在定义单词累加器
    val acc = new WordCountAccumulator
    //注册累加器
    sc.register(acc,"WordCountAccumulator")

    //文件路径
    val filePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/file/wordCount.txt"

    //文件读取
    val rdd:RDD[String] = sc.textFile(filePath)

    //循环统计单词个数
    rdd.collect.foreach((line:String) =>{

      //切分后统计
      line.split(" ").foreach((x:String)=>{

        //调用在定义累加器统计
         acc.add(x)
      })
    })

    //结果统计，结果：Map(hadoop -> 2, hive -> 1, kafka -> 1, hello -> 1, spool -> 1)
    println(acc.value)

    //停止
    sc.stop()
  }

}


//在定义单词统计累加器
class WordCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private var wordMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = wordMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new WordCountAccumulator()
    acc.wordMap = acc.wordMap.++(this.wordMap)
    acc
  }

  override def reset(): Unit = {
    this.wordMap.clear()
  }

  override def add(v: String): Unit = {
    this.wordMap.get(v) match {
      case Some(count) => this.wordMap.put(v, count+1)
      case None => this.wordMap.put(v, 1)
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = other match {
    case value: WordCountAccumulator => value.wordMap.++(this.wordMap)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: mutable.HashMap[String, Int] = this.wordMap
}


