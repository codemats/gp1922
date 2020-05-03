package spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

//通过NC统计单词个数
object SparkStreamingByNCWC {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)

    var count = 0
    //创建stream的操作对象
    val ssc = new StreamingContext(conf,Seconds(2))
    val ncStram: ReceiverInputDStream[String] = ssc.socketTextStream("172.16.109.132",9999)

    //单词统计

    //使用DStream操作
    //val result: DStream[(String, Int)] = ncStram.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //使用RDD操作
    val result = ncStram.transform(rdd=>{
      //以下RDD操作数据
      val tmpArr: RDD[Array[String]] = rdd.map(line => line.split(" "))
      val tmpArrmap: RDD[Array[(String, Int)]] = tmpArr.map(x => {x.map((_, 1))})
      val result: RDD[Map[String, Int]] = tmpArrmap.map(line => {
        val arrGroup: Map[String, Array[(String, Int)]] = line.groupBy(_._1)
        arrGroup.map(x => (x._1, x._2.size))
      })
      result
    })
    result.transform(rdd=>{
      if(rdd.count() > 0){
        count+=1
        println("-------正在保存数据--------")
        println(s"数据:${rdd}")
        rdd.saveAsTextFile(s"/Users/a10.12/Documents/study/bigdata/spark/gp1922/file/ii/${count}")
        println("-------保存数据成功--------")
      }
      rdd
    }).print()

    ssc.start()  //启动任务提交
    ssc.awaitTermination() //等待终止-关闭
  }
}
