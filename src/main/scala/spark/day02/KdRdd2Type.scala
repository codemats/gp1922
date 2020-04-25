package spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD创建的2种方式
  */
object KdRdd2Type {

  //文件路径
  val filePaht = "file/wordCount.txt"

  def main(args: Array[String]): Unit = {

    //1 磁盘数据中
    var conf = new SparkConf().setAppName("mkRDDType").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdds:RDD[String] = sc.textFile(filePaht)
    rdds.foreach(println)
    //文件内容
    //hadoop hive spool
    //hello hadoop kafka

    //2 数据在内存中
    val intRdd:RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    val rdds1 = sc.makeRDD(Array(1,2,3,4,5))
    println(rdds1.partitions.size)
    /*
    makeRDD 底层默认调用parallelize,默认分区：2个
      def makeRDD[T: ClassTag](
        seq: Seq[T],
        numSlices: Int = defaultParallelism): RDD[T] = withScope {
         parallelize(seq, numSlices)
      }
     */






  }
}
