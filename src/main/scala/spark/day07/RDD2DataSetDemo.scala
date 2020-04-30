package spark.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//RDD---->DataSet
object RDD2DataSetDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val rdd: RDD[String] = sc.makeRDD(List("lisi-203-34"))

    val personRDD: RDD[Person1] = rdd.map(line => {
      val arr = line.split("-")
      Person1(arr(0), arr(1), arr(2))
    })

    import spark.implicits._

    //转换：RDD.toDS()
    val dataSet: Dataset[Person1] = personRDD.toDS()
    dataSet.show()

    spark.stop()
    sc.stop()
  }
}
case class Person1(name:String,age:String,score:String)

