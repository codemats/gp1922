package spark.day02

import org.apache.spark.{SparkConf, SparkContext}

object ActionTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Action算子").setMaster("local[2]")
    val sc = new SparkContext(conf)

    println("-----Action算子------")
    val reduceArr = Array(1,6,3,5,2,4)
    val reduceRdd = sc.makeRDD(reduceArr,1)
    println("reduce:"+reduceRdd.reduce(_ + _))
    // 21 (1+2+3+4+5+6)

    println("count:"+reduceRdd.count())
    //6

    print("top:") //获取最大的前4位
    reduceRdd.top(4).foreach(print)

    println()
    print("take:") //take:1635
    reduceRdd.take(4).foreach(print)

    println()
    print("takeOrdered:") //take:1234
    reduceRdd.takeOrdered(4).foreach(print)

    println()
    print("first:") //first:1
    println(reduceRdd.first())

    val countByKeyRdd = sc.makeRDD(List(("tom",20),("tom",21),("jreey",20),("hive",10)),9)
    print("countByKey:") //Map(hive -> 1, tom -> 2, jreey -> 1)
    println(countByKeyRdd.countByKey())

    print("countByValue:") //Map((hive,10) -> 1, (tom,20) -> 1, (tom1,21) -> 1, (jreey,20) -> 1, (tom,21) -> 1)
    println(countByKeyRdd.countByValue())

    //数据较少使用
    print("foreach:") //foreach:(tom,20)(tom,21)(jreey,20)(hive,10)
    countByKeyRdd.foreach(print)

    //数据较多时使用
    print("foreachPartition:") //foreach:(tom,20)(tom,21)(jreey,20)(hive,10)
    countByKeyRdd.foreachPartition(_.foreach(print))











  }
}
