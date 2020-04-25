package spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 其他算子
  */
object OtherApertoTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("OtherApertoTest")
    val sc = new SparkContext(conf)

    //val rdd = sc.makeRDD(List(("tom",20),("tom",21),("jreey",20),("hive",10)))
    /*println("filterByRange")
    val rdd = sc.makeRDD(List(("a",20),("b",21),("c",20),("d",10),("l",90)))
    rdd.filterByRange("a","f").collect.foreach(print)
    //(a,20)(b,21)(c,20)(d,10)    2个字母的范围过滤  */

    // println("flatMapValues")
    // val flatMapValuesRdd = sc.makeRDD(List(("tom","23 12"),("lili","100 90")))
    // flatMapValuesRdd.flatMapValues(_.split(" ")).collect.foreach(print)
    //(tom,23)(tom,12)(lili,100)(lili,90)

    /*
    println("foldByKey")   //(hive,10)(tom,41)(jreey,20)
    val rdd = sc.makeRDD(List(("tom", 20), ("tom", 21), ("jreey", 20), ("hive", 10)))
    println("--------foldByKey--------")
    rdd.foldByKey(0)((x, y) => x + y).collect.foreach(println)

    println("--------reduceByKey--------")  //(hive,10)(tom,41)(jreey,20)
    rdd.reduceByKey(_ + _).collect.foreach(println)

    println("--------aggregateByKey--------")
    rdd.aggregateByKey(0)((x, y) => x + y, (a, b) => a + b).collect.foreach(println)

    println("--------groupByKey map reduce--------")  //(hive,10)(tom,41)(jreey,20)
    rdd.groupByKey().map(x=>(x._1,x._2.reduce(_+_))).collect.foreach(print)

    println("--------keyBy--------")  //(20,(tom,20))(20,(tom,20))(20,(jreey,20))(10,(hive,10))
    val rdd1 = sc.makeRDD(List(("tom", 20), ("tom", 20), ("jreey", 20), ("hive", 10)))
    rdd1.keyBy(x => x._2).collect.foreach(print)

    println("--------keys--------")
    rdd1.keys.collect.foreach(print)

    println("--------values--------")
    rdd1.values.collect.foreach(print)

    println("--------keys--------") //key:jreey,value:20 key:tom,value:20 key:hive,value:10
    for (elem <- rdd1.collectAsMap()) {
      print(s"key:${elem._1},value:${elem._2} ")
    }
    */
  }
}
