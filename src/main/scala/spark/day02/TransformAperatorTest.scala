package spark.day02

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

//Transform算子练习
object TransformAperatorTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Transform算子练习").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /*val arr = Array(1,2,3,4,5,6)
    //RDD元素*2，倒序
    arr.map(_*2).sortWith((x,y)=>x > y).foreach(println)
    //过滤大于3的元素
    println("过滤大于3的元素")
    arr.filter(x=> x <= 3).foreach(println)

    //切分后压平
    println("切分后压平")
    val arr2 = Array("hello hadoop spark h a c")
    arr2.flatMap(_.split(" ")).foreach(println)

    //大list里面有小list，切分后压平
    println("大list里面有小list，切分后压平")
    val list1 = List(List("hadoop hello"),List("tom cat jreey"),List("map split lili"))
    list1.flatMap(_.flatMap(_.split(" "))).foreach(println)

    //并集
    println("-------并集-------")
    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))
    val rdd2 = sc.makeRDD(Array(4,5,6,7,8,9))
    rdd1.union(rdd2).foreach(println)

    //交集
    println("-------交集-------")
    rdd1.intersection(rdd2).foreach(println)

    //去重
    println("-------去重--------")
    val list2 = List("tom","jerry","tom","tom")
    list2.distinct.foreach(println)

     //join
    println("---------join--------")
    val rddJoin1  = sc.makeRDD(List(("tom",19),("jreey",1),("jack",20)))
    val rddJoin2  = sc.makeRDD(List(("tom",20),("jreey",19),("lili",20)))

    rddJoin1.join(rddJoin2).collect.foreach(print)
    //(tom,(19,20))(jreey,(1,19))

    println("---------leftOuterjoin--------")
    val rddJoin1  = sc.makeRDD(List(("tom",19),("jreey",1),("jack",20)))
    val rddJoin2  = sc.makeRDD(List(("tom",20),("jreey",19),("lili",20)))

    rddJoin1.leftOuterJoin(rddJoin2).collect.foreach(print)
    //(tom,(19,Some(20)))(jack,(20,None))(jreey,(1,Some(19)))

    println("------groupBy---------")
    val groupByRDD = sc.makeRDD(List(("tom",1),("tom",3),("tom",5),("hadoop",2),("hive",4),("spark",6)))
    groupByRDD.groupBy(x=>x._1).collect.foreach(print)
    //(hive,CompactBuffer((hive,4)))(tom,CompactBuffer((tom,1), (tom,3), (tom,5)))(spark,CompactBuffer((spark,6)))(hadoop,CompactBuffer((hadoop,2)))

    val groupByRDD = sc.makeRDD(List(("tom",1),("tom",3),("tom",5),("hadoop",2),("hive",4),("spark",6)))
    groupByRDD.groupByKey().collect.foreach(print)
    //(hive,CompactBuffer(4))(tom,CompactBuffer(1, 3, 5))(spark,CompactBuffer(6))(hadoop,CompactBuffer(2))


    println("--------cogroup------------")
    val coGroupRdd = sc.makeRDD(List(("tom",1),("jreey",2),("spark",3),("tom",4)))
    coGroupRdd.cogroup(coGroupRdd).collect.foreach(print)
    //(tom,(CompactBuffer(1, 4),CompactBuffer(1, 4)))(spark,(CompactBuffer(3),CompactBuffer(3)))(jreey,(CompactBuffer(2),CompactBuffer(2)))


     println("--------map---mapPartitions-----")
    val listMap = sc.makeRDD(List(1,2,3,4,5,6))
    listMap.map(x=>x*10).collect.foreach(print)
    //102030405060  数据不多，可以使用
    println()
    listMap.mapPartitions(_.map(x=>x)).collect.foreach(print)
    //123456  数据过多采用

    println("-----重分区算子-----")
    //repartition底层：coalesce(numPartitions, shuffle = true(默认shuffle))
    var partitionRdd = sc.makeRDD(List(1,2,3,4,5,6),5)
    println("初始化之前的分区："+partitionRdd.partitions.size)

    val rePartitionRdd = partitionRdd.repartition(10)
    println("分区大10："+rePartitionRdd.partitions.size)

    val minPartitionRdd = rePartitionRdd.repartition(3)
    println("分区小3："+minPartitionRdd.partitions.size)


    val aggRdd = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9))
    val tuple = aggRdd.aggregate((0,0))((x,y)=>(x._1+y,x._2+1),(a,b)=>(a._1+b._1,a._2+b._2))
    println(tuple)

    val aggRdd2 = sc.makeRDD(List(("tom",20),("tom",21),("jreey",20),("hive",10)))
    aggRdd2.aggregateByKey(0)((x,y)=>x+y,(a,b)=>a+b).collect.foreach(println)
    //第2中方式：aggRdd2.groupByKey().map(x=>(x._1,x._2.reduce(_+_))).collect.foreach(println)
    //(hive,10)(tom,41)(jreey,20)


    val aggRdd2 = sc.makeRDD(List(("tom",20),("tom",21),("jreey",20),("hive",10)))
    aggRdd2.combineByKey(
      n=>n, //返回值类型
      (a:Int,b:Int)=>a+b, 线程内聚合，注意：需要加上参数类型
      (x:Int,y:Int)=>x+y  全局聚合，注意：需要加上参数类型
    ).collect.foreach(println)
    //结果
    (hive,10)
    (tom,41)
    (jreey,20)
    */
  }
}
