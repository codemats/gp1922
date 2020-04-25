package day02

import java.io.{File, PrintWriter}

object Day02 {
 def main(args:Array[String]):Unit={
   //定长数组
 // val arr = Array(1,2,3,4,5,6,7)
//  for (item<- arr) println(item)
 // val arr1 = Array(8,9)
  //合并数组
 // val array = Array.concat(arr, arr1)
 // println(array.toBuffer)
  /**
   * var arr3 = new Array[Int](2)
   * arr3(0) = 1
   * arr3(1) = 10
   * println(arr3.toBuffer)
   */

  /**
   * 变长数组
   * val arr4 = ArrayBuffer[String]()
   * //数据追加
   * arr4.append("lisi")
   * arr4.append("wangwu")
   * arr4+="zhaoliu"
   * arr4.insert(0,"wangmazi")
   * println(arr4.toBuffer)
   * //删除
   * arr4.remove(0)
   * println(arr4.toBuffer)
   * var arr5 = Array(3,5,1,2,4)
   * println(arr5.sortBy(x => x).toBuffer) // 排序小-大
   * println(arr5.sortBy(x => x).reverse.toBuffer)//排序大-小
   *
   * println(arr5.sortWith((a, b) => a > b).toBuffer)//排序大-小
   * println(arr5.sortWith((a, b) => a < b).toBuffer)//排序小-大
   */

  //map
  /**
   * 不可変map
   * var map1 = Map(("name","李四"),("age","20"),("school","清华大学"))
   * println(map1("name"))
   * for ((k,v)<- map1) println(s"key:${k},value:${v}")
   * println(map1.getOrElse("name1", "没有值"))
   *
   * 可变
   * var map2 = mutable.HashMap("name"->"lisi")
   * map2.put("age","20")
   * map2.put("school","北京大学")
   * for (elem <- map2) {
   * println(elem._1+","+elem._2)
   * }
   * println("----------------------------------")
   * val map = map2.+("ff" -> "ss")
   * for (elem <- map) {println(elem._1+","+elem._2)}
   */

  //元组tuple
  /**
   * var tup =(1,2,3,4,5)
   * print(tup._4)
   *
   * val tup1 = new Tuple2(1,2) //2个元素的tuple
   * println(tup1)
   * //取值
   * // 取值是第二个值，从1开始起点：tup1._2
   * println(tup1._2)
   */

  /**
   * 拉链 zip 多个数组合并成元组 array
   * var arr1 = Array(1,2,3,4)
   * var arr2 = Array(5,6,7,8)
   *
   * val tuples = arr1.zip(arr2)
   * println(tuples.toBuffer)
   * //结果：ArrayBuffer((1,5), (2,6), (3,7), (4,8))
   * println(tuples.map(x => x._1 + x._2).toBuffer)
   * //结果：ArrayBuffer(6, 8, 10, 12)
   */

  /**
   * list不可变
   * var list = List(1,2,3,4,5)
   * for (elem <- list) {println(elem)}
   *
   * println("=====================================")
   * var list1 = 9::list //9插入list的第一个位置
   * for (elem <- list1) {println(elem)}
   * println("==============list.+(\"111\")=======================")
   * var list2 = list.+:(90)
   * println(list2.toBuffer)
   *
   * var list4 = list.:::(List(9,8,7))
   * println(list4)
   *
   * 可变
   * var listBuffer = ListBuffer[Int](1,2,3,4)
   * //删除
   *   listBuffer.remove(1) //ArrayBuffer(1, 3, 4)
   * println(listBuffer.toBuffer)
   * //添加元素
   *   listBuffer.insert(listBuffer.size,5)
   * println(listBuffer)
   * //更新元素
   *   listBuffer.update(0,10)
   * println(listBuffer)
   */

  /**
   * set不可变
   * var set = scala.collection.immutable.Set(1,2,3)
   * for (elem <- set) {println(elem)}
   *
   * val set1 = new mutable.HashSet[Int]()
   * //加
   * set1.add(2)
   * set1.add(3)
   * //Set(2, 3)
   * //减
   * set1.remove(2)//Set(3)
   * println(set1)
   * //更新
   * set1.update(34, true)
   * set1.update(35, true)
   * println(set1)
   */

  //案例：计算单词个数
  var write = new PrintWriter(new File("./word.txt"))
  val wordsList = List("hello hadoop spark","hive kafka sql mysql","springboot springMvc hibernate hadoop")
  val s =  wordsList
    //数据压平
    .flatMap(_.split(" "))
    //转换tuple（单词，1）
    .map((_,1))//List((hello,1), (hadoop,1), (spark,1), (hive,1), (kafka,1), (sql,1), (mysql,1), (springboot,1), (springMvc,1), (hibernate,1), (hadoop,1))
    //单词分组
    .groupBy(_._1)//Map(sql -> List((sql,1)), springboot -> List((springboot,1)), kafka -> List((kafka,1)), springMvc -> List((springMvc,1)), hadoop -> List((hadoop,1), (hadoop,1)), spark -> List((spark,1)), hive -> List((hive,1)), hibernate -> List((hibernate,1)), mysql -> List((mysql,1)), hello -> List((hello,1)))
    //统计单词个数
    .map(x=>(x._1,x._2.size))//Map(sql -> 1, springboot -> 1, kafka -> 1, springMvc -> 1, hadoop -> 2, spark -> 1, hive -> 1, hibernate -> 1, mysql -> 1, hello -> 1)
  //循环打出结果
  /*s.foreach(x=>{
   val result = x._1+","+x._2 ;
   write.write(result)
   write.write("\n")
   println(result)
  })
  write.close()
  println("单词统计写入文件完成，文件路径：./word.txt")*/

  //练习---并行计算

  /*//但线程累加
  var arr = Array(1,2,3,4,5,6,7,8,9,10)
  val signSum = arr.reduce((a,b)=>a+b)
  println("单线程累加："+signSum)
  //多线程累加
  val resultSum = arr.par.reduce((a, b) => {
   println(Thread.currentThread().getName+s"：操作a:${a},b:${b}")
   a + b
  })
  //val resultSum = arr.par.reduceLeft((a, b) => a + b) //使用特定顺序方法时都是但线程操作
  println("多线程累加："+resultSum)

  //折叠
  println(arr.fold(0)((a, b) => a + b)) //55

   聚合--求和
   var arr1 = List(List(1,2,3),List(4,5,6),List(7,8,9))
  var sum1 = arr1.reduce((a,b)=>List(a.sum+b.sum))
  println(sum1)

  //var sum2 = arr1.aggregate(0:初始值)((a,b)=>a+b.sum：线程内求和,(a,b)=>a+b：线程之间求和)
  var sum2 = arr1.aggregate(0)((a,b)=>a+b.sum,(a,b)=>a+b)
  println(sum2)

  println(arr1.flatten[Int].sum)
  */

  /**
   * 需求：
   *   求聚合，结果为一个元组：（arr2的总和，参与计算的个数）== (45,9)
   *   var arr2 = Array(1,2,3,4,5,6,7,8,9)
   *
    * val arr2 = Array(1,2,3,4,5,6,7,8,9)
    * val sum3 = arr2.aggregate((0,0))((a,b)=>(a._1+b,a._2+1),(a,b)=>(a._1+b._1,a._2+b._2))
    * println(sum3)
   */

  /**
   * 求并集，交集，差集
   * var l1 = List(1,2,3)
   * var l2 = List(2,3,4)
   * println(l1.union(l2))
   * println(l1.intersect(l2))
   * println(l1.diff(l2))
   */

  //求聚合，结果为一个元组：（arr2的总和，参与计算的个数）== (45,9)
  var arr2 = Array(1,2,3,4,5,6,7,8,9)
  val tuple = arr2.aggregate((0,0))((a,b)=>(a._1+b,a._2+1),(x,y)=>(x._1+y._1,x._2+y._2))
  println(tuple._1+","+tuple._2)
 }
}
