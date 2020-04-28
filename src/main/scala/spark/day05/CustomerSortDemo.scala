package spark.day05

import org.apache.spark.rdd.RDD


//用户在定义排序
// 1.定义case样例类extends Ordered
// 2.覆写compare 的比较规则
// 3.排序arrData(返回必须是对象[1.定义case样例类extends Ordered的实例]).sortBy(x=>x).collect.foreach(println)
object CustomerSortDemo {

  def main(args: Array[String]): Unit = {
    val conf = SparkUtils.getConf(this.getClass.getName,"local[2]")
    val sc = SparkUtils.getSparkContext(conf)

    val rdd = sc.makeRDD(List("李梅 20 90","刘海 30 100","李海燕 27 120"))

    val arrData: RDD[Person] = rdd.map(item => {
      val arr = item.split(" ")
      val name = arr(0)
      val age = arr(1).toInt
      val fv = arr(2).toInt
      Person(name, age, fv)
    })

    //排序打印
    arrData.sortBy(x=>x).collect.foreach(println)
    //结果
    /**
      * Person(刘海,30,100)
        Person(李海燕,27,120)
        Person(李梅,20,90)
      */

    SparkUtils.stop(sc)
  }

}

//人物信息
case class Person(name:String,age:Int,fv:Int) extends Ordered[Person]{
  //实现比较信息
  override def compare(that: Person):Int = {
    //1 如果age相等，就比较fv
    if(this.age == that.age){
      return this.fv - that.fv
    }

    //2 比较age(降序)
    -(this.age - that.age)

  }
}



