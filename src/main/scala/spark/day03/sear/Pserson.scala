package spark.day03.sear

import org.apache.spark.{SparkConf, SparkContext}
import spark.day03.sear.animal.Animal

//模拟对象传输，是否需要序列化
object Pserson {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Pserson").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(0,1,2))
    val tom =  Tom
    rdd.map(x=>{
      val dd  = tom
      println(dd)
      dd.isPerson(x)
    }).collect.foreach(println)
    sc.stop()
  }
}


/**
  * 上帝来判断是否是人
  */
trait Lord{

  /**
    * 判断是否是人
    * @param animalNum 动物编号
    * @return true 是 | false 不是
    */
  def isPerson(animalNum:Int):String

}

 //Exception in thread "main" org.apache.spark.SparkException: Task not serializable
 class Tom1  extends Serializable with Lord {
   /**
     * 判断是否是人
     *
     * @param animalNum 动物编号
     * @return true 是 | false 不是
     */
    @Override def isPerson(animalNum: Int):String = {
     animalNum match {
       case Animal.Person._1 => {
         val value = Animal.Person._2
         println("这是个："+value)
         value
       }
       case Animal.cat._1 => {
         val value = Animal.cat._2
         println("这是个："+value)
         value
       }
       case Animal.pig._1 => {
         val value = Animal.pig._2
         println("这是个："+value)
         value
       }
       case _ => {
         println("这特么是啥啊")
         "三界中不存在该物种"
       }
     }
   }
 }
