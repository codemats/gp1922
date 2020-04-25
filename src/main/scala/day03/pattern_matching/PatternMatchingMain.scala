package day03.pattern_matching

import scala.util.Random


//模式匹配
object PatternMatchingMain {

  def main(args: Array[String]): Unit = {
    val patten = Array("lisi",20,true,90L,Teacher("lisi",20))
    //随机获取array里面的其中一个元素类型进行匹配
    val par = patten(Random.nextInt(patten.size))
    par match{
        //string
      case "lisi" => println("lisi")
        //Int
      case 20 => println("20")
        //Boolean
      case true => println("true")
        //Long
      case 90L => println("90L")
        //对象
      case Teacher(name,age) =>println("Teacher:"+name+",age:"+age)
        //默认前面没有匹配到
      case _ => println("没有匹配到...")
    }
  }
}
case class Teacher(var name:String,var age:Int)
