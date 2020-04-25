package day03.pattern_matching

object MatchFunction {
  def main(args: Array[String]): Unit = {
      m2("one")
    m1("two")
  }

  //偏函数匹配模式
  def m2:PartialFunction[String,Int]={
    case "one" => {println("输出的值1"); 1}
    case "two" => {println("输出的值2"); 2}
  }

  //一般匹配模式----->推荐
  def m1(str:String):Unit= str match {
    case "one" => {println("输出的值1")}
    case "two" => {println("输出的值2")}
  }

}


