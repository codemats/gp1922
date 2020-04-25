package day04.implicit_demo

object RichFileMain {
  def main(args: Array[String]): Unit = {
    val filePath = "/home/yangheng/IdeaProjects/bigdata/spark/gp1922/word.txt"

    //引入隐式方法   使用对应的【参数.方法】
    import MyPredef._
    val fileContent = filePath.read()
    print(fileContent)
    println(3.add())

    println(addAge(9))
  }


  //上面已经引入   import MyPredef._（里面定义隐式参数：implicit val age:Int = 10）
  // implicit val age:Int = 10 这个age=10会覆盖implicit b:Int ，也即 b = 10
  def addAge(a:Int)(implicit b:Int):Int={
    println(s"b的参数：${b}")
    a+b
  }
}
