package day03.app

//在object对象里面不需要写main方法，只需要继承App，就可以运行代码，作为
//程序的入口

//单例对象
/*object AppRunTest extends App {
  val userName = scala.io.StdIn.readLine("请输入姓名：")
  println(s"您输入的姓名是：${userName}")
}*/

object AppRunTest{
  def main(args: Array[String]): Unit = {
    val userName = scala.io.StdIn.readLine("请输入姓名：")
    println(s"您输入的姓名是：${userName}")
  }
}
