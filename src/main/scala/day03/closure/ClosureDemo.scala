package day03.closure

//闭包:可以访问一个函数里面局部变量的另外一个函数。
object ClosureDemo {

  def main(args: Array[String]) {
    println( "muliplier(1) value = " +  multiplier(1) )
    println( "muliplier(2) value = " +  multiplier(2) )
  }
  var factor = 3
  val multiplier = (i:Int) => i * factor
}
