package day03.closure

//闭包demo2
object ClosureDemo2 {
  def main(args: Array[String]): Unit = {
     val s = sum(x=>x)
     println(s(1,2))
  }

  def sum(f:Int=>Int):(Int,Int)=>Int={
    def sumf(a:Int,b:Int):Int={
      if(a>b) 0
      else
         f(a)+sumf(a+1,b)
    }
    sumf
  }
}
