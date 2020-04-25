package day03.`trait`

//特质--和java的接口类似，方法可以有方法体，也可以没有方法
trait FlyCatTrait {
  //有值字段
  val name:String = "lisi"
  //无值字段
  val age:Int
  //有方法体的方法
  def sayHello():String={
    "hello word"
  }
  //无方法体的方法
  def sayHi():Unit
}
