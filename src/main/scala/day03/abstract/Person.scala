package day03.`abstract`

//抽象类
abstract class Person {
  /**
   * 说话
   * @param message 说话的内容
   */
   def sayHi(message:String):Unit={
     this.myName()
     println(s"我向大家说：${message}")
   }

  /**
   * 我的名字
   */
  def myName():Unit;
}
