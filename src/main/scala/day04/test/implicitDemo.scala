package day04.test

object implicitDemo {


  def main(args: Array[String]): Unit = {

    import MyPredef._
    val pserson = new Persons("哈哈...你大爷还是你大爷")
    pserson.say()

    "我是世界第一....".sayHello()

  }
}

class Persons(message:String){

  def say()(implicit  userName:String): Unit ={
    println(s"${userName}向大家说好，内容是：${message}")
  }

  def sayHello():Unit={
    println(s"哈哈，谁是这里的老大，我老大说:${message}")
  }

}
