package day03.`abstract`

object PsersonMain {
  def main(args: Array[String]): Unit = {
    val persion = PersonService()
    persion.sayHi("哈哈...")
    persion.run()
    persion.myName()
  }
}
