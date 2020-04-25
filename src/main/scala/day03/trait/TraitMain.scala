package day03.`trait`

object TraitMain {
  def main(args: Array[String]): Unit = {
    //调用半生对象里的apply方法注入，实体对象
    val fCat = FlyCatEntity()
    fCat.sayHi()
    println(fCat.sayHello())
    println(fCat.name)
    println(fCat.age)
  }
}
