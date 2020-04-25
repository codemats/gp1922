package day03.constructor

object MainTest {

  def main(args: Array[String]): Unit = {
    //使用new是主类的实例化
    val person =  new PersonModel("lisi","1",90,"北京")

    //不使用new是半生对象实例化主类，半生对象中必须有apply方法初始化主类对象
    //val person =  PersonModel("lisi","1",90,"北京")
    /**
     * object PersonModel{
     * def apply(name: String, id: String, sorce: Int, address: String): PersonModel =
     * new PersonModel(name, id, sorce, address)
     * }
     */
    println(person.toString)
  }
}
