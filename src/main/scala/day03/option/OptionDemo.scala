package day03.option

//Option封装多类型数据，不知道是否获取数据
object OptionDemo {
  def main(args: Array[String]): Unit = {
    val option = Some("name","lis")
    val data = option.getOrElse("没有找到参数")
    println(data)
  }
}
