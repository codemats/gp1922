package day03.`abstract`

//在没有继承父类的情况下，如果要实现trait，此时的关键词“extends”
//在有父类的情况下，如果要实现trait，此时的关键词“with”
class PersonService extends Person with PersonTrait {
  /**
   * 我的名字
   */
  override def myName(): Unit = println("王麻子")

  /**
   * 跑的方法
   */
  override def run(): Unit = {
     println("只要是人都会跑？？")
  }
}

object PersonService{

  //方法注入
  def apply(): PersonService = new PersonService()
}
