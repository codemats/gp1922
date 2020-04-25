package day03.`trait`

class FlyCatEntity  extends FlyCatTrait {
  override val age: Int = 90
  override def sayHi(): Unit = println("飞猫说话拉")
}

object FlyCatEntity{
  //注入方法
  def apply(): FlyCatEntity = new FlyCatEntity()
}
