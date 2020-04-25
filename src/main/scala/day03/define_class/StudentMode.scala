package day03.define_class

//

//主类
class StudentMode {
 //字段定义 var 修饰 有get，set方法
  var id:String = _
  var name:String = _

  //字段定义 val 修饰 有get方法
  val school:String = "清华大学"

  //private 修饰的字段只能在本类和半生对象中使用
  private  var age:Int = 90

  //private [this]修饰的字段只能在本类中使用
  private [this] var score:Int = 100

  //定义方法，提供外部类使用该私有字段
  def setAge(age:Int):Unit={
     this.age = age
  }

  //覆写toString
  override def toString: String = s"id=${id},name=${name},school=${school},age=${age}"
}

//半生对象
object StudentMode{ // 有 objet（StudentMode）修饰和主类名一样的话，称为“StudentMode”的伴生对象

  var stu = new StudentMode
  //私有字段private  var age:Int = 90 可以在其半生对象使用
  stu.age = 200 ;

  //字段private [this](修饰) var score:Int = 100  只能在本类中使用
 // stu.score
}
