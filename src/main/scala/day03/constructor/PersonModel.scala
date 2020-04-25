package day03.constructor

//主类
//主类后面(var(可改变)|val(不可改变) 参数名称：参数类型 或者 参数名称：参数类型=初始值(该类型默认是val))就是主构造器
class PersonModel(var name:String,var id:String,val sorce:Int,faceValue:Int=60) {

  //新加字段
  var address:String =_

  //辅助构造器
  def this( name:String, id:String,sorce:Int,address:String){
    //第一行必须是主构造器
    this(name,id,sorce)
    //下面才是新字段副值操作或者其他业务操作
    this.address = address
  }

  //覆写toSring
  override def toString: String = {
    s"name:${name},id:${id},sorce:${sorce},faceValue:${faceValue},address:${address}"
  }

}

object PersonModel{ //在没有主类的时候就是单例对象
  //方法注入
  def apply(name: String, id: String, sorce: Int, address: String): PersonModel = {
    println("PersonModel apply方法运行...")
    new PersonModel(name, id, sorce, address)
  }

  //提取数据
  def unapply(personModel: PersonModel): Option[(String, String, Int, String)] = {
    if (personModel == null) None
    else
      Some(personModel.name,personModel.id,personModel.sorce,personModel.address)
  }
}
