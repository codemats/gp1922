package day03.define_class

import java.util.UUID

object StudentMain {

  def main(args: Array[String]): Unit = {
    val student = new StudentMode()
    student.id = UUID.randomUUID().toString.replaceAll("-","")
    student.name = "李四"
    //val修饰的不能赋值，只有get方法
    //student.school = "ww"

     //age 为私有对象，该不是其半生对象，无法访问
     //student.age
    println(student.toString)
  }
}
