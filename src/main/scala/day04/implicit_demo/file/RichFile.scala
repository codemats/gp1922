package day04.implicit_demo.file

import scala.io.Source

//文件内容读取
class RichFile(filePath:String) {

  var a:Int = _

  def this(a:Int,filePath:String){
    this(filePath)
    this.a = a
  }

  def add():String={
    if (a % 2 == 0) "是偶数"
    else "是奇数"
  }

  def read():String={
    Source.fromFile(filePath).mkString
  }
}
