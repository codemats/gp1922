package day04.implicit_demo

import day04.implicit_demo.file.RichFile

object MyPredef {

   //隐式方法,调用方法为：参数.方法   【filePath.readFileContent()】
   @inline implicit def readFileContent(filePath:String):RichFile={
     new RichFile(filePath)
   }

  @inline implicit def add(a:Int):RichFile={
    new RichFile(a,"")
  }

  //定义隐式常量，需要使用的地方引入参数即可
  implicit val age:Int = 10
}
