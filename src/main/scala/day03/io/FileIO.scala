package day03.io

import scala.io.{Source}

//读取路径文件内容
object FileIO {

  def main(args: Array[String]): Unit = {
    val filePath = "./word.txt"
    val pathSource = Source.fromFile(filePath)
    val iterator = pathSource.getLines()
    while(iterator.hasNext){
      println(iterator.next())
    }
  }
}
