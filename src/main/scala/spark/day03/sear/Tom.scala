package spark.day03.sear

import spark.day03.sear.animal.Animal

object Tom extends Serializable {
    /**
      * 判断是否是人
      *
      * @param animalNum 动物编号
      * @return true 是 | false 不是
      */
    def isPerson(animalNum: Int):String = {
      animalNum match {
        case Animal.Person._1 => {
          val value = Animal.Person._2
          println("这是个："+value)
          value
        }
        case Animal.cat._1 => {
          val value = Animal.cat._2
          println("这是个："+value)
          value
        }
        case Animal.pig._1 => {
          val value = Animal.pig._2
          println("这是个："+value)
          value
        }
        case _ => {
          println("这特么是啥啊")
          "三界中不存在该物种"
        }
      }
    }
}
