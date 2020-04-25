package day01

object Day01 {

  def main(args:Array[String]):Unit = {
    //    println("-------------------var----------------------")
    //    var name:String = "lisi"
    //    name = "wangwu"
    //    println(s"var定义的变量name:${name}")

    //    println("--------------------val---------------------")
    //    val userName:String = "王麻子"
    //    //userName = "李老双" // val定义的是常量，不能改变;var定义的是变量，可以改变
    //    println(s"userName:${userName}")

    //    println("--------------------类型自动推断---------------------")
    //    val age = 29
    //    println(age.getClass.getName+s"age:${age}")

    //    println("--------------------多定义变量---------------------")
    //    val (a,b,c) = (1,2,"as") //  类型自动推断
    //    println(a,b,c)
    //
    //    println("--------------------多定义变量---------------------")
    //    // 基本数值类型： Byte,Char,Short,Int Long float Double
    //    // 非数值类型：  Boolean Unit
    //
    //    println("--------------------if条件---------------------")
    //    val schoolCode:Int = 0 ;
    //    val schoolName = if(schoolCode == 0) "北京大学" else "其他大学"
    //    println(s"你的大学是:${schoolName}")
    //
    //    println("--------------------if条件返回空值()---------------------")
    //    val x = 0 ;
    //    var z = if(x > 1) 1 else ()
    //    println(s"z:${z}")
    //
    //    println("--------------------if条件块表达式---------------------")
    //    val y = 0 ;
    //    val result = {
    //      if(y == 0) ("我是大哥")
    //      else if(y > 1) ("你是大哥")
    //      else ("他才是大哥")
    //    }
    //    println(s"谁是大哥：${result}")
    //  }
    //
    //  println("--------------------定义 key1=a,key2=b,key3=c---------------------")
    //  val prop = "key1=a,key2=b,key3=c"
    //  println(prop)
    //
    //  val str =
    //    """|key1=a
    //      |key2=b
    //      |key3=c
    //      |""".stripMargin
    //
    //  str.split("\n").foreach(x=> println(x+"AAAAA"))

    // println("--------------------键盘输入---------------------")
    //  val ageString: String = scala.io.StdIn.readLine("请输入你的年龄:")
    //  if(ageString.toInt <0 ) {
    //    println("你的年龄不符合，禁止入内")
    //  }else{
    //   println("欢迎光临....")
    //  }

    //  println("--------------------for-打印1到10--------------------")
    //  for (num <- 1 to 10) println(num)

//    println("--------------------for-打印1到10（不含10）--------------------")
//    for (num <- 1 until 10) println(num)

//    println("--------------------for-打印 高级for--------------------")
//    for (i<- 1 to 10 ;j<- 1 until 10 if i==j) println(i+j)

//    println("--------------------break例子，scala里面没有像java一样有break关键词--------------------")
//    import util.control.Breaks._
//    breakable(
//      for (item <- 1 to 10){
//        if (item == 8){
//          println(s"循环的次数[${item}]到了，可以跳出循环啦")
//          break()
//        }
//      }
//    )

   // println("--------------------方法的申明-------------------")


    /**
     * 格式：def 方法名称(参数列表) 返回值类型 = 方法体实现
     */
   // println("方法的调用结果："+add(9, 9))


    println("--------------------函数-标志（=>）------------------")
     val func = (a:Int,b:Int)=>a*b
    println("func:"+func(2,3))

    var f2 = ()=> 10*2
    println("f2:"+f2())

    var funcMap1 = map1 _ //方法转换成函数
    var arr = Array(1,2,3,4,5,6)
    arr.map(funcMap1(_)).foreach(println)
    arr.map(map1).foreach(println)

    var addFunc:Int=>Int=x=>x*x
    println(addFunc(10))

    println(h((a,b)=>a*b, 20, 30))
  }


  def h(f:(Int,Int)=>Int,a:Int,b:Int):Int=f(a,b)


   def map1(a:Int):Int={
     a*10
   }


  //方法的定义返回值Int
//  def add(a:Int,b:Int):Int={
//    a+b
//  }

  //方法的定义返回值自动推断 注意：如果没有写“=”就没有返回值
  def add(a:Int,b:Int)={
    a+b
  }

}
