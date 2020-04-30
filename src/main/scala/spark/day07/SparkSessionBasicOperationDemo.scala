package spark.day07

import org.apache.spark.sql.{DataFrame, SparkSession}

//sparkSql基本开发流程
object SparkSessionBasicOperationDemo {

  def main(args: Array[String]): Unit = {

    val filePath ="/Users/a10.12/Documents/study/bigdata/spark/gp1922/testFile/people.json"

    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[2]").getOrCreate()

    //引入隐式转换
    import spark.implicits._

    //读取文件（json）
    //方式1
    //val contentDataFrame: DataFrame = spark.read.format("json").load(filePath)

    //方式2
    val contentDataFrame: DataFrame = spark.read.json(filePath)

    //数据展示
    contentDataFrame.show()

    /**
      * +----+-------+
        | age|   name|
        +----+-------+
        |null|Michael|
        |  30|   Andy|
        |  19| Justin|
        +----+-------+
      */

    //使用sql操作数据流程
    println("--------使用sql操作数据流程-----------")

    //创建临时表（有临时表[作用域：当前sparkSession，使用较多]和全局表[作用域：当前应用程序，较少使用]）
    contentDataFrame.createOrReplaceTempView("people") //people为表名称

    //创建并使用sql操作数据
    val sqlDataDf: DataFrame = spark.sql("select * from people where age > 18")

    //sql操作数据展示
    sqlDataDf.show()

    /**
      * +---+------+
        |age|  name|
        +---+------+
        | 30|  Andy|
        | 19|Justin|
        +---+------+
      */

    println("-----使用DSL语言操作--------")

    contentDataFrame.select("age","name").where($"age" > 18).show()

    /**
      * +---+------+
        |age|  name|
        +---+------+
        | 30|  Andy|
        | 19|Justin|
        +---+------+
      */

    println("----DSL分组操作----")
    //需要导入，否则无法执行agg等操作
    import org.apache.spark.sql.functions._
    contentDataFrame.select("age","name").where($"age" > 18).groupBy($"age").agg(col("age")).show()

    /**
      * +---+---+
        |age|age|
        +---+---+
        | 19| 19|
        | 30| 30|
        +---+---+
      */
   //停止
    spark.stop()

  }

}
