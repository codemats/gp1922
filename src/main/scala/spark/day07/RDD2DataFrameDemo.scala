package spark.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//RDD--->DataFrame的方式
object RDD2DataFrameDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    import spark.implicits._

    println("-------------方式1(Tuple)--知道-----------")

    //list中是最后生成schem的字段值
    val personFileds:List[String] = List("lisi-23-34")
    val personFiledsRdd=sc.makeRDD(personFileds).map(item => {
      val arr: Array[String] = item.split("-")
      (arr(0),arr(1),arr(2))
    })
    //定义schem的字段名称
    val nameFrame: DataFrame = personFiledsRdd.toDF("name","age","score")
    nameFrame.printSchema()

    /** 字段信息显示
      * root
           |-- name: string (nullable = true)
           |-- age: string (nullable = true)
           |-- score: string (nullable = true)
      */
    nameFrame.show()

    /** 值显显示
      * +----+---+-----+
        |name|age|score|
        +----+---+-----+
        |lisi| 23|   34|
        +----+---+-----+
      */

    println("-------------方式2(反射)--常用最多-----------")

    val personFiedsValue:List[String] = List("刘德华-20-100-刘德华-20-100-刘德华-20-100-刘德华-20-100-刘德华-20-100-刘德华-20-100-刘德华-20-100-刘德华-20-100")
    val personRDD: RDD[Person] = sc.makeRDD(personFiedsValue).map(line => {
      val arr = line.split("-")
      Person(
       arr(0).trim, arr(1).trim, arr(2).trim
      ,arr(3).trim, arr(4).trim, arr(5).trim
      ,arr(6).trim, arr(7).trim, arr(8).trim
      ,arr(9).trim, arr(10).trim, arr(11).trim
      ,arr(12).trim, arr(13).trim, arr(14).trim
      ,arr(15).trim, arr(16).trim, arr(17).trim
      ,arr(18).trim, arr(19).trim, arr(20).trim
      ,arr(21).trim,arr(22).trim,arr(23).trim
      )
    })
    val personFrame: DataFrame = personRDD.toDF()
    personFrame.printSchema()
    personFrame.show()

    /**
      * root
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- score: string (nullable = true)
 |-- name1: string (nullable = true)
 |-- age1: string (nullable = true)
 |-- score1: string (nullable = true)
 |-- name2: string (nullable = true)
 |-- age2: string (nullable = true)
 |-- score2: string (nullable = true)
 |-- name3: string (nullable = true)
 |-- age3: string (nullable = true)
 |-- score3: string (nullable = true)
 |-- name4: string (nullable = true)
 |-- age4: string (nullable = true)
 |-- score4: string (nullable = true)
 |-- name5: string (nullable = true)
 |-- age5: string (nullable = true)
 |-- score5: string (nullable = true)
 |-- name6: string (nullable = true)
 |-- age6: string (nullable = true)
 |-- score6: string (nullable = true)
 |-- name7: string (nullable = true)
 |-- age7: string (nullable = true)
 |-- score7: string (nullable = true)

+----+---+-----+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+
|name|age|score|name1|age1|score1|name2|age2|score2|name3|age3|score3|name4|age4|score4|name5|age5|score5|name6|age6|score6|name7|age7|score7|
+----+---+-----+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+
| 刘德华| 20|  100|  刘德华|  20|   100|  刘德华|  20|   100|  刘德华|  20|   100|  刘德华|  20|   100|  刘德华|  20|   100|  刘德华|  20|   100|  刘德华|  20|   100|
+----+---+-----+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+-----+----+------+

      */

    println("-------------方式3(StructType)--（比较常用）-----------")

    //定义数据为Row(很多字段值)
    val peopleList:RDD[String] = sc.makeRDD(List("李四-21-23"))
    val peopleRDD: RDD[Row] = peopleList.map(line => {
      val arr: Array[String] = line.split("-")
      Row(arr(0), arr(1), arr(2))
    })

    //定义字段值信息
    val schema: StructType = StructType(
      List(
        StructField("name", StringType, nullable = true),
        StructField("age", StringType, nullable = true),
        StructField("score", StringType, nullable = true)
    ))

    //创建dataFrame
    val dataFrame: DataFrame = spark.createDataFrame(peopleRDD,schema)
    dataFrame.printSchema()
    dataFrame.show()

    /**
      * root
         |-- name: string (nullable = true)
         |-- age: string (nullable = true)
         |-- score: string (nullable = true)

        +----+---+-----+
        |name|age|score|
        +----+---+-----+
        |  李四| 21|   23|
        +----+---+-----+
      */

    /**
      * 官方例子
      *import org.apache.spark.sql.types._

        // Create an RDD
        val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

        // The schema is encoded in a string
        val schemaString = "name age"

        // Generate the schema based on the string of schema
        val fields = schemaString.split(" ")
          .map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        // Convert records of the RDD (people) to Rows
        val rowRDD = peopleRDD
          .map(_.split(","))
          .map(attributes => Row(attributes(0), attributes(1).trim))

        // Apply the schema to the RDD
        val peopleDF = spark.createDataFrame(rowRDD, schema)

        // Creates a temporary view using the DataFrame
        peopleDF.createOrReplaceTempView("people")

        // SQL can be run over a temporary view created using DataFrames
        val results = spark.sql("SELECT name FROM people")

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        results.map(attributes => "Name: " + attributes(0)).show()
      */

    spark.stop()
  }
}


//样例类---方式2生产DataFrame,注意：样例类转DataFrame的字段没有限制为22个，被限制22的为tuple
case class Person(name:String,
                  age:String,
                  score:String,
                  name1:String,
                  age1:String,
                  score1:String,
                  name2:String,
                  age2:String,
                  score2:String,
                  name3:String,
                  age3:String,
                  score3:String,
                  name4:String,
                  age4:String,
                  score4:String,
                  name5:String,
                  age5:String,
                  score5:String,
                  name6:String,
                  age6:String,
                  score6:String,
                  name7:String,
                  age7:String,
                  score7:String
                 )

