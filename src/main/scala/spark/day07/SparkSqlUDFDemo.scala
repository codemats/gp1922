package spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.api.java.{UDF1, UDF2}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}


//用户自定义函数
object SparkSqlUDFDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //注册用户自定义函数：grade:自定义函数名称，在写sql是用到,new Grade(参数1，参数2....),LongType:返回值类型
    spark.udf.register("grade",new Grade("salary"),LongType)

    val dataFrame: DataFrame = spark.read.json("/Users/a10.12/Documents/study/bigdata/spark/gp1922/testFile/emp.json")

    dataFrame.createOrReplaceTempView("emp")

    spark.sql("select * ,grade(age,salary) as grade from emp").show()

    spark.stop()

    /**
      * t1:20
t2:100
t1:15
t2:200
t1:25
t2:300
t1:30
t2:200
t1:40
t2:100
+---+------------+---+----+------+-----+-----+
|age|departmentId| id|name|salary|  sex|grade|
+---+------------+---+----+------+-----+-----+
| 20|           1|  1|  李四|   100|  man|    5|
| 15|           2|  2|  李梅|   200|  man|   13|
| 25|           3|  3| 张阳婵|   300|women|   12|
| 30|           2|  4| 李四妹|   200|women|    6|
| 40|           2|  5| 王二小|   100|women|    2|
+---+------------+---+----+------+-----+-----+
      */
  }
}

//含有数值的是Long类型,UDF数值[输入类型...，返回值类型]
class Grade(salary:String) extends  UDF2[Long,Long,Long]{
  override def call(t1: Long, t2: Long): Long = {
    println("t1:"+t1) //age
    println("t2:"+t2) //salary
    t2/t1
  }
}
