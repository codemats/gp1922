package spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql._


//需求：
// 统计部门中所有大于20岁的不同性别员工的平均薪资和平均年龄
object PracticeDemo01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //文件路径
    val empFilePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/testFile/emp.json"
    val departmentFilePath = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/testFile/department.json"

    //读取文件--员工信息
    val empDataFrame: DataFrame = spark.read.json(empFilePath)
    //读取文件--部门信息
    val departmentDataFrame:DataFrame = spark.read.json(departmentFilePath)

    import spark.implicits._
    //实现方式1--使用DSL
    empDataFrame.filter($"age" > 20).join(departmentDataFrame, empDataFrame("departmentId") === departmentDataFrame("id"))
    .groupBy(empDataFrame("sex"),departmentDataFrame("id"),departmentDataFrame("name"))
    .agg("salary"->"avg","age"->"avg").show()

    /**
    +-----+---+-----------+--------+
    |  sex| id|avg(salary)|avg(age)|
    +-----+---+-----------+--------+
    |women|  2|      150.0|    35.0|
    |women|  3|      300.0|    25.0|
    +-----+---+-----------+--------+ */

    //sparkSql实现

    empDataFrame.createOrReplaceTempView("employee")
    departmentDataFrame.createOrReplaceTempView("department")

    //先关联求出数据，再创建一张临时表
    val empDeparFrame = spark.sql("select em.*,de.name as departName from employee em left join department de on em.departmentId = de.id where em.age > 20")
    empDeparFrame.createOrReplaceTempView("empDeparFrame")

    //做最后的聚合动作
    val result: DataFrame = spark.sql("select sex,departmentId,departName,avg(salary) as avg_salary,avg(age) as avg_age from empDeparFrame group by sex,departmentId,departName ")
    result.select("sex","departName","avg_salary","avg_age").show()
    /**
      *+-----+----------+----------+-------+
        |  sex|departName|avg_salary|avg_age|
        +-----+----------+----------+-------+
        |women|       产品部|     300.0|   25.0|
        |women|     人力资源部|     150.0|   35.0|
        +-----+----------+----------+-------+
      */

    //结果写到文件中，使用JSON格式[调整分区数为1，只写到一个文件,默认200个分区，写到文件有200个]
    val value: Dataset[Row] = result.repartition(1)
    value.select("sex","departName","avg_salary","avg_age").write.json("/Users/a10.12/Documents/study/bigdata/spark/gp1922/file/empAndDepart.json")

    //数据结果写到mysql数据中
    result.select("sex","departName","avg_salary", "avg_age").write
      .mode(SaveMode.Append) //写模式：覆盖
      .format("jdbc") //写数据方式：jdbc
      .option("url", "jdbc:mysql://localhost/spark_user?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC") //数据库URL
      .option("dbtable", "spark_user.emp_depart_avg") //数据库表
      .option("user", "root") //数据库用户
      .option("password", "root") //数据库密码
      .save() //保存操作

    //停止
    spark.stop()
  }
}
