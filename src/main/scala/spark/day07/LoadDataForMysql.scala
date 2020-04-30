package spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//从mysql中获取数据---mysql表数据
object LoadDataForMysql {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val fieldQuery = "人力资源部"

    //注意：spark在读取mysql数据表时，如果只是查询的sql语句，读取数据时底层会自动加上where 1= 0[s"SELECT * FROM $table WHERE 1=0"]
    //导致sql出错
    //解决方式：查询mysql的语句使用小括号括起来，里面写sql语句，即「(sql语句) as 别名」让其是个子查询即可，最终获得的sql为
    //s"SELECT * FROM (select * from 数据库.表名称 where 条件） as 别名 WHERE 1=0"
    val sql =s"(select ed.* from spark_user.emp_depart_avg ed where 1=1 and ed.departName='${fieldQuery}') tmp"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost/spark_user?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC")
      .option("dbtable",sql)
      .option("user", "root")
      .option("password", "root")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    jdbcDF.show()
    spark.stop() //spark.close()方法最后还是调用了stop()方法

  }
}
