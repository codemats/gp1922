package spark.day09

//测试mysql工具
object TestMysqlUtils {
  def main(args: Array[String]): Unit = {
    val mysqlUtils = new SparkStreamingOffsetUtils(
      "jdbc:mysql://localhost/spark_user?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC" ,
      "root",
      "root")

    //数据保存
    val saveSql = "insert into kafka_offset(id,topic,start_offset,end_offsert,value) values(?,?,?,?,?)"
    val sqlArgs = List("1","test0","100","1001","hello word")
    val count: Int = mysqlUtils.save(saveSql,sqlArgs)
    println(s"数据保存结果：${count}")

    //数据更新
    val updateSql = "update kafka_offset set start_offset= ? , end_offsert= ? , value=? where topic=?"
    val sqlUpateArgs = List("1000","10010","你好李梅...","test0")
    val countUpadte: Int = mysqlUtils.update(updateSql,sqlUpateArgs)
    println(s"数据保存结果：${countUpadte}")
  }
}
