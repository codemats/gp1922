package spark.day06

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//数据格式： 访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
//需求：统计查询词的访问次数，并保存到mysql中
object SogoQ2MysqlDemo {

  val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    //文件路径
    val filePaht = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/src/main/scala/spark/day06/SogouQ.sample"

    //这里在定义修改读取编码为GBK[文件是GBK]，sparkContenxt.textFile(filePath)默认读取编码是UTF-8
    val rdd: RDD[String] = this.textFile(filePaht)

    val tuples: RDD[(String, Int)] = rdd.map(line => {
      val lineArr: Array[String] = line.split("\t")
      (lineArr(2).trim, 1)
    })
    val reduceWordCount: RDD[(String, Int)] = tuples.reduceByKey(_+_)

    val resultList: List[(String, Int)] = reduceWordCount.collect.toList

    val mysqlUrl = "jdbc:mysql://localhost/spark_user?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC"
    val mySqlUtils = new MySqlUtils(mysqlUrl,"root","root")
    mySqlUtils.getCon()

    resultList.foreach(word =>{
      val sql = "insert into word_info(word,count) values(?,?)"
      println(sql)
      mySqlUtils.save(sql,word._1,word._2)
    })

    mySqlUtils.close()
    sc.stop()
  }

  def textFile(
                path: String,
                minPartitions: Int = sc.defaultMinPartitions): RDD[String] =  {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => new String(pair._2.getBytes(), "GBK")).setName(path)
  }
}


class MySqlUtils(url:String,userName:String,password:String){
  /**
    * 获取连接
    * @return connection
    */

  private [this] var con :Connection=_
  private [this] var  statement: PreparedStatement = _

  def getCon():Connection={
    Class.forName("com.mysql.jdbc.Driver")
    con = DriverManager.getConnection(this.url,this.userName,this.password)
    con
  }

  /**
    * 数据保存
    * @param sql 执行保存的sql语句
    * @return Boolean
    */
  def save(sql:String,word:String,count:Int):Boolean={
    statement = this.con.prepareStatement(sql)
    statement.setString(1,word)
    statement.setInt(2,count)
    if (statement.executeUpdate() > 0) true else false
  }

  /**
    * 关闭资源
    */
  def close():Unit={
    if (this.statement != null) statement.close()
    if (this.con != null) con.close()
  }



}





